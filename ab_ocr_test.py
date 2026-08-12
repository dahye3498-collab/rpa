# -*- coding: utf-8 -*-
"""
ab_ocr_test.py — 품목표 OCR A/B 테스트 (gpt-4.1  vs  claude-sonnet-5)

같은 이미지·같은 프롬프트·같은 타일링 조건으로 두 모델을 나란히 돌려
정확도(행 수, 품목 일치)와 속도를 비교한다. 전면 교체 전 근거 확보용.

준비:
    1) venv에 anthropic 설치됨 (pip install anthropic)
    2) .env 에  OPENAI_API_KEY  와  ANTHROPIC_API_KEY  둘 다 필요

실행:
    venv/Scripts/python.exe ab_ocr_test.py --date 2026-08-12 --limit 5
    venv/Scripts/python.exe ab_ocr_test.py --limit 8            # 최신 날짜 자동
    venv/Scripts/python.exe ab_ocr_test.py --files 가나축산유통주_*.png 금미트_*.png

옵션:
    --date    수집일 폴더 (기본: visionmeat 내 최신 날짜)
    --limit   테스트 이미지 수 (기본 5)
    --files   특정 파일명(패턴) 지정 (지정 시 --date/--limit 무시하고 해당 파일만)
    --model   Claude 모델 (기본 claude-sonnet-5)
    --think   Claude 사고(thinking) 켜기 (기본 꺼짐; GPT와 공정 비교 위해 기본 off)
    --out     결과 파일 접두어 (기본 scratchpad 아래 ab_ocr_<date>)

출력:
    - 콘솔: 이미지별/합계 비교표
    - <out>.xlsx : 요약 + 항목 차이 시트
    - <out>.json : 두 엔진 원본 추출 결과(수동 대조용)
"""
import os
import re
import sys
import time
import json
import glob
import base64
import argparse
from io import BytesIO
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv()

import batch_processor as bp   # 프롬프트·타일링·병합 로직 재사용

try:
    from PIL import Image
    PIL_OK = True
except Exception:
    PIL_OK = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRATCH = os.environ.get(
    "AB_OUT_DIR",
    os.path.join(BASE_DIR, "ab_ocr_out"),
)


# ── 공통 유틸 ────────────────────────────────────────────────
def _norm(s) -> str:
    return re.sub(r"\s+", "", str(s if s is not None else "")).lower()


def item_key(r: dict) -> tuple:
    """행 동일성 키 (품목·브랜드·창고). 판매가·재고 등 변동 필드는 제외."""
    return (_norm(r.get("품목")), _norm(r.get("브랜드")), _norm(r.get("창고")))


def parse_json_loose(text: str) -> dict:
    """마크다운 펜스/잡텍스트가 섞여도 첫 {...마지막 } 구간을 파싱."""
    if not text:
        return {}
    t = text.strip()
    t = re.sub(r"^```(?:json)?", "", t).strip()
    t = re.sub(r"```$", "", t).strip()
    try:
        return json.loads(t)
    except Exception:
        pass
    i, j = t.find("{"), t.rfind("}")
    if i != -1 and j != -1 and j > i:
        try:
            return json.loads(t[i:j + 1])
        except Exception:
            return {}
    return {}


def salvage_objects(text: str) -> list:
    """잘린/깨진 JSON에서 완결된 {..} 행 객체만 개별 파싱해 복구.
    (max_tokens 초과 등으로 배열이 미완결일 때 부분이라도 살림)"""
    out = []
    for m in re.findall(r"\{[^{}]*\}", text or ""):
        try:
            o = json.loads(m)
        except Exception:
            continue
        if isinstance(o, dict) and "품목" in o:
            out.append(o)
    return out


# ── 압축 출력(TSV) 포맷 — 출력 토큰 대폭 절감 ────────────────
# 매 행 한글 키 14개를 반복하는 대신 값만 탭 구분으로 출력 → 다시 매핑
COMPACT_COLS = ["축종", "원산지", "보관", "품목", "브랜드", "등급", "EST",
                "평중_kg", "스펙_설명", "재고_box", "창고", "소비기한",
                "판매가_원", "수정일", "비고"]


def build_compact_prompt() -> str:
    cols = " | ".join(f"{i+1}.{c}" for i, c in enumerate(COMPACT_COLS))
    prompt = f"""이 이미지는 축산물 품목표(재고/가격 리스트)입니다.
표의 각 품목 행을 TSV(탭 구분) 한 줄로 출력하세요.
헤더·설명·코드펜스·번호 없이 오직 데이터 행만 출력합니다.

열 순서(정확히 15열, 탭 14개로 구분):
{cols}

규칙:
1. 병합 셀(축종·원산지·브랜드·창고)은 해당 그룹의 모든 행에 반드시 반복해 채우세요.
   특히 창고가 여러 행에 병합되면 빈칸으로 두지 말고 각 행에 같은 값을 채우세요.
2. 표의 모든 행을 하나도 빠짐없이. 요약·생략·중복제거 금지.
   2단(좌우 2열) 레이아웃이면 좌측 열 전체를 먼저, 그다음 우측 열 전체를 출력하세요.
3. 값은 보이는 그대로(verbatim). 품목명은 4.품목, 브랜드명은 5.브랜드로 분리.
4. 창고와 등급·EST 혼동 금지. 숫자/영문코드(예: 86M, 208A)는 창고가 아니라 EST.
5. 포장방식(VP, IWP 등)은 15.비고.
6. 숫자 필드(평중_kg, 재고_box, 판매가_원)는 단위 없이 숫자만.
7. 값이 없으면 빈칸(탭 사이에 아무것도 쓰지 않음). 단 병합된 창고·원산지·브랜드는 규칙1대로 채움.
8. 각 셀 안에 탭·줄바꿈을 넣지 마세요. 한 품목 = 정확히 한 줄 = 탭 14개.
"""
    try:
        import warehouses
        prompt += "\n" + warehouses.prompt_block()
    except Exception:
        pass
    return prompt


def parse_tsv_rows(text: str) -> list:
    """TSV 텍스트를 행 dict 리스트로. 잘린 마지막 줄은 자연히 버려짐."""
    rows = []
    n = len(COMPACT_COLS)
    for line in (text or "").splitlines():
        s = line.strip()
        if not s or s.startswith("```") or s.startswith("#"):
            continue
        cells = line.split("\t")
        if len(cells) < 4:      # 품목(4번째)도 못 채운 잘린 줄 → 스킵
            continue
        cells = (cells + [""] * n)[:n]
        r = {COMPACT_COLS[i]: cells[i].strip() for i in range(n)}
        if r.get("품목"):
            rows.append(r)
    return rows


# ── 가격표(100만 토큰당, 2026-08 도입가 반영) ────────────────
PRICING = {
    "claude-sonnet-5":  (2.0, 10.0),   # 도입가 (~2026-08-31), 이후 3/15
    "claude-haiku-4-5": (1.0, 5.0),
    "claude-opus-5":    (5.0, 25.0),
}
_USAGE = {"in": 0, "out": 0}


# ── Claude OCR (batch_processor의 GPT 경로와 동일 조건) ───────
import anthropic
_aclient = None


def aclient():
    global _aclient
    if _aclient is None:
        _aclient = anthropic.Anthropic()  # ANTHROPIC_API_KEY 사용
    return _aclient


_CLAUDE_SYS = (
    "You are a precise OCR extraction engine for Korean meat-product price tables. "
    "Follow the user's schema exactly and output ONLY valid JSON — no markdown fences, no commentary."
)


def claude_ocr_from_b64(b64, media_type, prompt, response_key, model,
                        think=False, compact=False):
    # max_tokens 16000: 밀집표(수십 행) 출력이 잘리지 않게 넉넉히.
    # 16k는 비스트리밍 HTTP 타임아웃 경계라 스트리밍으로 호출.
    kwargs = dict(
        model=model,
        max_tokens=16000,
        system=_CLAUDE_SYS,
        messages=[{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
            ],
        }],
    )
    kwargs["thinking"] = {"type": "adaptive"} if think else {"type": "disabled"}
    for attempt in range(1, 4):
        try:
            with aclient().messages.stream(**kwargs) as st:
                resp = st.get_final_message()
            try:
                _USAGE["in"] += resp.usage.input_tokens
                _USAGE["out"] += resp.usage.output_tokens
            except Exception:
                pass
            text = "".join(b.text for b in resp.content if b.type == "text")
            if compact:                       # TSV: 잘린 마지막 줄은 자연히 버려짐
                return parse_tsv_rows(text)
            data = parse_json_loose(text)
            val = data.get(response_key) if isinstance(data, dict) else None
            if isinstance(val, list) and val:
                return val
            # JSON이 잘렸거나 파싱 실패 → 완결 행만 복구
            sal = salvage_objects(text)
            if sal:
                if resp.stop_reason == "max_tokens":
                    print(f"  [claude 출력 잘림 복구: {len(sal)}행]", flush=True)
                return sal
            return val if isinstance(val, list) else []
        except Exception as e:
            print(f"  [claude 재시도 {attempt}/3] {e}", flush=True)
            time.sleep(2 * attempt)
    return []


def _tile_bounds(h):
    band, ov = bp.OCR_TILE_HEIGHT, bp.OCR_TILE_OVERLAP
    bounds, y = [], 0
    while y < h:
        y2 = min(h, y + band)
        bounds.append((y, y2))
        if y2 >= h:
            break
        y += max(1, band - ov)
    return bounds


def claude_extract_products(path, model, think=False, compact=False):
    """batch_processor.extract_data_from_image 의 Claude 버전 (동일 타일링).
    compact=True면 TSV 압축 출력 프롬프트 사용(출력 토큰 절감)."""
    if compact:
        prompt, key = build_compact_prompt(), None
    else:
        prompt, key = bp._build_prompt("품목표")
    tall = False
    if PIL_OK:
        try:
            with Image.open(path) as im:
                tall = im.height > bp.OCR_TILE_THRESHOLD
        except Exception:
            tall = False
    if not tall:
        b64 = bp.encode_image(path)
        return claude_ocr_from_b64(b64, bp.get_mime_by_ext(path), prompt, key, model, think, compact)

    with Image.open(path) as im:
        w, h = im.size
        bounds = _tile_bounds(h)
        tiles = []
        for (y1, y2) in bounds:
            crop = im.crop((0, y1, w, y2))
            buf = BytesIO()
            crop.save(buf, format="PNG")
            b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
            rows = claude_ocr_from_b64(b64, "image/png", prompt, key, model, think, compact)
            tiles.append(rows if isinstance(rows, list) else [])
    return bp._merge_product_tiles(tiles)


# ── 대상 이미지 수집 ──────────────────────────────────────────
def latest_date():
    ds = sorted(
        os.path.basename(os.path.dirname(os.path.dirname(p)))
        for p in glob.glob(os.path.join(BASE_DIR, "visionmeat", "*", "품목표", "screenshots"))
    )
    ds = [d for d in ds if re.match(r"^\d{4}-\d{2}-\d{2}$", d)]
    return ds[-1] if ds else None


def collect_images(date, limit, files):
    shot_dir = os.path.join(BASE_DIR, "visionmeat", date, "품목표", "screenshots")
    if files:
        out = []
        for pat in files:
            out += sorted(glob.glob(os.path.join(shot_dir, pat)))
        return out
    imgs = sorted(glob.glob(os.path.join(shot_dir, "*.png")))
    return imgs[:limit] if limit and limit > 0 else imgs


# ── 메인 ─────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--files", nargs="*", default=None)
    ap.add_argument("--model", default="claude-sonnet-5")
    ap.add_argument("--think", action="store_true")
    ap.add_argument("--engine", choices=["both", "claude", "gpt"], default="both",
                    help="both=둘 다 실행 / claude=Claude만(이전 GPT결과 재사용) / gpt=GPT만")
    ap.add_argument("--compact", action="store_true",
                    help="Claude 출력을 TSV 압축포맷으로(출력 토큰·비용 절감)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    run_gpt = args.engine in ("both", "gpt")
    run_claude = args.engine in ("both", "claude")
    if run_gpt and not os.getenv("OPENAI_API_KEY"):
        print("❌ .env 에 OPENAI_API_KEY 가 없습니다."); sys.exit(1)
    if run_claude and not os.getenv("ANTHROPIC_API_KEY"):
        print("❌ .env 에 ANTHROPIC_API_KEY 가 없습니다. Anthropic 콘솔에서 키를 발급받아 추가하세요.")
        print("   (예:  ANTHROPIC_API_KEY=sk-ant-...  를 .env 에 한 줄 추가)")
        sys.exit(1)

    date = args.date or latest_date()
    if not date:
        print("❌ 대상 날짜 폴더를 찾지 못했습니다."); sys.exit(1)
    images = collect_images(date, args.limit, args.files)
    if not images:
        print(f"❌ {date} 에서 테스트할 이미지를 찾지 못했습니다."); sys.exit(1)

    os.makedirs(SCRATCH, exist_ok=True)
    out_prefix = args.out or os.path.join(SCRATCH, f"ab_ocr_{date}")

    # 한쪽만 실행할 때 반대편은 이전 결과 json 에서 재사용
    prior = {}
    if not (run_gpt and run_claude):
        jprev = out_prefix + ".json"
        if os.path.exists(jprev):
            try:
                with open(jprev, encoding="utf-8") as f:
                    prior = json.load(f)
                print(f"ℹ️ 이전 결과 재사용: {jprev}")
            except Exception:
                prior = {}

    print("=" * 72)
    print(f"  품목표 OCR A/B  |  gpt-4.1  vs  {args.model}  "
          f"(think={args.think}, engine={args.engine}, compact={args.compact})")
    print(f"  날짜 {date}  ·  이미지 {len(images)}장")
    print("=" * 72)

    rows_summary, diff_rows, raw = [], [], {}
    tot_g_rows = tot_c_rows = tot_g_time = tot_c_time = 0.0
    tot_common = tot_g_only = tot_c_only = 0

    for idx, path in enumerate(images, 1):
        name = re.sub(r"_\d+\.png$", "", os.path.basename(path))
        print(f"\n[{idx}/{len(images)}] {name}")

        if run_gpt:
            t0 = time.time()
            try:
                g = bp.extract_data_from_image(path, "품목표")
                g = g if isinstance(g, list) else []
            except Exception as e:
                print(f"  GPT 오류: {e}"); g = []
            gt = time.time() - t0
        else:
            g = prior.get(name, {}).get("gpt", []) or []
            gt = 0.0
            print(f"  (GPT 이전결과 재사용: {len(g)}행)")

        if run_claude:
            t0 = time.time()
            try:
                c = claude_extract_products(path, args.model, args.think, args.compact)
            except Exception as e:
                print(f"  Claude 오류: {e}"); c = []
            ct = time.time() - t0
        else:
            c = prior.get(name, {}).get("claude", []) or []
            ct = 0.0

        gk = {}
        for r in g:
            gk[item_key(r)] = r
        ck = {}
        for r in c:
            ck[item_key(r)] = r
        common = set(gk) & set(ck)
        g_only = set(gk) - set(ck)
        c_only = set(ck) - set(gk)

        print(f"  행 수:  GPT {len(g):>3}  |  Claude {len(c):>3}   "
              f"(공통 {len(common)}, GPT만 {len(g_only)}, Claude만 {len(c_only)})")
        print(f"  시간:   GPT {gt:5.1f}s |  Claude {ct:5.1f}s")

        rows_summary.append({
            "업체": name, "GPT_행수": len(g), "Claude_행수": len(c),
            "공통": len(common), "GPT만": len(g_only), "Claude만": len(c_only),
            "GPT_초": round(gt, 1), "Claude_초": round(ct, 1),
        })
        for k in sorted(g_only):
            r = gk[k]
            diff_rows.append({"업체": name, "엔진": "GPT만", "품목": r.get("품목", ""),
                              "브랜드": r.get("브랜드", ""), "창고": r.get("창고", "")})
        for k in sorted(c_only):
            r = ck[k]
            diff_rows.append({"업체": name, "엔진": "Claude만", "품목": r.get("품목", ""),
                              "브랜드": r.get("브랜드", ""), "창고": r.get("창고", "")})
        raw[name] = {"gpt": g, "claude": c}

        tot_g_rows += len(g); tot_c_rows += len(c)
        tot_g_time += gt; tot_c_time += ct
        tot_common += len(common); tot_g_only += len(g_only); tot_c_only += len(c_only)

    # ── 합계 ──
    print("\n" + "=" * 72)
    print("  합계")
    print(f"  총 행수:  GPT {int(tot_g_rows)}  |  Claude {int(tot_c_rows)}")
    print(f"  항목 일치: 공통 {tot_common} · GPT만 {tot_g_only} · Claude만 {tot_c_only}")
    print(f"  총 시간:  GPT {tot_g_time:.1f}s  |  Claude {tot_c_time:.1f}s")

    # ── Claude 비용 실측 (usage 기반) ──
    if run_claude and (_USAGE["in"] or _USAGE["out"]):
        pin, pout = PRICING.get(args.model, (0, 0))
        cost = _USAGE["in"] / 1e6 * pin + _USAGE["out"] / 1e6 * pout
        n = len(images) or 1
        print("-" * 72)
        print(f"  Claude 토큰:  입력 {_USAGE['in']:,}  ·  출력 {_USAGE['out']:,}  ({args.model})")
        print(f"  Claude 비용:  이번 {len(images)}장 ≈ ${cost:.3f}  "
              f"(장당 ${cost/n:.3f})")
        print(f"  일 150장 환산 ≈ ${cost/n*150:.2f}/일  ·  월 ≈ ${cost/n*150*30:.0f}/월")
    print("=" * 72)
    print("  ※ 행수·항목차이는 정확도의 대략 지표일 뿐, 최종 판단은 원본 대조가 필요합니다.")
    print("     아래 xlsx 의 '항목차이' 시트에서 각 엔진만 잡은 품목을 원본과 대조하세요.")

    # ── 저장 ──
    try:
        import pandas as pd
        xlsx = out_prefix + ".xlsx"
        with pd.ExcelWriter(xlsx) as xw:
            pd.DataFrame(rows_summary).to_excel(xw, sheet_name="요약", index=False)
            if diff_rows:
                pd.DataFrame(diff_rows).to_excel(xw, sheet_name="항목차이", index=False)
        print(f"\n📄 요약 저장: {xlsx}")
    except Exception as e:
        print(f"xlsx 저장 실패: {e}")

    jpath = out_prefix + ".json"
    with open(jpath, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)
    print(f"📄 원본 결과 저장: {jpath}")


if __name__ == "__main__":
    main()
