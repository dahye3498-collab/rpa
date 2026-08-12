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


def claude_ocr_from_b64(b64, media_type, prompt, response_key, model, think=False):
    kwargs = dict(
        model=model,
        max_tokens=8000,
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
            resp = aclient().messages.create(**kwargs)
            text = "".join(b.text for b in resp.content if b.type == "text")
            data = parse_json_loose(text)
            val = data.get(response_key, [])
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


def claude_extract_products(path, model, think=False):
    """batch_processor.extract_data_from_image 의 Claude 버전 (동일 타일링)."""
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
        return claude_ocr_from_b64(b64, bp.get_mime_by_ext(path), prompt, key, model, think)

    with Image.open(path) as im:
        w, h = im.size
        bounds = _tile_bounds(h)
        tiles = []
        for (y1, y2) in bounds:
            crop = im.crop((0, y1, w, y2))
            buf = BytesIO()
            crop.save(buf, format="PNG")
            b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
            rows = claude_ocr_from_b64(b64, "image/png", prompt, key, model, think)
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
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("❌ .env 에 OPENAI_API_KEY 가 없습니다."); sys.exit(1)
    if not os.getenv("ANTHROPIC_API_KEY"):
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

    print("=" * 72)
    print(f"  품목표 OCR A/B  |  gpt-4.1  vs  {args.model}  (think={args.think})")
    print(f"  날짜 {date}  ·  이미지 {len(images)}장")
    print("=" * 72)

    rows_summary, diff_rows, raw = [], [], {}
    tot_g_rows = tot_c_rows = tot_g_time = tot_c_time = 0.0
    tot_common = tot_g_only = tot_c_only = 0

    for idx, path in enumerate(images, 1):
        name = re.sub(r"_\d+\.png$", "", os.path.basename(path))
        print(f"\n[{idx}/{len(images)}] {name}")

        t0 = time.time()
        try:
            g = bp.extract_data_from_image(path, "품목표")
            g = g if isinstance(g, list) else []
        except Exception as e:
            print(f"  GPT 오류: {e}"); g = []
        gt = time.time() - t0

        t0 = time.time()
        try:
            c = claude_extract_products(path, args.model, args.think)
        except Exception as e:
            print(f"  Claude 오류: {e}"); c = []
        ct = time.time() - t0

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
