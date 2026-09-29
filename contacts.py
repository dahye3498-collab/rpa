# -*- coding: utf-8 -*-
"""
contacts.py — 품목표 스크린샷 상단부에서 업체 연락처 추출/캐시

- visionmeat/*/품목표/screenshots/*.png 의 상단(헤더)만 OCR → {업체명, 담당자, 전화[], 팩스[]}
- 업체(파일명에서 _타임스탬프 제거) 단위로 1건만 추출(연락처는 업체별 고정)
- visionmeat/database/vendor_contacts.json 에 캐시(증분: 신규 업체만 OCR)
"""
import os
import re
import glob
import json
import base64
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from openai import OpenAI

try:
    from PIL import Image
    PIL_OK = True
except Exception:
    PIL_OK = False

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VM_ROOT = os.path.join(BASE_DIR, "visionmeat")
CONTACTS_PATH = os.path.join(VM_ROOT, "database", "vendor_contacts.json")

_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
_MODEL = os.getenv("OPENAI_VISION_MODEL", "gpt-4.1")
_HEADER_PX = int(os.getenv("CONTACT_HEADER_PX", "750"))

_PROMPT = (
    "이 이미지는 축산물 품목표(거래처 가격표)의 상단부입니다. "
    "업체명, 담당자명, 전화번호(휴대폰·대표번호 모두), 팩스번호를 추출하세요. "
    'JSON만: {"업체명":"", "담당자":"", "전화":[], "팩스":[]}'
)


def _vendor_from_filename(fn: str) -> str:
    return re.sub(r"_\d+\.(png|jpg|jpeg)$", "", str(fn), flags=re.I)


def _all_screenshots() -> dict:
    """업체 -> 대표 스크린샷 경로 (최신 파일)."""
    vend = {}
    for f in glob.glob(os.path.join(VM_ROOT, "*", "품목표", "screenshots", "*.png")):
        v = _vendor_from_filename(os.path.basename(f))
        # 최신(파일명 타임스탬프 큰 것) 우선
        if v not in vend or os.path.basename(f) > os.path.basename(vend[v]):
            vend[v] = f
    return vend


def _ocr_header(path: str) -> dict:
    im = Image.open(path)
    w, h = im.size
    crop = im.crop((0, 0, w, min(h, _HEADER_PX)))
    buf = BytesIO()
    crop.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode()
    r = _client.chat.completions.create(
        model=_MODEL,
        messages=[{"role": "user", "content": [
            {"type": "text", "text": _PROMPT},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "high"}},
        ]}],
        response_format={"type": "json_object"},
    )
    d = json.loads(r.choices[0].message.content)
    # 정규화
    tel = d.get("전화") or []
    fax = d.get("팩스") or []
    if isinstance(tel, str): tel = [tel]
    if isinstance(fax, str): fax = [fax]
    return {
        "업체명": (d.get("업체명") or "").strip(),
        "담당자": (d.get("담당자") or "").strip(),
        "전화": [str(t).strip() for t in tel if str(t).strip()],
        "팩스": [str(x).strip() for x in fax if str(x).strip()],
    }


def load_contacts() -> dict:
    if os.path.exists(CONTACTS_PATH):
        try:
            with open(CONTACTS_PATH, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def build_contacts(force: bool = False, max_workers: int = 8) -> dict:
    """신규 업체만 상단 OCR → 연락처 인덱스 갱신. 반환: 전체 인덱스."""
    if not PIL_OK:
        print("PIL 없음 — 연락처 추출 불가")
        return load_contacts()

    idx = {} if force else load_contacts()
    vend = _all_screenshots()
    todo = {v: p for v, p in vend.items() if force or v not in idx}
    print(f"연락처 추출 대상: {len(todo)}업체 (전체 {len(vend)}, 캐시 {len(idx)})")
    if not todo:
        return idx

    def _work(item):
        v, p = item
        try:
            return v, _ocr_header(p)
        except Exception as e:
            return v, {"업체명": "", "담당자": "", "전화": [], "팩스": [], "_err": str(e)}

    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for fut in as_completed([ex.submit(_work, it) for it in todo.items()]):
            v, info = fut.result()
            idx[v] = info
            done += 1
            if done % 20 == 0:
                print(f"  ...{done}/{len(todo)}")

    os.makedirs(os.path.dirname(CONTACTS_PATH), exist_ok=True)
    with open(CONTACTS_PATH, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=False, indent=2)
    print(f"저장: {CONTACTS_PATH} ({len(idx)}업체)")
    return idx


def contact_str(vendor: str, idx: dict = None) -> str:
    """업체명 → '담당자 전화 / 전화2 (팩스 …)' 형태 요약 문자열."""
    idx = idx if idx is not None else load_contacts()
    c = idx.get(vendor)
    if not c:
        return ""
    parts = []
    if c.get("담당자"):
        parts.append(c["담당자"])
    tel = c.get("전화") or []
    if tel:
        parts.append(" / ".join(tel))
    fax = c.get("팩스") or []
    s = " ".join(parts)
    if fax:
        s += f" (F.{fax[0]})"
    return s.strip()


if __name__ == "__main__":
    import sys
    build_contacts(force="--force" in sys.argv)
