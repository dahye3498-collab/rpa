# -*- coding: utf-8 -*-
"""
product_search.py — 품목표 데이터 검색 엔진

- visionmeat/database/*_품목표_데이터.xlsx 를 로드(파일 변경 시 자동 갱신)
- 동의어/OCR 오독 정규화 검색 (끝갈비=립앤드=립엔드=RIB END, 갈갈비·팁앤드 등)
- 창고 오독 보정 (강한1→강동1 등)
- 결과에 원본 스크린샷 경로(수집일/파일명) 포함
"""
import os
import re
import glob
import pandas as pd

try:
    import contacts as _contacts
except Exception:
    _contacts = None

try:
    import warehouses as _wh
except Exception:
    _wh = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "visionmeat", "database")

# ── 품목 동의어 그룹 (첫 항목이 대표명). OCR 오독 변형도 함께 넣어 검색 누락 방지 ──
SYNONYM_GROUPS = [
    ["끝갈비", "립앤드", "립엔드", "리브앤드", "rib end", "ribend", "갈갈비", "팁앤드", "립앤", "립엔"],
    ["등갈비", "백립", "빽립", "back rib", "등뼈"],
    ["살치살", "살치"],
    ["아롱사태", "아롱"],
    ["부채살", "부채"],
    ["삼겹양지", "삼겹", "양지"],
    ["차돌박이", "차돌"],
    ["척아이롤", "척아이"],
]

# ── 창고 OCR 오독 보정 ──
WAREHOUSE_FIX = {
    "강한1": "강동1", "강동l": "강동1", "강동I": "강동1",
    "강한2": "강동2",
}

# 검색 가능한 필드
SEARCH_FIELDS = ["품목", "브랜드", "창고", "원산지", "등급", "전체"]


def _norm(s) -> str:
    return re.sub(r"\s+", "", str(s if s is not None else "")).lower()


# variant(정규화) -> 대표명
_VAR2CANON = {}
for _g in SYNONYM_GROUPS:
    for _v in _g:
        _VAR2CANON[_norm(_v)] = _g[0]


def expand_query(q: str) -> set:
    """검색어를 동의어 그룹으로 확장한 정규화 term 집합 반환."""
    nq = _norm(q)
    if not nq:
        return set()
    terms = {nq}
    canon = _VAR2CANON.get(nq)
    if not canon:
        for v, c in _VAR2CANON.items():
            if nq in v or v in nq:
                canon = c
                break
    if canon:
        for g in SYNONYM_GROUPS:
            if g[0] == canon:
                terms |= {_norm(v) for v in g}
                break
    return terms


_cache = {"sig": None, "rows": []}


def _db_signature():
    files = sorted(glob.glob(os.path.join(DB_DIR, "*_품목표_데이터.xlsx")))
    sig = [(f, os.path.getmtime(f)) for f in files]
    # 연락처 인덱스 변경도 감지 (재빌드 시 검색 결과 갱신)
    cpath = os.path.join(DB_DIR, "vendor_contacts.json")
    if os.path.exists(cpath):
        sig.append((cpath, os.path.getmtime(cpath)))
    return tuple(sig)


def load_rows(force: bool = False) -> list:
    """DB 폴더의 품목표 xlsx 전체 로드(변경 감지 캐시)."""
    sig = _db_signature()
    if not force and _cache["sig"] == sig:
        return _cache["rows"]

    contact_idx = _contacts.load_contacts() if _contacts else {}

    rows = []
    for f, _ in sig:
        try:
            df = pd.read_excel(f).fillna("")
        except Exception:
            continue
        for rec in df.to_dict("records"):
            r = {k: ("" if (v is None or (isinstance(v, float) and pd.isna(v))) else v) for k, v in rec.items()}
            wh = str(r.get("창고", "")).strip()
            r["창고"] = _wh.canonicalize(wh) if _wh else WAREHOUSE_FIX.get(wh, wh)
            # 업체명(파일명에서 _타임스탬프.png 제거)
            fn = str(r.get("파일명", ""))
            vendor = re.sub(r"_\d+\.(png|jpg|jpeg)$", "", fn, flags=re.I)
            r["업체"] = vendor
            # 연락처 첨부
            c = contact_idx.get(vendor) or {}
            r["담당자"] = c.get("담당자", "")
            r["전화"] = " / ".join(c.get("전화", []) or [])
            r["팩스"] = " / ".join(c.get("팩스", []) or [])
            r["연락처"] = _contacts.contact_str(vendor, contact_idx) if _contacts else ""
            rows.append(r)

    _cache["sig"] = sig
    _cache["rows"] = rows
    return rows


def search(q: str = "", warehouse: str = "", origin: str = "",
           brand: str = "", field: str = "품목", limit: int = 1000) -> dict:
    """
    품목 검색. 반환: {"count": 전체매칭수, "results": [행,...] (limit까지)}
    """
    rows = load_rows()
    terms = expand_query(q) if q else None
    wq, oq, bq = _norm(warehouse), _norm(origin), _norm(brand)
    field = field if field in SEARCH_FIELDS else "품목"

    out = []
    for r in rows:
        if terms:
            if field == "전체":
                hay = _norm(" ".join(str(v) for k, v in r.items() if k not in ("파일명",)))
            else:
                hay = _norm(r.get(field, ""))
            if not any(t and t in hay for t in terms):
                continue
        if wq and wq not in _norm(r.get("창고", "")):
            continue
        if oq and oq not in _norm(r.get("원산지", "")):
            continue
        if bq and bq not in _norm(r.get("브랜드", "")):
            continue
        out.append(r)

    return {"count": len(out), "results": out[:limit]}


def stats() -> dict:
    rows = load_rows()
    dates = sorted({str(r.get("수집일", "")) for r in rows if r.get("수집일")})
    vendors = {str(r.get("업체", "")) for r in rows if r.get("업체")}
    return {"total_rows": len(rows), "dates": dates, "vendor_count": len(vendors)}
