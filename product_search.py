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
import difflib
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
    # 브랜드 한글↔영문 별칭 (소스에 없는 것 보완)
    ["TRUE WEST", "트루웨스트", "TRUEWEST", "트루웨스트"],
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


# exact(정확히 일치) 모드에서 품목/브랜드는 '낱말' 매칭, 부가필드는 부분일치 유지
_AUX_FIELDS = ("축종", "원산지", "보관", "등급", "EST", "스펙_설명",
               "재고_box", "창고", "소비기한", "수정일", "비고")


def _word_match(term: str, value) -> bool:
    """term이 value(품목/브랜드)와 '한 낱말'로 맞는지 — 완전일치 또는 접두일치.
    내부/접미 부분일치는 제외 → '전지'가 '목전지'에는 안 걸림('전지','전지살'은 걸림)."""
    v = _norm(value)
    if not term or not v:
        return False
    return v == term or v.startswith(term)


def _match_terms(r: dict, terms: set, field: str, exact: bool) -> bool:
    """행 r 이 검색어 terms 에 매칭되는지. exact=False면 기존 부분일치."""
    if not exact:
        if field == "전체":
            hay = _norm(" ".join(str(v) for k, v in r.items() if k != "파일명"))
        else:
            hay = _norm(r.get(field, ""))
        return any(t and t in hay for t in terms)

    # exact 모드: 품목/브랜드는 낱말(완전/접두) 일치
    def hit_item(val):
        return any(_word_match(t, val) for t in terms)

    if field == "품목":
        return hit_item(r.get("품목", ""))
    if field == "브랜드":
        return hit_item(r.get("브랜드", ""))
    if field in ("창고", "원산지", "등급"):
        hay = _norm(r.get(field, ""))
        return any(t and t in hay for t in terms)
    # 전체: 품목·브랜드는 낱말 일치, 그 외 부가필드는 부분일치
    if hit_item(r.get("품목", "")) or hit_item(r.get("브랜드", "")):
        return True
    aux = _norm(" ".join(str(r.get(k, "")) for k in _AUX_FIELDS))
    return any(t and t in aux for t in terms)


# ── 소스데이터 동의어 그룹 로드 (품목/브랜드/축종) ──
SOURCE_XLSX = os.path.join(BASE_DIR, "[운영] 자동변환_소스데이터.xlsx")


def _load_source_groups(sheets) -> list:
    """각 행 = [표준명, 동의어1, 동의어2, ...] 형태 시트를 그룹 리스트로 로드."""
    groups = []
    for sh in sheets:
        try:
            df = pd.read_excel(SOURCE_XLSX, sheet_name=sh, header=None)
        except Exception:
            continue
        for _, r in df.iterrows():
            vals = [str(x).strip() for x in r.tolist()
                    if str(x).strip() and str(x).strip().lower() != "nan"]
            if len(vals) >= 2:  # 표준 + 동의어 최소 1개
                groups.append(vals)
    return groups


# 하드코딩 그룹(OCR 오독 포함) + 소스데이터 그룹(품목·브랜드·축종)
ALL_GROUPS = SYNONYM_GROUPS + _load_source_groups(
    ["품목명_한글사용", "브랜드명_한글사용", "축종_한글사용"]
)
_GROUPS_NORM = [[_norm(v) for v in g] for g in ALL_GROUPS]


def expand_query(q: str) -> set:
    """검색어가 어떤 동의어 그룹의 정확한 변형이면 그 그룹 전체(정규화)로 확장."""
    nq = _norm(q)
    if not nq:
        return set()
    terms = {nq}
    for gn in _GROUPS_NORM:
        if nq in gn:
            terms |= set(gn)
    return terms


# ── 한글 자모 분해 기반 퍼지(오타) 매칭 ──
_CHO = list("ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ")
_JUNG = list("ㅏㅐㅑㅒㅓㅔㅕㅖㅗㅘㅙㅚㅛㅜㅝㅞㅟㅠㅡㅢㅣ")
_JONG = [""] + list("ㄱㄲㄳㄴㄵㄶㄷㄹㄺㄻㄼㄽㄾㄿㅀㅁㅂㅄㅅㅆㅇㅈㅊㅋㅌㅍㅎ")


def _decompose(s) -> str:
    """한글 음절을 자모로 분해(오타 1~2글자 차이를 잘 잡기 위함). 비한글은 소문자."""
    out = []
    for ch in str(s):
        c = ord(ch)
        if 0xAC00 <= c <= 0xD7A3:
            i = c - 0xAC00
            out.append(_CHO[i // 588])
            out.append(_JUNG[(i % 588) // 28])
            j = i % 28
            if j:
                out.append(_JONG[j])
        elif not ch.isspace():
            out.append(ch.lower())
    return "".join(out)


_fuzzy_cache = {"sig": None, "vals": [], "decomp": []}


def _distinct_values():
    """검색 대상 고유값(품목·브랜드) 목록 캐시 (데이터 변경 시 갱신)."""
    sig = _db_signature()
    if _fuzzy_cache["sig"] == sig and _fuzzy_cache["vals"]:
        return _fuzzy_cache
    vals = set()
    for r in load_rows():
        for k in ("품목", "브랜드"):
            v = str(r.get(k, "")).strip()
            if 2 <= len(v) <= 24:
                vals.add(v)
    vals = sorted(vals)
    _fuzzy_cache.update(sig=sig, vals=vals, decomp=[_decompose(v) for v in vals])
    return _fuzzy_cache


def fuzzy_matches(q: str, cutoff: float = 0.82, limit: int = 12) -> list:
    """검색어와 자모 유사도가 높은 실제 품목/브랜드 값을 반환(오타 보정)."""
    dq = _decompose(q)
    if len(dq) < 3:
        return []
    dc = _distinct_values()
    scored = []
    for v, dv in zip(dc["vals"], dc["decomp"]):
        if not dv:
            continue
        ratio = difflib.SequenceMatcher(None, dq, dv).ratio()
        if ratio >= cutoff:
            scored.append((ratio, v))
    scored.sort(reverse=True)
    return [v for _, v in scored[:limit]]


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


def recent_dates(n: int) -> list:
    """가장 최근 수집일 n개(내림차순). n<=0이면 전체."""
    rows = load_rows()
    ds = sorted({str(r.get("수집일", "")) for r in rows if str(r.get("수집일", "")).strip()}, reverse=True)
    return ds[:n] if n and n > 0 else ds


def search(q: str = "", warehouse: str = "", origin: str = "",
           brand: str = "", field: str = "전체", limit: int = 1000,
           recent: int = 3, exact: bool = False) -> dict:
    """
    품목 검색. recent=최근 수집일 N개만 조회(기본 3, 0이면 전체).
    exact=True면 품목/브랜드를 낱말(완전/접두) 일치로 검색 (예: '전지'에 '목전지' 제외).
    반환: {"count": 전체매칭수, "results": [행,...] (limit까지), "dates": 조회된 날짜}
    """
    rows = load_rows()
    terms = expand_query(q) if q else None
    # 오타 보정: 검색어와 자모 유사한 실제 품목/브랜드도 검색어에 포함
    if q and terms is not None:
        for m in fuzzy_matches(q):
            terms |= expand_query(m)
    wq, oq, bq = _norm(warehouse), _norm(origin), _norm(brand)
    field = field if field in SEARCH_FIELDS else "품목"

    allowed = set(recent_dates(recent)) if (recent and recent > 0) else None

    out = []
    for r in rows:
        if allowed is not None and str(r.get("수집일", "")) not in allowed:
            continue
        if terms:
            if not _match_terms(r, terms, field, exact):
                continue
        if wq and wq not in _norm(r.get("창고", "")):
            continue
        if oq and oq not in _norm(r.get("원산지", "")):
            continue
        if bq and bq not in _norm(r.get("브랜드", "")):
            continue
        out.append(r)

    return {
        "count": len(out),
        "results": out[:limit],
        "dates": sorted(allowed, reverse=True) if allowed is not None else "전체",
    }


def stats() -> dict:
    rows = load_rows()
    dates = sorted({str(r.get("수집일", "")) for r in rows if r.get("수집일")})
    vendors = {str(r.get("업체", "")) for r in rows if r.get("업체")}
    return {"total_rows": len(rows), "dates": dates, "vendor_count": len(vendors)}
