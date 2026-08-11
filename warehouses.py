# -*- coding: utf-8 -*-
"""
warehouses.py — 창고 표준 목록 로더 + 정규화

- 표준 목록 원본: ref/창고_목록.md (사용자 관리)
- OCR 프롬프트에 목록을 주입해 창고명을 표준으로 정규화(prompt_block)
- 검색/적재 시 보수적 정규화(canonicalize): 정확 일치 + 알려진 오독만 보정
  (강동1/강동2처럼 한 글자 차이는 절대 자동 교정하지 않음)
"""
import os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MD_PATH = os.path.join(BASE_DIR, "ref", "창고_목록.md")

# 알려진 OCR 오독 보정 (안전한 것만 명시적으로)
KNOWN_FIX = {
    "강한1": "강동1",
    "강한2": "강동2",
    "강동l": "강동1",
    "강동I": "강동1",
    "오로라cs": "오로라CS",
}


def load_warehouses() -> list:
    out = []
    if os.path.exists(MD_PATH):
        for ln in open(MD_PATH, encoding="utf-8"):
            s = ln.strip()
            if not s or s.startswith("#") or s.startswith(">"):
                continue
            s = re.sub(r"^[-*]\s+", "", s)  # 혹시 모를 불릿 제거
            out.append(s)
    # 중복 제거(순서 유지)
    seen, res = set(), []
    for w in out:
        if w not in seen:
            seen.add(w)
            res.append(w)
    return res


WAREHOUSES = load_warehouses()


def _norm(s) -> str:
    return re.sub(r"\s+", "", str(s if s is not None else "")).lower()


_NORM_MAP = {_norm(w): w for w in WAREHOUSES}


def _canon_one(token: str) -> str:
    t = str(token or "").strip()
    if not t:
        return t
    n = _norm(t)
    if n in KNOWN_FIX:              # 알려진 오독
        return KNOWN_FIX[n]
    if n in _NORM_MAP:             # 표준 목록과 정확 일치(띄어쓰기/대소문자만 보정)
        return _NORM_MAP[n]
    return t                       # 불확실하면 원문 유지(오매핑 방지)


def canonicalize(name: str) -> str:
    """창고 값 보수적 정규화. 여러 창고는 / , 로 분리해 각각 처리 후 ' / '로 합침."""
    raw = str(name or "").strip()
    if not raw:
        return raw
    parts = re.split(r"\s*[/,]\s*", raw)
    out = [_canon_one(p) for p in parts if p.strip()]
    return " / ".join(out) if out else raw


def prompt_block() -> str:
    """OCR 프롬프트에 주입할 창고 표준 목록 블록."""
    return (
        "창고(보관위치) 표준 목록 — 창고 값은 아래 중 가장 가까운 이름으로 정규화하세요. "
        "목록에 없으면 원문 그대로 두고, '강동1'/'강동2'처럼 숫자만 다른 것은 혼동 금지. "
        "한 셀에 여러 창고면 ' / '로 모두 유지:\n"
        + ", ".join(WAREHOUSES)
    )
