# -*- coding: utf-8 -*-
"""
vision-meat 표기 정규화 엔진
[운영] 자동변환_소스데이터.xlsx 를 단일 기준(master)으로 삼는다.

시트 구조 (A열 = 정식명, B열~ = 변형표기)
    브랜드명_한글사용 / 품목명_한글사용 / 창고명_한글사용 / 축종_한글사용

핵심 원칙
  1. 정규화 기준은 오직 마스터 엑셀. 코드에 동의어를 하드코딩하지 않는다.
     (규칙 추가/수정은 리즈가 엑셀만 고치면 됨 → 코드 배포 불필요)
  2. 마스터에 없는 표기는 임의로 바꾸지 않는다. 원문 유지 + 미등록으로 보고.
  3. OCR 오탈자는 초성 혼동 보정으로 한 번 더 시도하되,
     후보가 둘 이상이면 손대지 않고 사람이 판단하도록 넘긴다.

사용
    from normalize import Master
    m = Master("[운영] 자동변환_소스데이터.xlsx")

    m.item("빽립")        -> '등갈비'
    m.item("찐양")        -> '깐양'      (초성 보정)
    m.brand("놀란")       -> 'NOLAN'
    m.warehouse("아주기흥") -> 'ACE 기흥'
    m.species("소")       -> '우육'

    m.unknowns(rows)     -> 마스터 미등록 표기 빈도표 (엑셀에 추가할 후보)

외부 의존성: openpyxl (엑셀 읽기) 만. 나머지는 표준 라이브러리.
"""

import os
import re
import json
import unicodedata

SHEETS = {
    "brand": "브랜드명_한글사용",
    "item": "품목명_한글사용",
    "warehouse": "창고명_한글사용",
    "species": "축종_한글사용",
}

COLUMN_OF = {          # 파싱 결과 dict 의 열 이름 -> 카테고리
    "브랜드": "brand",
    "품목": "item",
    "창고": "warehouse",
    "축종": "species",
}

# ---------------------------------------------------------------------------
# 한글 자모 (OCR 초성 오인식 보정)
# ---------------------------------------------------------------------------

CHO = ["ㄱ", "ㄲ", "ㄴ", "ㄷ", "ㄸ", "ㄹ", "ㅁ", "ㅂ", "ㅃ", "ㅅ", "ㅆ",
       "ㅇ", "ㅈ", "ㅉ", "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ"]
JUNG = ["ㅏ", "ㅐ", "ㅑ", "ㅒ", "ㅓ", "ㅔ", "ㅕ", "ㅖ", "ㅗ", "ㅘ", "ㅙ", "ㅚ",
        "ㅛ", "ㅜ", "ㅝ", "ㅞ", "ㅟ", "ㅠ", "ㅡ", "ㅢ", "ㅣ"]
JONG = ["", "ㄱ", "ㄲ", "ㄳ", "ㄴ", "ㄵ", "ㄶ", "ㄷ", "ㄹ", "ㄺ", "ㄻ", "ㄼ",
        "ㄽ", "ㄾ", "ㄿ", "ㅀ", "ㅁ", "ㅂ", "ㅄ", "ㅅ", "ㅆ", "ㅇ", "ㅈ",
        "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ"]

# 스캔본에서 실제로 뭉개져 뒤바뀌는 조합만. 넓히면 오탐이 급증한다.
CHO_CONFUSION = [
    {"ㄱ", "ㄲ", "ㅋ", "ㅈ", "ㅉ", "ㅊ"},   # 깐/간/칸/찐/진, 빽/뺵 계열
    {"ㄷ", "ㄸ", "ㅌ"},
    {"ㅂ", "ㅃ", "ㅍ"},                     # 볼/뽈/폴, 백/빽
    {"ㅅ", "ㅆ"},
    {"ㅇ", "ㅎ"},
]
JUNG_CONFUSION = [
    {"ㅏ", "ㅑ"}, {"ㅓ", "ㅕ"}, {"ㅗ", "ㅛ"}, {"ㅜ", "ㅠ"}, {"ㅡ", "ㅣ"},
]


def _decompose(ch):
    code = ord(ch) - 0xAC00
    if code < 0 or code > 11171:
        return None
    return CHO[code // 588], JUNG[(code % 588) // 28], JONG[code % 28]


def _compose(cho, jung, jong=""):
    return chr(0xAC00 + CHO.index(cho) * 588 + JUNG.index(jung) * 28 + JONG.index(jong))


def _rep(x, groups, order):
    for g in groups:
        if x in g:
            return sorted(g, key=order.index)[0]
    return x


def fuzzy_key(word):
    """OCR 혼동 자모를 대표값으로 눌러버린 검색 키. '빽립'과 '백립'이 같은 키."""
    out = []
    for ch in word:
        d = _decompose(ch)
        if d is None:
            out.append(ch)
            continue
        cho, jung, jong = d
        out.append(_compose(_rep(cho, CHO_CONFUSION, CHO),
                            _rep(jung, JUNG_CONFUSION, JUNG),
                            jong))
    return "".join(out)


# ---------------------------------------------------------------------------
# 문자열 전처리
# ---------------------------------------------------------------------------

_NOISE = re.compile(r"[​﻿\xa0]")
_BARE = re.compile(r"[\s()（）\[\]·.\-_/]")

# 창고 열에 섞여 들어오는 상태·안내 문구. 창고가 아니라 비고로 보낸다.
# (예정 / 문의 / 판매완료 / 통관예정 / 입항예정 / 품절 / 집중판매 ...)
STATUS_RE = re.compile(
    r"예정|완료|판매중|판매종료|품절|종료|통관|입고|입항|문의|집중|"
    r"발주|생산중|재고없|소진|마감|\d+월\s*중순|\d+/\d+\s*입고"
)


def clean(s):
    if s is None:
        return ""
    s = unicodedata.normalize("NFC", str(s))
    s = _NOISE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def bare(s):
    """비교용 축약형: 공백/괄호/기호 제거 + 대문자화."""
    return _BARE.sub("", clean(s)).upper()


# ---------------------------------------------------------------------------
# 마스터
# ---------------------------------------------------------------------------

class Master(object):

    def __init__(self, xlsx_path, cache=True):
        self.path = xlsx_path
        self.exact = {}    # category -> {bare(alias): 정식명}
        self.fuzzy = {}    # category -> {fuzzy_key: 정식명 or None(모호)}
        self.canon = {}    # category -> [정식명...]
        self._load(cache)

    # -- 로딩 ---------------------------------------------------------------

    def _cache_path(self):
        return os.path.join(os.path.dirname(os.path.abspath(self.path)),
                            ".master_cache.json")

    def _load(self, cache):
        cp = self._cache_path()
        try:
            if cache and os.path.exists(cp) and \
                    os.path.getmtime(cp) >= os.path.getmtime(self.path):
                with open(cp, encoding="utf-8") as f:
                    d = json.load(f)
                self.exact, self.fuzzy, self.canon = d["exact"], d["fuzzy"], d["canon"]
                return
        except Exception:
            pass

        self._read_xlsx()

        if cache:
            try:
                with open(cp, "w", encoding="utf-8") as f:
                    json.dump({"exact": self.exact, "fuzzy": self.fuzzy,
                               "canon": self.canon}, f, ensure_ascii=False)
            except Exception:
                pass

    def _read_xlsx(self):
        import openpyxl
        wb = openpyxl.load_workbook(self.path, data_only=True, read_only=True)

        for cat, sheet in SHEETS.items():
            if sheet not in wb.sheetnames:
                continue
            exact, fuzzy, canon = {}, {}, []
            for row in wb[sheet].iter_rows(values_only=True):
                vals = [clean(c) for c in row if clean(c)]
                if not vals:
                    continue
                head = vals[0]
                canon.append(head)
                for alias in vals:                 # 정식명 자신도 별칭에 포함
                    b = bare(alias)
                    if b:
                        exact.setdefault(b, head)
                    fk = fuzzy_key(b)
                    if not fk:
                        continue
                    if fk in fuzzy and fuzzy[fk] != head:
                        fuzzy[fk] = None           # 후보 충돌 -> 자동보정 금지
                    else:
                        fuzzy.setdefault(fk, head)
            self.exact[cat] = exact
            self.fuzzy[cat] = fuzzy
            self.canon[cat] = canon
        wb.close()

    # -- 정규화 -------------------------------------------------------------

    def resolve(self, value, category, use_fuzzy=True):
        """
        (정식명, 매칭방식) 반환.
        매칭방식: 'exact' | 'part' | 'fuzzy' | ''(미등록)
        미등록이면 원문(공백정리본)을 그대로 돌려준다. 임의 치환은 하지 않는다.
        """
        s = clean(value)
        if not s:
            return "", ""
        exact = self.exact.get(category, {})

        b = bare(s)
        if b in exact:
            return exact[b], "exact"

        # 'EXCEL(엑셀)' / 'TEYS [티스]' 처럼 괄호로 병기된 경우 조각별로 재시도
        for part in re.split(r"[/,·\[\]()（）]", s):
            pb = bare(part)
            if pb and pb in exact:
                return exact[pb], "part"

        # 'EXCEL 엑셀' / '스탠브룩 GF' 처럼 괄호 없이 띄어쓴 경우.
        # 해석되는 토큰들이 모두 같은 정식명을 가리킬 때만 채택한다.
        # 'EXCEL SWIFT'(서로 다른 두 브랜드)는 후보가 2개라 손대지 않는다.
        if " " in s:
            found = set()
            for tok in s.split(" "):
                tb = bare(tok)
                if tb and tb in exact:
                    found.add(exact[tb])
            if len(found) == 1:
                return found.pop(), "token"

        # '냉장 부채살' / '동결삼겹' 처럼 보관상태가 품목 앞에 붙은 경우
        if category == "item":
            stripped = re.sub(r"^(냉장|냉동|동결|급냉|상온)\s*", "", s)
            if stripped != s:
                sb = bare(stripped)
                if sb in exact:
                    return exact[sb], "storage"

        # '강동2냉장' / 'CH물류' / '프라자로지스' / '에이스처인사업소' 처럼
        # 창고명 뒤에 부속어가 붙은 경우
        if category == "warehouse":
            stripped = re.sub(r"(냉장|냉동|물류|로지스|창고|자창|사업소)+$", "", s).strip()
            if stripped and stripped != s:
                sb = bare(stripped)
                if sb in exact:
                    return exact[sb], "suffix"
                hit = self.fuzzy.get(category, {}).get(fuzzy_key(sb))
                if use_fuzzy and hit:
                    return hit, "suffix+fuzzy"

        if use_fuzzy:
            hit = self.fuzzy.get(category, {}).get(fuzzy_key(b))
            if hit:
                return hit, "fuzzy"

        return s, ""

    def normalize(self, value, category, use_fuzzy=True):
        return self.resolve(value, category, use_fuzzy)[0]

    def split_status(self, value):
        """
        창고 셀에 섞여 들어온 상태·안내 문구를 분리한다.
        '예정' '문의' '판매완료' '통관예정' 등은 창고가 아니라 비고에 들어가야 한다.

            split_status("효성냉장(입항예정)") -> ("효성냉장", "입항예정")
            split_status("예정")              -> ("", "예정")
            split_status("강동1")             -> ("강동1", "")

        반환: (창고로 남길 부분, 비고로 뺄 부분)
        """
        s = clean(value)
        if not s:
            return "", ""

        keep, notes = [], []
        # 괄호 안 상태문구를 먼저 분리: '효성냉장(입항예정)'
        def _pull(m):
            inner = clean(m.group(1))
            if inner and STATUS_RE.search(inner):
                notes.append(inner)
                return " "
            return m.group(0)

        s = re.sub(r"[(（]([^)）]*)[)）]", _pull, s)

        for part in re.split(r"[,/|]|\s{2,}", s):
            p = clean(part)
            if not p:
                continue
            if STATUS_RE.search(p) and not self.is_known(p, "warehouse"):
                notes.append(p)
            else:
                keep.append(p)

        return ", ".join(keep), ", ".join(notes)

    def normalize_multi(self, value, category, sep=", "):
        """
        '강동2 / 마장동', '처인냉장,마장동매장' 처럼 한 셀에 여러 값이 든 경우
        각각 정규화해 합쳐 돌려준다. 단일값이면 normalize() 와 동일.
        """
        s = clean(value)
        if not s:
            return ""
        parts = [p for p in re.split(r"[,/|]|\s{2,}", s) if clean(p)]
        if len(parts) < 2:
            return self.normalize(s, category)
        out = []
        for p in parts:
            v = self.normalize(p, category)
            if v and v not in out:
                out.append(v)
        return sep.join(out)

    def is_known(self, value, category, use_fuzzy=True):
        """마스터로 정식명이 확정되면 True. 'EXCEL(엑셀)' 같은 병기형도 True."""
        return bool(self.resolve(value, category, use_fuzzy)[1])

    def brand(self, v):
        return self.normalize(v, "brand")

    def item(self, v):
        return self.normalize(v, "item")

    def warehouse(self, v):
        return self.normalize(v, "warehouse")

    def species(self, v):
        return self.normalize(v, "species")

    # -- 행 단위 적용 -------------------------------------------------------

    def apply_row(self, row):
        """
        파싱 결과 dict 한 줄을 정규화한 새 dict 로 반환.
        창고 열의 상태문구는 비고로 옮긴다.
        """
        out = dict(row)

        if "창고" in out:
            wh, note = self.split_status(out.get("창고"))
            out["창고"] = self.normalize_multi(wh, "warehouse")
            if note:
                prev = clean(out.get("비고"))
                out["비고"] = (prev + ", " + note) if prev else note

        for col, cat in COLUMN_OF.items():
            if col == "창고" or col not in out:
                continue
            out[col] = self.normalize(out.get(col), cat)
        return out

    def apply_rows(self, rows):
        return [self.apply_row(r) for r in rows]

    # -- 미등록 표기 수집 ---------------------------------------------------

    def unknowns(self, rows, min_count=1):
        """
        마스터에 없는 표기를 카테고리별 빈도순으로 반환.
        여기 올라온 것들을 엑셀에 추가하면 다음 회차부터 자동 정규화된다.
        반환: {category: [(원문, 횟수, 추정정식명 or ''), ...]}
        """
        seen = {}
        for r in rows:
            for col, cat in COLUMN_OF.items():
                v = clean(r.get(col))
                if cat == "warehouse":
                    v = self.split_status(v)[0]      # 상태문구는 비고행이므로 제외
                if not v or self.is_known(v, cat):
                    continue
                seen.setdefault(cat, {})
                seen[cat][v] = seen[cat].get(v, 0) + 1

        out = {}
        for cat, d in seen.items():
            items = []
            for v, n in d.items():
                if n < min_count:
                    continue
                guess = self.fuzzy.get(cat, {}).get(fuzzy_key(bare(v))) or ""
                items.append((v, n, guess))
            items.sort(key=lambda x: (-x[1], x[0]))
            out[cat] = items
        return out


# ---------------------------------------------------------------------------
# CLI: 미등록 표기 점검
#   python normalize.py "[운영] 자동변환_소스데이터.xlsx" 20260812_품목표_데이터.xlsx
# ---------------------------------------------------------------------------

def _load_rows(path):
    import openpyxl
    ws = openpyxl.load_workbook(path, data_only=True).worksheets[0]
    rows = list(ws.iter_rows(values_only=True))
    hdr = [str(h) if h is not None else "" for h in rows[0]]
    return [dict(zip(hdr, r)) for r in rows[1:]]


if __name__ == "__main__":
    import sys

    master_path = sys.argv[1]
    m = Master(master_path)
    print("마스터 로드: " + ", ".join(
        "%s %d개" % (SHEETS[c], len(m.canon.get(c, []))) for c in SHEETS))

    if len(sys.argv) < 3:
        for t in ["빽립", "찐양", "아주기흥", "놀란", "한라동탄", "소"]:
            for cat in ("item", "warehouse", "brand", "species"):
                r = m.normalize(t, cat)
                if r != t:
                    print("  %-10s [%s] -> %s" % (t, cat, r))
        sys.exit()

    rows = _load_rows(sys.argv[2])
    print("대상 %d행\n" % len(rows))
    unk = m.unknowns(rows, min_count=2)
    for cat in ("item", "brand", "warehouse", "species"):
        lst = unk.get(cat, [])
        print("[%s] 미등록 %d종" % (SHEETS[cat], len(lst)))
        for v, n, guess in lst[:15]:
            print("   %-28s %3d회 %s" % (v[:28], n, ("-> " + guess) if guess else ""))
        print()
