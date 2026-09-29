# -*- coding: utf-8 -*-
"""
품목표 데이터에서 [브랜드 + 품목] 을 뽑아 카톡 보고용 단가조사 양식으로 출력.

표기 정규화는 전부 normalize.Master([운영] 자동변환_소스데이터.xlsx) 에 위임한다.
이 파일에는 동의어를 하드코딩하지 않는다.

사용:
    python price_survey.py <품목표_데이터.xlsx> <브랜드> <품목>
        [--master "[운영] 자동변환_소스데이터.xlsx"]
        [--price "(주)푸드앤팜=3,000원"]

예:
    python price_survey.py 20260812_품목표_데이터.xlsx 놀란 깐양 --price "(주)푸드앤팜=3,000원"
    python price_survey.py 20260812_품목표_데이터.xlsx TRUEWEST 빽립
"""

import os
import re
import json
import argparse

import openpyxl

from normalize import Master, clean

DEFAULT_MASTER = "[운영] 자동변환_소스데이터.xlsx"


# ---------------------------------------------------------------------------
# 업체명
# ---------------------------------------------------------------------------

def load_vendors(*dirs):
    for d in dirs:
        p = os.path.join(d, "vendor_contacts.json")
        if os.path.exists(p):
            try:
                with open(p, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    return {}


def prettify_stem(stem):
    """연락처 DB에 업체명이 비었을 때 파일명으로 표기 복원. '주캐시카우' -> '(주)캐시카우'"""
    s = stem.replace("_", " ").strip()
    if s.startswith("주식회사"):
        return s
    if s.startswith("주") and len(s) > 2:
        return "(주)" + s[1:].strip()
    if s.endswith("주") and len(s) > 2:
        return s[:-1].strip() + "(주)"
    return s


def vendor_name(filename, vendors):
    """'주팬텀미트_1786496884.png' -> '(주)팬텀미트'"""
    stem = re.sub(r"_\d+\.(png|jpg|jpeg)$", "", clean(filename), flags=re.I)
    hit = vendors.get(stem)
    if hit is None:
        for k, v in vendors.items():
            if k.replace("_", "") == stem.replace("_", ""):
                hit = v
                break
    if hit:
        name = clean(hit.get("업체명"))
        if name:
            return name
    return prettify_stem(stem)


# ---------------------------------------------------------------------------
# 조회 / 출력
# ---------------------------------------------------------------------------

def load_rows(path):
    ws = openpyxl.load_workbook(path, data_only=True).worksheets[0]
    rows = list(ws.iter_rows(values_only=True))
    hdr = [str(h) if h is not None else "" for h in rows[0]]
    return [dict(zip(hdr, r)) for r in rows[1:]]


def build_line(r, item, price, master):
    """'깐양 호주 냉동 3,000원 (유상) *평중 16.56kg, 소비기한 28.01'"""
    head = [item]
    for k in ("원산지", "보관"):
        v = clean(r.get(k))
        if v:
            head.append(v)
    head.append(price or "가격미확인")

    wh = master.warehouse(r.get("창고"))
    if wh:
        head.append("(%s)" % wh)

    notes = []
    w = clean(r.get("평중_kg"))
    if w:
        notes.append("평중 %skg" % w)
    exp = clean(r.get("소비기한"))
    if exp:
        notes.append("소비기한 %s" % exp)
    grade = clean(r.get("등급"))
    if grade and grade != "-":
        notes.append(grade)
    stock = clean(r.get("재고_box"))
    if stock:
        notes.append("재고 %sbox" % stock)

    line = " ".join(head)
    if notes:
        line += " *" + ", ".join(notes)
    return line


def survey(path, brand, item, prices=None, master_path=DEFAULT_MASTER, title_date=None):
    prices = prices or {}
    data_dir = os.path.dirname(os.path.abspath(path))
    master = Master(master_path)
    vendors = load_vendors(data_dir, os.path.dirname(os.path.abspath(master_path)))

    rows = load_rows(path)
    q_brand = master.brand(brand)
    q_item = master.item(item)

    hits = [r for r in rows
            if master.brand(r.get("브랜드")) == q_brand
            and master.item(r.get("품목")) == q_item]

    groups, order = {}, []
    for r in hits:
        name = vendor_name(r.get("파일명"), vendors)
        if name not in groups:
            groups[name] = []
            order.append(name)
        groups[name].append(r)

    date = title_date
    if not date and hits:
        date = clean(hits[0].get("수집일"))
    if date and "-" in str(date):
        parts = str(date).split("-")
        date = "%d/%d" % (int(parts[1]), int(parts[2]))

    out = ["[%s %s 단가조사] %s" % (q_brand, q_item, date or ""), ""]
    if not hits:
        out.append("해당 브랜드 품목 없음")
        return "\n".join(out)

    for name in order:
        out.append("-- %s --" % name)
        for r in groups[name]:
            out.append(build_line(r, q_item, prices.get(name), master))
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx")
    ap.add_argument("brand")
    ap.add_argument("item")
    ap.add_argument("--master", default=DEFAULT_MASTER)
    ap.add_argument("--price", action="append", default=[],
                    help='회신받은 단가. 예: --price "(주)푸드앤팜=3,000원"')
    ap.add_argument("--date", default=None)
    a = ap.parse_args()

    prices = {}
    for p in a.price:
        if "=" in p:
            k, v = p.split("=", 1)
            prices[k.strip()] = v.strip()

    print(survey(a.xlsx, a.brand, a.item, prices, a.master, a.date))


if __name__ == "__main__":
    main()
