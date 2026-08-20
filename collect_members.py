# -*- coding: utf-8 -*-
"""회원정보 게시판 특정 연도 전체 텍스트 수집 (스크린샷 없이 텍스트만).

- 게시판을 1회 순회하며 해당 연도(기본 2026) 글을 모두 텍스트로 긁어 Excel 저장.
- 결과:
    visionmeat/<연도>_전체/회원정보/excel/<연도>_회원정보_데이터.xlsx
    visionmeat/database/<연도>_회원정보_데이터.xlsx   (검색/취합용 사본)
- OCR 불필요(텍스트 게시판) → 과금 없음.

실행:
    venv/Scripts/python.exe collect_members.py                 # 2026년 회원정보
    venv/Scripts/python.exe collect_members.py --year 2026
    venv/Scripts/python.exe collect_members.py --board 등업신청 --year 2026
"""
import sys
import argparse
sys.stdout.reconfigure(encoding="utf-8")

from datetime import datetime, date
import rpa_automation as R


def main():
    ap = argparse.ArgumentParser(description="회원정보(텍스트 게시판) 연도별 수집")
    ap.add_argument("--year", type=int, default=2026, help="수집 연도 (기본 2026)")
    ap.add_argument("--board", default="회원정보",
                    help="텍스트 게시판명 (회원정보/등업신청/구매/판매)")
    args = ap.parse_args()

    today = datetime.now().date()
    lo = date(args.year, 1, 1)
    hi = min(date(args.year, 12, 31), today)   # 미래 날짜 방지
    if lo > today:
        print(f"[{args.board}] {args.year}년은 아직 시작 전입니다."); return

    print(f"[{args.board}] {args.year}년 범위 텍스트 수집: {lo} ~ {hi}")
    R.run_rpa(
        date_list=[datetime.now()],     # 스크린샷 게시판 없음(window_start용 더미)
        target_boards=[args.board],
        text_range=(lo, hi),
    )
    print("완료.")


if __name__ == "__main__":
    main()
