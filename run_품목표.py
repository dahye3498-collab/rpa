# -*- coding: utf-8 -*-
"""
품목표 최근 N일치 수집 러너 (기본 3일, 오늘 포함)

동작:
  1) [기준일 ~ 오늘] 품목표 게시판을 최신순 단일 패스로 스크린샷 캡처
     (각 글은 실제 작성일 폴더 visionmeat/YYYY-MM-DD/품목표/screenshots 에 저장)
  2) GPT Vision OCR로 구조화 → Excel 저장 (기본: 검토 GUI 표시)

실행 예:
  python run_품목표.py                     # 최근 3일, OCR 후 검토 GUI
  python run_품목표.py --today-only        # 오늘치만
  python run_품목표.py --limit 10          # 캡처 10건까지만 (검증용)
  python run_품목표.py --today-only --limit 10 --auto   # 오늘 10건, GUI 없이 자동저장
  python run_품목표.py --capture-only      # 캡처만 (OCR 건너뜀)
  python run_품목표.py --ocr-only          # 캡처 건너뛰고 기존 스크린샷만 OCR
  python run_품목표.py --days 5            # 최근 5일

주의:
  - rpa_automation 은 매번 로그인하므로, 카카오 봇체크(캡차)가 뜨면
    브라우저에서 처리 후 터미널에서 Enter 로 이어집니다. (로컬 GUI 브라우저)
"""
import argparse
from datetime import datetime, timedelta

import rpa_automation
import batch_processor


def main():
    ap = argparse.ArgumentParser(description="품목표 수집 + OCR")
    ap.add_argument("--days", type=int, default=3, help="오늘 포함 최근 며칠 (기본 3)")
    ap.add_argument("--today-only", action="store_true", help="오늘 하루만 수집")
    ap.add_argument("--limit", type=int, default=None, help="캡처 건수 제한 (검증용)")
    ap.add_argument("--auto", action="store_true", help="OCR 후 검토 GUI 없이 자동 저장")
    ap.add_argument("--capture-only", action="store_true", help="스크린샷 캡처만 수행")
    ap.add_argument("--ocr-only", action="store_true", help="캡처 없이 기존 스크린샷만 OCR")
    args = ap.parse_args()

    today = datetime.now()
    n_days = 1 if args.today_only else args.days
    # 오래된 날짜 → 최신 날짜 순
    dates = [today - timedelta(days=i) for i in range(n_days - 1, -1, -1)]
    date_strs = [d.strftime("%Y-%m-%d") for d in dates]
    print(f"[품목표] 대상 날짜 {len(date_strs)}일: {date_strs}"
          + (f" | 캡처 제한 {args.limit}건" if args.limit else ""))

    # 1) 스크린샷 캡처
    if not args.ocr_only:
        print("\n===== 1단계: 품목표 스크린샷 캡처 =====")
        rpa_automation.run_rpa(
            date_list=dates,
            target_boards=["품목표"],
            max_posts=args.limit,
        )

    # 2) OCR 구조화 + Excel 저장
    if not args.capture_only:
        print("\n===== 2단계: GPT Vision OCR 구조화 =====")
        batch_processor.run_enhanced_batch_all(
            show_gui=not args.auto,
            target_dates=date_strs,
            target_boards=["품목표"],
        )

    print("\n완료.")


if __name__ == "__main__":
    main()
