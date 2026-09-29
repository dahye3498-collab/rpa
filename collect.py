# -*- coding: utf-8 -*-
"""
collect.py — 월/수/금 자동 수집 루틴용 엔트리포인트

동작(무인 실행 가정, GUI 브라우저 필요):
  1) 최근 3일(오늘 포함) 품목표 스크린샷 캡처 (기존 캡처분은 중복 스킵)
  2) OCR 미처리 날짜만 GPT Vision 구조화 → Excel 저장 (자동 감지, 재OCR 최소화)
  3) 신규 업체 연락처 증분 갱신

주의: 카카오 봇체크(캡차)가 뜨면 무인 실행은 로그인 실패로 빈 수집이 될 수 있음.
      그런 날은 수동으로 `python run_품목표.py --days 3 --auto` 실행 권장.
"""
import sys
from datetime import datetime, timedelta

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import rpa_automation
import batch_processor
import contacts

DAYS = 3


def main():
    today = datetime.now()
    dates = [today - timedelta(days=i) for i in range(DAYS - 1, -1, -1)]
    date_strs = [d.strftime("%Y-%m-%d") for d in dates]
    print(f"===== [루틴 수집 시작] {datetime.now():%Y-%m-%d %H:%M} / 대상 {date_strs} =====", flush=True)

    # 1) 캡처
    rpa_automation.run_rpa(date_list=dates, target_boards=["품목표"])

    # 2) OCR (미처리 날짜 자동 감지 → 재OCR 최소화)
    batch_processor.run_enhanced_batch_all(show_gui=False, target_boards=["품목표"])

    # 3) 연락처 증분 갱신
    try:
        contacts.build_contacts()
    except Exception as e:
        print(f"[연락처 갱신 오류] {e}", flush=True)

    print(f"===== [루틴 수집 완료] {datetime.now():%Y-%m-%d %H:%M} =====", flush=True)


if __name__ == "__main__":
    main()
