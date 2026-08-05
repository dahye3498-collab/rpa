# VisionMeat RPA / OCR

다음카페 **미트피플**(`cafe.daum.net/meetpeople`) 게시판을 자동 크롤링하여 축산물 데이터를 Excel로 구조화하는 자동화 도구입니다.

- **품목표(업체별 품목표)**: 게시글이 이미지(가격표)라 → 스크린샷 캡처 후 **GPT Vision OCR**로 구조화
- **텍스트 게시판(구매/판매/회원정보/등업신청)**: 본문 텍스트를 직접 추출 → Excel 저장
- 웹 제어판(Flask + SSE)에서 실시간 진행률 확인 / 일시정지 / 재개 지원

---

## 1. 빠른 시작

```bash
# 의존성 (venv 사용 권장 - 시스템 python엔 openai 등이 없음)
venv/Scripts/python.exe -m pip install -r requirements.txt
venv/Scripts/python.exe -m playwright install chromium

# 품목표 최근 3일 수집 + OCR (검토 GUI)
venv/Scripts/python.exe run_품목표.py

# 품목표 오늘치만, GUI 없이 자동 저장
venv/Scripts/python.exe run_품목표.py --today-only --auto
```

> Windows 콘솔은 cp949라 로그 이모지가 깨질 수 있습니다. UTF-8로 보려면 `PYTHONIOENCODING=utf-8`를 앞에 붙이세요.

---

## 2. 환경변수 (`.env`)

| 키 | 설명 |
|---|---|
| `OPENAI_API_KEY` | GPT Vision OCR용 |
| `OPENAI_VISION_MODEL` | 비전 모델 (기본 `gpt-4.1`) |
| `LOGIN_EMAIL` / `LOGIN_PWD` | 다음/카카오 로그인 (품목표 자동 로그인) |
| `NOTION_TOKEN` / `NOTION_DB_ID` | 뉴스레터 노션 푸시용 |
| `OCR_WORKERS` | OCR 병렬 워커 수 |
| `RESIZE_IMAGES` / `MAX_IMG_SIZE` | 캡처 이미지 리사이즈 옵션 |

`.env`, `venv/`, `visionmeat/`, `browser_session/` 는 `.gitignore` 처리되어 있습니다.

---

## 3. 구성 요소

| 파일 | 역할 |
|---|---|
| **`run_품목표.py`** | 품목표 수집 러너 (캡처 + OCR 원스톱). 옵션: `--today-only` `--days N` `--limit N` `--auto` `--capture-only` `--ocr-only` |
| **`rpa_automation.py`** | 품목표(스크린샷) + 텍스트 게시판 크롤링 엔진 |
| **`rpa_members.py`** | 회원정보/등업신청 전용 크롤러 (조회수 미증가 API fetch, 세션 영속화, 버전 자동증가) |
| **`batch_processor.py`** | 캡처 이미지 → GPT Vision OCR → 15개 컬럼 구조화 + 유사어 매핑 + 검토 GUI |
| **`app.py` / `job_manager.py` / `templates/index.html`** | Flask 웹 제어판 (SSE 실시간 진행률, 일시정지/중단/스마트 재개) |
| **`news_push.py` / `update_routine.py`** | 주간 축산물 뉴스레터를 노션 DB에 푸시 + 로컬 MD 취합 (RPA와 별개 기능) |
| `ref/` | 상품목록·유사어 매핑·워크플로우 문서 |
| `Dockerfile` / `railway.toml` | 배포 설정 |

---

## 4. 품목표 수집 동작 방식 (2단계)

게시판이 매우 활발해 하루 약 140~150건이 올라옵니다(업체들이 매일 아침 가격표 게시). 이를 안정적으로 처리하기 위해 `capture_recent_posts()`가 2단계로 동작합니다.

1. **목록 순회 (캡처 없음)** — 최신순으로 페이지를 넘기며 기준일 이후 글의 (제목·href·날짜)를 수집. 기준일보다 오래된 글이 5개 연속 나오면 종료.
2. **본문 캡처** — 수집한 href로 `iframe#down`을 직접 이동시켜 `#user_contents`를 스크린샷. 목록 왕복이 없어 페이지네이션이 어긋나지 않음.

이후 `batch_processor`가 스크린샷을 GPT Vision으로 읽어 아래 컬럼으로 구조화합니다.

> 축종 · 원산지 · 보관 · 품목 · 브랜드 · 등급 · EST · 평중(kg) · 스펙/설명 · 재고(box) · 창고 · 소비기한 · 판매가(원) · 수정일 · 비고 (+ 파일명 · 수집일)

병합셀 값(원산지·브랜드 등)은 해당하는 모든 행에 반복 채워집니다.

---

## 5. 산출물 경로

```
visionmeat/
├─ YYYY-MM-DD/
│  └─ 품목표/
│     ├─ screenshots/           # 게시글별 캡처 PNG
│     └─ excel/YYYYMMDD_품목표_데이터.xlsx
└─ database/                    # 전체 취합본 (게시판별 Excel)
```

텍스트 게시판은 `text_data/text_data.json` + `excel/`에 저장됩니다.

---

## 6. 게시판 목록

| 게시판 | fldid | 방식 |
|---|---|---|
| 품목표(업체별 품목표) | `LdED` | 스크린샷 + OCR |
| 구매 | `HoTs` | 텍스트 |
| 판매 | `HoUW` | 텍스트 |
| 회원정보 | `DrGV` | 텍스트 (`rpa_members.py`) |
| 등업신청 | `HoSn` | 텍스트 (`rpa_members.py`) |

---

## 7. 기타 실행

```bash
# 웹 제어판 (http://localhost:5000)
venv/Scripts/python.exe app.py

# 회원정보/등업신청 크롤러
venv/Scripts/python.exe rpa_members.py

# OCR 재처리 (대화형 메뉴: 신규검토 / 자동저장 / 전체재처리)
venv/Scripts/python.exe batch_processor.py
```

---

## 8. 참고 / 주의

- 품목표는 이미지 OCR이라 값이 100% 정확하지 않습니다. **대량 수집 전 `--limit`로 소량 검증**을 권장합니다.
- 품목표는 로그인 시 세션을 저장하지 않아 실행마다 로그인합니다. 카카오 봇체크(캡차)가 뜨면 브라우저에서 처리 후 터미널에서 Enter를 누르세요. (회원 크롤러는 `browser_session/`에 세션 영속화)
- 판매가·보관 등은 원본 게시글에 값이 없는 경우가 많아 빈값 비율이 높을 수 있습니다(OCR 오류 아님).
- 뉴스레터 노션 등록 전에는 실제 출처 기반 팩트체크가 필요합니다(AI 생성 수치 그대로 게시 금지).
