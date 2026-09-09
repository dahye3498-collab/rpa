# RPA 프로젝트 현황 정리

> 최종 업데이트: 2026-09-09

---

## 프로젝트 개요

두 개의 독립적인 자동화 파이프라인이 하나의 레포지터리에서 동작하고 있습니다.

| 파이프라인 | 역할 | 실행 방식 |
|---|---|---|
| **VisionMeat RPA** | 다음 카페 축산물 게시판 스크래핑 → AI 분석 → Excel 저장 | Flask 웹 앱 (수동 트리거) |
| **FOODIVERSE 뉴스레터** | 글로벌 축산물 시장 뉴스 → Notion DB 자동 등록 | Claude Code 예약 스케줄 (매일 08:00 KST) |

---

## 파이프라인 1: VisionMeat RPA

### 목적
다음 카페 `meetpeople` (축산업 커뮤니티)의 **구매/판매/품목표** 게시판을 자동 순회하여,
업체별 축산물 재고 현황을 스크린샷 캡처 → GPT Vision으로 분석 → Excel로 저장합니다.

### 아키텍처

```
Flask 웹 앱 (app.py)
    │
    ├── /api/run  ──▶  JobManager (job_manager.py)
    │                       │
    │                       ├── 1단계: RPA 캡처 (rpa_automation.py)
    │                       │         - Playwright로 Daum 카페 접속/로그인
    │                       │         - 게시판 순회 (구매/판매/품목표)
    │                       │         - 날짜 필터링 (3일 이내)
    │                       │         - 게시글 콘텐츠 영역 스크린샷 저장
    │                       │
    │                       └── 2단계: AI 분석 (batch_processor.py)
    │                                 - GPT-4.1 Vision으로 이미지 분석
    │                                 - 15개 필드 구조화 추출
    │                                 - Excel 파일로 저장
    │
    └── /api/stream (SSE) ──▶ 브라우저에 실시간 진행상황 push
```

### 핵심 파일

| 파일 | 역할 |
|---|---|
| `app.py` | Flask 웹 서버. API 엔드포인트 + SSE 스트림 |
| `job_manager.py` | 스레드 안전 싱글톤. 실행/일시정지/중단 제어 |
| `rpa_automation.py` | Playwright 기반 캡처 자동화 |
| `batch_processor.py` | GPT Vision 분석 + Excel 저장 |
| `rpa_members.py` | 회원 정보 관련 처리 |

### RPA 실행 흐름 (상세)

1. **다음 카페 접속** → `https://cafe.daum.net/meetpeople`
2. **카카오 로그인** → 이메일/비밀번호 자동 입력 (봇 감지 시 수동 개입 요청)
3. **게시판 전환** → 구매 / 판매 / 품목표 순서로 `iframe#down` 내 목록 접근
4. **날짜 필터링** → 오늘 기준 3일 이내 글만 대상 (과거 글 5개 연속 발견 시 중단)
5. **스크린샷 캡처** → `#user_contents` 영역만 선택적 캡처
6. **중복 방지** → 이미 캡처된 파일명 체크 후 스킵
7. **저장 경로**: `visionmeat/YYYY-MM-DD/[게시판명]/screenshots/`

### Excel 추출 필드 (15개)
```
축종 | 원산지 | 보관 | 품목 | 브랜드 | 등급 | EST
평중(kg) | 스펙/설명 | 재고(box) | 창고 | 소비기한
판매가(원) | 수정일 | 비고
```
- Sheet 1: 상품 목록
- Sheet 2: 거래처 정보 (회사명, 주소, 연락처)

### Flask API 엔드포인트

| Method | 경로 | 설명 |
|---|---|---|
| GET | `/` | 웹 제어판 UI |
| GET | `/api/stream` | SSE 실시간 상태 스트림 |
| POST | `/api/run` | 작업 시작 (mode, date_from, date_to, boards, credentials) |
| POST | `/api/pause` | 일시정지 |
| POST | `/api/resume` | 재개 |
| POST | `/api/stop` | 중단 |
| GET | `/api/status` | 현재 상태 조회 |
| GET | `/api/logs` | 로그 조회 |
| GET | `/api/resume_state` | 날짜별 수집 완료 현황 |

### 실행 모드

| mode | 동작 |
|---|---|
| `full` | RPA 캡처 + AI 분석 전체 실행 |
| `rpa_only` | 캡처만 수행 |
| `batch_only` | 이미 캡처된 이미지 분석만 수행 |

### 환경 변수 (.env)

```
OPENAI_API_KEY=...          # GPT Vision 분석용
OPENAI_VISION_MODEL=gpt-4.1 # 기본값
LOGIN_EMAIL=...             # 카카오 로그인 이메일
LOGIN_PWD=...               # 카카오 로그인 비밀번호
```

### 인프라

- **배포**: Railway (`railway.toml` 존재)
- **컨테이너**: Docker (`Dockerfile` 존재)
- **서버 포트**: `PORT` 환경변수 (기본 5000)
- **headless 모드**: Railway 환경에서는 자동으로 headless 브라우저 사용

---

## 파이프라인 2: FOODIVERSE 뉴스레터 자동화

### 목적
글로벌 축산물 시장 동향을 매일 오전 8시(KST) 수집하여 Notion DB에 자동 등록.
금요일에는 주간 5일치 기사를 모아 7장 카드뉴스 HTML 생성.

### 실행 스케줄

| 요일 | 주제 | 카테고리 |
|---|---|---|
| 월요일 | 글로벌 소고기 시장 | Weekly Report |
| 화요일 | 글로벌 돼지고기 시장 | Weekly Report |
| 수요일 | 가금류 시장 | Weekly Report |
| 목요일 | 수출입 & 무역 이슈 | Alert |
| 금요일 | 주간 축산 시장 종합 + 카드뉴스 생성 | Weekly Report |

### 데이터 흐름

```
WebSearch (USDA / MLA / Rabobank / Reuters 등)
    │
    ▼
뉴스레터 콘텐츠 작성 (한국어 + 영어)
    │
    ├── Notion DB 등록 (뉴스레터)
    │       DB ID: 3484fcedeeb68064be8bd499a4f9f459
    │       필드: 이름(KR) | 날짜 | 선택(카테고리) | Summary(KR)
    │             Title_EN | Summary_EN | 게시(체크박스)
    │
    └── (금요일만) 카드뉴스 HTML 생성
            - Notion API로 이번 주 5개 기사 조회
            - 7장 카드 구성 (인트로 1 + 기사 5 + 아웃트로 1)
            - 파일 저장: /tmp/card_news_YYYY-MM-DD.html
```

### 핵심 파일

| 파일 | 역할 |
|---|---|
| `notion_push_template.py` | Notion 등록 + SMS DB 연동 템플릿 |

### Notion 연동 정보

| 항목 | 값 |
|---|---|
| 뉴스레터 DB | `3484fcedeeb68064be8bd499a4f9f459` |
| SMS DB | `3d14fcedeeb681e99fe0e4a77627423b` |
| API Version | `2022-06-28` |

### SMS 연동 (notion_push_template.py 기준)

뉴스레터 등록과 동시에 SMS DB에도 발송 대기 레코드 생성:
- 카테고리 매핑: `Weekly Report → 주간종합`, `Alert → 무역이슈`
- 이모지: 소고기🐄 / 돼지고기🐖 / 가금류🐔 / 무역이슈🚢 / 주간종합📊
- 글자 수 제한: 90자 이하 (초과 시 말줄임)
- 발송상태 초기값: `대기중`

### 오늘(2026-09-09 화요일) 실행 결과

- **주제**: 글로벌 돼지고기 시장
- **헤드라인**: EU 돼지고기 수출 8% 감소·중국 수입 30% 급락…2026 글로벌 돼지고기 시장 재편 가속
- **카테고리**: Weekly Report
- **Notion 등록 ID**: `3d54fced-eeb6-81e0-a7ef-c18a5290efe5`
- **출처**: USDA FAS, Rabobank, S&P Global

---

## Cowork 호환 여부

> **질문: 지금 진행되는 작업을 Claude Cowork에서도 할 수 있나요?**

**뉴스레터 자동화 → 가능** (단, 제약 있음)

| 항목 | 내용 |
|---|---|
| WebSearch | Cowork 세션에서도 사용 가능 |
| Notion API 호출 | Python `urllib` 스크립트로 가능 |
| 예약 스케줄 (`CronCreate`) | 현재 세션에서 설정된 스케줄은 이 세션에 귀속 — Cowork 세션에서 새로 설정 필요 |
| 토큰/시크릿 공유 | `.env`나 환경변수를 Cowork 세션에도 동일하게 설정해야 함 |

**VisionMeat RPA → 제한적**

| 항목 | 내용 |
|---|---|
| Playwright | Cowork(원격 클라우드) 환경에 Chromium이 사전 설치되어 있어 실행 가능 |
| 카카오 로그인 | 봇 감지 시 수동 개입 필요 → 비대화형 환경에서는 막힐 수 있음 |
| 파일시스템 저장 | Cowork는 에페머럴 컨테이너 — Excel/스크린샷 파일은 세션 종료 시 사라짐 |
| 권장 | Railway 배포 후 웹 제어판(`/api/run`)으로 트리거하는 방식이 더 안정적 |

---

## 의존성 (requirements.txt)

```
playwright      # 브라우저 자동화
openai          # GPT Vision 분석
pandas          # 데이터 처리
openpyxl        # Excel 저장
python-dotenv   # 환경변수 관리
flask           # 웹 서버
flask-cors      # CORS
pillow          # 이미지 처리
```

---

## 디렉토리 구조

```
rpa/
├── app.py                    # Flask 웹 서버 (제어판)
├── job_manager.py            # 스레드 안전 작업 관리자
├── rpa_automation.py         # Playwright 캡처 자동화
├── batch_processor.py        # GPT Vision 분석 + Excel 저장
├── rpa_members.py            # 회원 정보 처리
├── notion_push_template.py   # Notion + SMS DB 등록 템플릿
├── requirements.txt
├── Dockerfile
├── railway.toml              # Railway 배포 설정
├── ref/
│   ├── workflow.md           # RPA 동작 명세 (HTML 구조 참조)
│   └── 국제식품_상품목록.xlsx  # 참조 엑셀
├── templates/
│   └── index.html            # 웹 제어판 UI
└── visionmeat/               # 캡처 결과물 저장 (런타임 생성)
    └── YYYY-MM-DD/
        ├── 구매/screenshots/
        ├── 판매/screenshots/
        └── 품목표/screenshots/
```
