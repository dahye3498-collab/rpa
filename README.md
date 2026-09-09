# VisionMeat — 품목표 수집·검색 시스템

다음카페 **미트피플**(`cafe.daum.net/meetpeople`)의 업체별 품목표(축산물 가격표)를
자동 수집 → GPT Vision OCR로 구조화 → **웹에서 품목 검색**까지 처리하는 도구.

- **수집(RPA)**: 로컬 PC에서 브라우저로 로그인 후 스크린샷 캡처 (품목표는 이미지 게시물)
- **OCR**: OpenAI **GPT-4.1(비전)**, 긴 이미지 자동 타일링
- **검색**: Flask 웹앱 — 동의어·오독 정규화, 창고 표준화, 업체 연락처, 원본 대조

현재 데이터 규모(예): 4일치 · 11,000+ 품목 · 260+ 업체.

---

## 1. 빠른 시작

```bash
# 의존성 (venv 사용 — 시스템 python엔 openai 등이 없음)
venv/Scripts/python.exe -m pip install -r requirements.txt
venv/Scripts/python.exe -m playwright install chromium

# 품목표 수집 (최근 3일 캡처 + OCR 자동저장)
venv/Scripts/python.exe run_품목표.py --days 3 --auto

# 검색 웹앱 실행 → http://localhost:5000/search
venv/Scripts/python.exe app.py
```

> Windows 콘솔은 cp949라 로그가 깨질 수 있음 → `set PYTHONIOENCODING=utf-8` 권장.

---

## 2. 파이프라인

```
① 수집(RPA)              ② OCR 구조화                  ③ 검색
run_품목표.py       →     batch_processor          →    product_search + Flask
(로컬·카카오 로그인)       (GPT-4.1 비전 + 타일링)         (동의어·연락처·원본대조)
```

### 품목표 캡처 = 2단계 방식 (`rpa_automation.capture_recent_posts`)
1. **목록 순회(캡처 X)**: 최신순 페이지를 넘기며 기준일 이후 글의 (제목·href·날짜) 수집.
2. **본문 캡처**: href로 iframe을 이동시켜 `#user_contents` 스크린샷.
   → '목록 버튼이 1페이지로 튕기는' 문제 없이 안정적. `device_scale_factor=2`로 선명 캡처.

### OCR (`batch_processor`)
- OpenAI GPT-4.1 비전, `detail:high`, 병렬 10워커.
- **자동 타일링**: 세로 1400px 초과 이미지를 1500px 밴드(300 겹침)로 쪼개 OCR 후 병합
  → 긴 이미지에서 생기는 환각·행 누락·병합셀 창고 누락 방지.
- 15개 컬럼 구조화(축종·원산지·보관·품목·브랜드·등급·EST·평중·재고·**창고**·소비기한·판매가·수정일·비고).
- 유사어/표준 정규화: `[운영] 자동변환_소스데이터.xlsx` + `ref/창고_목록.md` 참조.

---

## 3. 검색 웹앱 (`app.py` + `templates/search.html`)

접속: `http://localhost:5000/search`

- **동의어·오독 확장**: "끝갈비" → 립앤드·립엔드·RIB END(+오독 갈갈비·팁앤드). 402개 그룹
  (하드코딩 8 + 소스데이터 품목·브랜드·축종 394).
- **창고 정규화**: 강한1→강동1, 세미→세미(이스트밸리), 아주기흥→에이스기흥 등 (`ref/창고_목록.md`).
- **업체 연락처**: 결과에 담당자·전화·팩스 표시 (`contacts.py` 인덱스).
- **원본 대조**: 결과마다 `🔍원본` → 스크린샷 열림 (`/screenshot/<date>/<file>`).
- **조회 기간**: 기본 **최근 3일**(최근 수집일 3개). 드롭다운으로 7일/전체 전환.
- **판매가 비공개**: 검색 화면·API 응답 모두에서 제외.

주요 API: `/api/search?q=&warehouse=&origin=&brand=&field=&recent=` · `/api/search_stats` · `/api/build_contacts`

---

## 4. 자동 수집 루틴 (월/수/금)

Windows 작업 스케줄러에 등록됨: **`VisionMeat_품목표수집`** — 월·수·금 **11:00** 실행.

- 실행 파일: `routine_collect.cmd` → `collect.py`
  (최근 3일 캡처 → 미처리 날짜만 OCR → 연락처 증분 갱신). 로그: `routine_run.log`
- 로컬 로그인 세션에서 GUI 브라우저로 동작 → **PC가 켜져 있고 로그인된 상태**여야 함.
- 카카오 봇체크(캡차)가 뜨는 날은 무인 실행이 실패할 수 있음 → 수동 실행 권장:
  `venv/Scripts/python.exe run_품목표.py --days 3 --auto`

```powershell
# 상태/다음 실행 확인
Get-ScheduledTaskInfo -TaskName "VisionMeat_품목표수집"
# 지금 즉시 1회 실행
Start-ScheduledTask -TaskName "VisionMeat_품목표수집"
# 해제
Unregister-ScheduledTask -TaskName "VisionMeat_품목표수집" -Confirm:$false
```

---

## 5. 외부 접속 (터널)

로컬 앱을 외부에서 쓰려면 cloudflare 터널로 공개 URL 발급(무료, PC 켜진 동안 유효):

```bash
"C:\Program Files (x86)\cloudflared\cloudflared.exe" tunnel --url http://localhost:5000
```
출력의 `https://xxxx.trycloudflare.com/search` 로 접속. (재시작 시 URL 변경됨)

---

## 6. 환경변수 (`.env`)

| 키 | 설명 |
|---|---|
| `OPENAI_API_KEY` | OCR용 |
| `OPENAI_VISION_MODEL` | 비전 모델 (미설정 시 `gpt-4.1`) |
| `LOGIN_EMAIL` / `LOGIN_PWD` | 카카오 로그인 |
| `RESIZE_IMAGES` | **반드시 0** (1이면 긴 이미지를 1280px로 줄여 OCR 품질 급락) |
| `OCR_WORKERS` | OCR 병렬 수 (기본 10) |
| `OCR_TILE_THRESHOLD/HEIGHT/OVERLAP` | 타일링 튜닝 (기본 1400/1500/300) |
| `NOTION_TOKEN` / `NOTION_DB_ID` | 뉴스레터 노션 푸시용(별개 기능) |

`.env` · `venv/` · `visionmeat/`(데이터·스크린샷·연락처) · `*.log` 는 `.gitignore`.

---

## 7. 구성 파일

| 파일 | 역할 |
|---|---|
| `run_품목표.py` | 수집 러너 (`--days/--today-only/--limit/--auto/--capture-only/--ocr-only`) |
| `collect.py` + `routine_collect.cmd` | 월/수/금 루틴 엔트리포인트 |
| `rpa_automation.py` | 품목표 캡처 엔진 (2단계, 2배율) |
| `batch_processor.py` | GPT-4.1 OCR + 타일링 + 유사어 정규화 |
| `product_search.py` | 검색 엔진 (동의어·창고정규화·연락처·기간필터) |
| `contacts.py` | 업체 연락처 인덱스 (스크린샷 상단 OCR) |
| `warehouses.py` + `ref/창고_목록.md` | 창고 표준 76 + 동의어 |
| `app.py` + `templates/search.html` | 검색 웹앱 |
| `rpa_members.py` | 회원정보/등업신청 크롤러 (텍스트, 조회수 미증가 fetch) |
| `news_push.py` / `update_routine.py` | 뉴스레터 노션 푸시(별개 기능) |

---

## 8. 산출물 경로

```
visionmeat/
├─ YYYY-MM-DD/품목표/
│  ├─ screenshots/            # 게시글별 캡처 PNG
│  └─ excel/YYYYMMDD_품목표_데이터.xlsx
└─ database/
   ├─ YYYYMMDD_품목표_데이터.xlsx   # 날짜별 취합본 (검색 소스)
   └─ vendor_contacts.json         # 업체 연락처 인덱스
```

---

## 9. 알려진 한계 / 주의

- 품목표는 이미지 OCR이라 100% 정확하지 않음 → **희귀·고가 품목은 `🔍원본`으로 대조** 권장.
- 초기 수집분(813px 1배율)은 글자 오독이 남음 → 2배율 재수집분부터 개선.
- 조회수: 캡처하는 글당 +1 증가(내용을 보려면 최소 1회 열람 불가피, 무조회 방식은 이 게시판에선 불가로 확인됨).
- 창고 표준/동의어는 `ref/창고_목록.md`만 수정하면 반영. **업체명 표준 목록은 추후 반영 예정.**
