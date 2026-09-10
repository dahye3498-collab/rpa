# FOODIVERSE 글로벌 축산물 뉴스레터 자동화 루틴

> 이 문서는 Claude Code (Cowork 포함) 에서 루틴을 **그대로 재현**할 수 있도록 작성되었습니다.

---

## 루틴 개요

매일 오전 08:00 KST, 오늘 요일에 맞는 글로벌 축산물 시장 뉴스레터를 작성해 Notion DB에 자동 등록합니다.
금요일에는 추가로 이번 주 5일치 기사를 모아 **7장 카드뉴스 HTML 파일**을 생성합니다.

---

## 필요한 정보 (Cowork 세션에 전달할 것)

| 항목 | 값 |
|---|---|
| Notion Token | `YOUR_NOTION_TOKEN` (`.env` 또는 환경변수 `NOTION_TOKEN` 참조) |
| 뉴스레터 DB ID | `3484fcedeeb68064be8bd499a4f9f459` |
| SMS DB ID | `3d14fcedeeb681e99fe0e4a77627423b` |
| Notion API Version | `2022-06-28` |

---

## 전체 실행 순서 (STEP별)

---

### STEP 1 — 오늘 요일 확인

```bash
python3 -c "import datetime; print(datetime.date.today().weekday(), datetime.date.today().strftime('%A'))"
```

출력 예시: `1 Tuesday` → 화요일(1) 확인

---

### STEP 2 — 요일별 주제 & 검색어 선택

| 요일 | weekday | 주제 | 카테고리 |
|---|---|---|---|
| 월 | 0 | 글로벌 소고기 시장 | Weekly Report |
| 화 | 1 | 글로벌 돼지고기 시장 | Weekly Report |
| 수 | 2 | 가금류 시장 | Weekly Report |
| 목 | 3 | 수출입 & 무역 이슈 | Alert |
| 금 | 4 | 주간 축산 시장 종합 | Weekly Report |

**요일별 WebSearch 검색어:**

```
월(0):
  - "global beef cattle market price 2026"
  - "USDA beef cattle outlook"
  - "beef export price Australia USA"

화(1):
  - "global pork market price 2026"
  - "pork trade news China Europe 2026"
  - "USDA hog outlook 2026"

수(2):
  - "global poultry chicken market 2026"
  - "broiler chicken price trade news"
  - "poultry production outlook USDA"

목(3):
  - "livestock meat trade news 2026"
  - "beef pork export import tariff"
  - "meat trade alert WTO sanction"

금(4):
  - "global livestock market weekly summary 2026"
  - "beef pork poultry market week"
  - "USDA MLA weekly report"
```

---

### STEP 3 — WebSearch로 실제 데이터 수집

- 위 검색어 중 2~3개를 WebSearch 툴로 실행
- 반드시 **실제 수치**(가격/수출량/생산량 등) 포함된 결과 사용
- 신뢰 출처 우선 (1순위 = 기관 원자료, 2순위 = 산업지/통신사):

| 구분 | 출처 | 강점 | 주로 쓰는 요일 |
|---|---|---|---|
| 기관 | **USDA** (ERS / WASDE / AMS) | 미국 생산·가격·재고 원자료 | 월·화·수·금 |
| 기관 | **MLA** | 호주 소·양 가격 지표(EYCI 등) | 월·금 |
| 기관 | **FAO** | 육류가격지수, 세계 수급 전망 | 금 |
| 기관 | **USMEF** | 미국 소·돼지고기 수출 물량/금액 월별 통계 | 목·금 |
| 산업지 | **Beef Central** | 호주 경매장 실거래가, 산지 동향 | 월·목 |
| 리서치 | **Rabobank**, **S&P Global** | 분기 전망, 글로벌 증감률 | 전 요일 |
| 통신사 | **Reuters**, **Bloomberg** | 속보성 무역·관세 이슈 | 목 |

- USMEF 검색 시 `usmef.org` 도메인 한정이 정확도가 높음 (예: "USMEF beef pork export statistics 2026")
- Beef Central은 경매장별 일별 리포트가 올라오므로 호주 시세 인용에 적합 (`beefcentral.com` 한정 검색)
- Reuters / Bloomberg는 유료벽으로 본문 조회가 막히는 경우가 있음 — 검색 스니펫에 수치가 없으면 기관 원자료로 대체
- AI 생성 수치 사용 금지 — 검색으로 확인된 데이터만 사용

---

### STEP 4 — 뉴스레터 콘텐츠 작성

아래 4개 변수를 채웁니다:

| 변수 | 설명 | 조건 |
|---|---|---|
| `kr_title` | 한국어 헤드라인 | 핵심 수치 포함, 임팩트 있게 |
| `kr_summary` | 한국어 요약 | 2~3문장, 실제 수치 + 출처 명시, 1000자 이하 |
| `en_title` | 영문 헤드라인 | kr_title의 영어 버전 |
| `en_summary` | 영문 요약 | kr_summary의 영어 버전 |

---

### STEP 5 — Notion 등록 스크립트 작성 & 실행

아래 코드를 `/tmp/notion_push.py`로 저장한 뒤 `python3 /tmp/notion_push.py`로 실행합니다.
**`FILL_*` 부분을 STEP 4에서 작성한 실제 내용으로 교체하세요.**

```python
import urllib.request, json, datetime, sys
sys.stdout.reconfigure(encoding='utf-8')
token = 'YOUR_NOTION_TOKEN'
db = '3484fcedeeb68064be8bd499a4f9f459'
today = datetime.date.today().isoformat()

kr_title   = 'FILL_KR_HEADLINE'
kr_summary = 'FILL_KR_SUMMARY'
en_title   = 'FILL_EN_HEADLINE'
en_summary = 'FILL_EN_SUMMARY'
category   = 'FILL_CATEGORY'   # 'Weekly Report' 또는 'Alert'

payload = {
    'parent': {'database_id': db},
    'properties': {
        '이름':       {'title':     [{'text': {'content': kr_title}}]},
        '날짜':       {'date':      {'start': today}},
        '선택':       {'select':    {'name': category}},
        'Summary':    {'rich_text': [{'text': {'content': kr_summary}}]},
        'Title_EN':   {'rich_text': [{'text': {'content': en_title}}]},
        'Summary_EN': {'rich_text': [{'text': {'content': en_summary}}]},
        '게시':       {'checkbox':  True},
    }
}
body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
req = urllib.request.Request('https://api.notion.com/v1/pages', data=body, method='POST')
req.add_header('Authorization', 'Bearer ' + token)
req.add_header('Notion-Version', '2022-06-28')
req.add_header('Content-Type', 'application/json; charset=utf-8')
with urllib.request.urlopen(req) as resp:
    result = json.loads(resp.read().decode('utf-8'))
    print('SUCCESS:', result['id'])
```

성공 시 출력 예시:
```
SUCCESS: 3d54fced-eeb6-81e0-a7ef-c18a5290efe5
```

> **SMS DB도 함께 등록하려면** `notion_push_template.py` 파일을 참고해 SMS 페이로드를 추가하세요.

---

### STEP 6 — 카드뉴스 HTML 생성 (금요일 = weekday 4 인 경우만)

오늘이 **금요일(weekday=4)** 인 경우에만 실행합니다.

아래 코드를 `/tmp/make_cardnews.py`로 저장한 뒤 실행합니다.

```python
import urllib.request, json, datetime, sys
sys.stdout.reconfigure(encoding='utf-8')

token = 'YOUR_NOTION_TOKEN'
db    = '3484fcedeeb68064be8bd499a4f9f459'
today = datetime.date.today()

# 이번 주 월요일 계산
monday = today - datetime.timedelta(days=today.weekday())

# ── 1) Notion에서 이번 주 기사 5개 조회 ────────────────────────
query = {
    'filter': {
        'and': [
            {'property': '게시', 'checkbox': {'equals': True}},
            {'property': '날짜', 'date': {'on_or_after': monday.isoformat()}},
            {'property': '날짜', 'date': {'on_or_before': today.isoformat()}},
        ]
    },
    'sorts': [{'property': '날짜', 'direction': 'ascending'}],
    'page_size': 5
}
body = json.dumps(query, ensure_ascii=False).encode('utf-8')
url  = f'https://api.notion.com/v1/databases/{db}/query'
req  = urllib.request.Request(url, data=body, method='POST')
req.add_header('Authorization', 'Bearer ' + token)
req.add_header('Notion-Version', '2022-06-28')
req.add_header('Content-Type', 'application/json; charset=utf-8')

with urllib.request.urlopen(req) as resp:
    data = json.loads(resp.read().decode('utf-8'))

articles = []
for page in data.get('results', []):
    props = page['properties']
    title   = props['이름']['title'][0]['text']['content'] if props['이름']['title'] else ''
    summary = props['Summary']['rich_text'][0]['text']['content'] if props['Summary']['rich_text'] else ''
    date    = props['날짜']['date']['start'] if props['날짜']['date'] else ''
    articles.append({'title': title, 'summary': summary[:180], 'date': date})

# ── 2) 카드별 이미지 & 배지 색상 ───────────────────────────────
CARD_META = [
    {'img': 'https://picsum.photos/seed/beef-cattle/720/440',    'badge': '소고기',   'color': '#dc2626'},
    {'img': 'https://picsum.photos/seed/pork-farm/720/440',      'badge': '돼지고기', 'color': '#d97706'},
    {'img': 'https://picsum.photos/seed/chicken-poultry/720/440','badge': '가금류',   'color': '#16a34a'},
    {'img': 'https://picsum.photos/seed/cargo-trade/720/440',    'badge': '무역이슈', 'color': '#2563eb'},
    {'img': 'https://picsum.photos/seed/weekly-report/720/440',  'badge': '주간종합', 'color': '#7c3aed'},
]

# ── 3) HTML 생성 ────────────────────────────────────────────────
card_style = """
.card{width:360px;border-radius:16px;overflow:hidden;
      box-shadow:0 2px 16px rgba(0,0,0,.1);background:#fff;margin:0 auto}
.card-img{position:relative;height:220px;overflow:hidden}
.card-img img{width:100%;height:100%;object-fit:cover}
.card-img::after{content:'';position:absolute;inset:0;
  background:linear-gradient(to bottom,transparent 40%,rgba(0,0,0,.45))}
.badge{position:absolute;top:12px;left:12px;z-index:1;
       color:#fff;font-size:11px;font-weight:700;padding:4px 10px;
       border-radius:20px;letter-spacing:.5px}
.card-body{padding:20px}
.card-title{font-size:15px;font-weight:700;margin:0 0 8px;line-height:1.4;color:#111}
.card-summary{font-size:13px;color:#444;line-height:1.6;margin:0 0 12px}
.card-date{font-size:11px;color:#999}
"""

intro_card = """
<div class="card" style="background:#111;color:#fff;min-height:300px;
     display:flex;flex-direction:column;align-items:center;justify-content:center;padding:32px 24px;box-sizing:border-box">
  <div style="font-size:11px;letter-spacing:3px;color:#aaa;margin-bottom:12px">FOODIVERSE</div>
  <div style="font-size:22px;font-weight:800;text-align:center;line-height:1.4;margin-bottom:24px">
    글로벌 축산 시장<br>주간 리포트
  </div>
  <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:center">
    <span style="background:#dc2626;color:#fff;padding:5px 12px;border-radius:20px;font-size:11px;font-weight:700">소고기</span>
    <span style="background:#d97706;color:#fff;padding:5px 12px;border-radius:20px;font-size:11px;font-weight:700">돼지고기</span>
    <span style="background:#16a34a;color:#fff;padding:5px 12px;border-radius:20px;font-size:11px;font-weight:700">가금류</span>
    <span style="background:#2563eb;color:#fff;padding:5px 12px;border-radius:20px;font-size:11px;font-weight:700">무역이슈</span>
  </div>
</div>
"""

outro_card = """
<div class="card" style="background:#111;color:#fff;min-height:200px;
     display:flex;flex-direction:column;align-items:center;justify-content:center;padding:32px 24px;box-sizing:border-box">
  <div style="font-size:13px;color:#aaa;margin-bottom:8px">매일 오전 8시 업데이트</div>
  <div style="font-size:24px;font-weight:800;letter-spacing:1px">FOODIVERSE</div>
</div>
"""

article_cards = ''
for i, art in enumerate(articles):
    meta = CARD_META[i] if i < len(CARD_META) else CARD_META[-1]
    article_cards += f"""
<div class="card">
  <div class="card-img">
    <span class="badge" style="background:{meta['color']}">{meta['badge']}</span>
    <img src="{meta['img']}" alt="{meta['badge']}">
  </div>
  <div class="card-body">
    <div class="card-title">{art['title']}</div>
    <div class="card-summary">{art['summary']}</div>
    <div class="card-date">{art['date']}</div>
  </div>
</div>
"""

html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FOODIVERSE 주간 카드뉴스 {today.isoformat()}</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: #f0f0f0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       padding: 24px 0; }}
.wrap {{ max-width: 420px; margin: 0 auto; display: flex; flex-direction: column; gap: 16px; padding: 0 16px; }}
{card_style}
</style>
</head>
<body>
<div class="wrap">
{intro_card}
{article_cards}
{outro_card}
</div>
</body>
</html>"""

out = f'/tmp/card_news_{today.isoformat()}.html'
with open(out, 'w', encoding='utf-8') as f:
    f.write(html)
print(f'카드뉴스 저장 완료: {out}')
```

---

## 완료 후 출력할 내용

```
✅ 오늘 요일: [요일명] (weekday=[숫자])
✅ 주제: [오늘 주제]
✅ 헤드라인: [kr_title]
✅ 카테고리: [category]
✅ 날짜: [YYYY-MM-DD]
✅ Notion 등록 ID: [id]
(금요일만) ✅ 카드뉴스: /tmp/card_news_YYYY-MM-DD.html
```

---

## Cowork에서 실행하는 방법

### 방법 A: 이 MD 파일을 그대로 붙여넣기

Cowork 세션을 열고 아래 프롬프트를 입력하세요:

```
이 레포(dahye3498-collab/rpa)의 NEWSLETTER_ROUTINE.md 파일을 읽고,
그 안의 루틴을 오늘 날짜 기준으로 실행해줘.
STEP 1~5를 순서대로 진행하고, 금요일이면 STEP 6도 실행해줘.
```

### 방법 B: 예약 스케줄 등록 (CronCreate)

Cowork 세션에서 아래 프롬프트로 매일 자동 실행 등록:

```
매일 오전 8시(KST = UTC+9, 즉 UTC 23:00)에 다음 작업을 자동 실행해줘:
NEWSLETTER_ROUTINE.md의 STEP 1~6을 순서대로 실행 (금요일이면 카드뉴스도 생성)
```

> **주의**: 스케줄은 세션에 귀속됩니다. 세션이 종료되면 스케줄도 사라지므로
> 새 Cowork 세션을 열 때마다 다시 등록해야 합니다.

---

## 규칙 & 주의사항

- AI 생성 수치 사용 금지 — WebSearch로 확인된 실제 데이터만 사용
- 데이터 수집이 안 되는 경우에도 Notion 등록은 진행 (Summary에 사유 명시)
- kr_summary는 1000자 이하
- SMS 내용은 90자 이하 (초과 시 말줄임 처리)
- 출처는 반드시 Summary에 명시 (USDA / MLA / FAO / USMEF / Beef Central / Rabobank / Reuters 등)

---

## 실행 이력

| 날짜 | 요일 | 주제 | Notion ID |
|---|---|---|---|
| 2026-09-09 | 화요일 | 글로벌 돼지고기 시장 | `3d54fced-eeb6-81e0-a7ef-c18a5290efe5` |
