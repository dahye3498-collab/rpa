# -*- coding: utf-8 -*-
"""
매일 뉴스 1건을 노션 DB에 푸시하고 로컬 MD 파일에 append.

사용법:
  python news_push.py
  → 대화형 입력 (헤드라인, 요약, 카테고리)

  또는 다른 스크립트에서 import:
  from news_push import push_article
  push_article(date_str, headline, summary, category)
"""
import urllib.request, urllib.error
import json, datetime, os, sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

TOKEN       = os.getenv('NOTION_TOKEN', '')
DATABASE_ID = os.getenv('NOTION_DB_ID', '3484fcedeeb68064be8bd499a4f9f459')
MD_BASE_DIR = r'C:\Users\foodiverse1\카드뉴스\_뉴스취합'

DAYS_KR = ['월', '화', '수', '목', '금', '토', '일']


def _iso_week_str(date_str: str) -> str:
    d = datetime.date.fromisoformat(date_str)
    iso = d.isocalendar()
    return f'{iso[0]}-W{iso[1]:02d}'


def _day_name(date_str: str) -> str:
    d = datetime.date.fromisoformat(date_str)
    return DAYS_KR[d.weekday()]


def push_to_notion(date_str: str, headline: str, summary: str, category: str) -> str:
    payload = {
        'parent': {'database_id': DATABASE_ID},
        'properties': {
            '이름':    {'title': [{'text': {'content': headline}}]},
            '날짜':    {'date': {'start': date_str}},
            '선택':    {'select': {'name': category}},
            'Summary': {'rich_text': [{'text': {'content': summary}}]},
            '게시':    {'checkbox': True},
        }
    }
    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    req = urllib.request.Request('https://api.notion.com/v1/pages', data=body, method='POST')
    req.add_header('Authorization', 'Bearer ' + TOKEN)
    req.add_header('Notion-Version', '2022-06-28')
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read().decode('utf-8'))
    return result['id']


def append_to_local_md(date_str: str, headline: str, category: str) -> str:
    week_str = _iso_week_str(date_str)
    day_name = _day_name(date_str)
    md_path  = os.path.join(MD_BASE_DIR, f'{week_str}.md')
    os.makedirs(MD_BASE_DIR, exist_ok=True)

    # 같은 날짜 헤더가 이미 있으면 헤더 없이 bullet만 추가
    header_line = f'## {date_str} ({day_name})'
    existing = ''
    if os.path.exists(md_path):
        with open(md_path, 'r', encoding='utf-8') as f:
            existing = f.read()

    if header_line in existing:
        block = f'- [{category}] {headline}\n'
    else:
        block = f'\n{header_line}\n\n- [{category}] {headline}\n'

    with open(md_path, 'a', encoding='utf-8') as f:
        f.write(block)
    return md_path


def push_article(date_str: str, headline: str, summary: str, category: str):
    print(f'[Notion] 푸시 중... {headline[:40]}')
    page_id = push_to_notion(date_str, headline, summary, category)
    print(f'[Notion] SUCCESS: {page_id}')

    md_path = append_to_local_md(date_str, headline, category)
    print(f'[Local]  기록 완료: {md_path}')


if __name__ == '__main__':
    today = datetime.date.today().isoformat()
    print(f'날짜 [{today}] — 다른 날짜면 직접 입력 (Enter 시 오늘): ', end='')
    date_in = input().strip() or today

    print('헤드라인: ', end='')
    headline = input().strip()

    print('요약 (여러 줄 가능, 빈 줄 입력 시 종료):')
    lines = []
    while True:
        line = input()
        if line == '':
            break
        lines.append(line)
    summary = '\n'.join(lines)

    print('카테고리 (Weekly Report / Alert / Trend) [기본: Weekly Report]: ', end='')
    cat_in = input().strip()
    category = cat_in if cat_in else 'Weekly Report'

    push_article(date_in, headline, summary, category)
