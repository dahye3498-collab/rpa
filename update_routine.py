# -*- coding: utf-8 -*-
import urllib.request, urllib.error, json, sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Claude.ai API auth token (session cookie 방식 불가 - RemoteTrigger 툴로만 가능)
# 이 스크립트는 payload 검증용입니다.

prompt = (
    "매주 금요일, 글로벌 축산물 시장 동향 뉴스레터를 작성해 노션 데이터베이스에 자동 등록하는 작업입니다.\n\n"
    "## 1단계: WebSearch로 실제 데이터 수집\n"
    "WebSearch 툴로 아래 키워드를 검색해 이번 주 핵심 동향을 찾으세요:\n"
    "- global beef pork livestock market price 2026\n"
    "- USDA livestock outlook 2026\n"
    "- Australia beef export MLA 2026\n"
    "실제 수치(가격/수출량/점유율 등)가 포함된 신뢰할 수 있는 내용 1~2개를 선정하세요.\n\n"
    "## 2단계: 뉴스레터 내용 결정\n"
    "- 헤드라인: 짧고 임팩트 있는 한국어 제목 (핵심 수치 포함)\n"
    "- Summary: 2~3문장, 핵심 수치와 시사점, 출처 명시. 1800자 이하.\n"
    "- 카테고리: Weekly Report / Alert / Trend 중 내용에 맞게 선택\n\n"
    "## 3단계: 노션 등록\n"
    "아래 Python 코드를 Write 툴로 /tmp/notion_push.py에 저장한 뒤 Bash로 실행하세요.\n"
    "headline, summary, category 변수를 실제 내용으로 채우세요.\n\n"
    "```\n"
    "import urllib.request, json, datetime, os, sys\n"
    "sys.stdout.reconfigure(encoding='utf-8')\n"
    "token = os.environ['NOTION_TOKEN']\n"
    "database_id = os.environ.get('NOTION_DB_ID', '3484fcedeeb68064be8bd499a4f9f459')\n"
    "today = datetime.date.today().isoformat()\n"
    "headline = '여기에 실제 헤드라인'\n"
    "summary = '여기에 실제 요약'\n"
    "category = 'Weekly Report'\n"
    "\n"
    "# 1) 노션 푸시\n"
    "payload = {'parent': {'database_id': database_id}, 'properties': {"
    "'이름': {'title': [{'text': {'content': headline}}]}, "
    "'날짜': {'date': {'start': today}}, "
    "'선택': {'select': {'name': category}}, "
    "'Summary': {'rich_text': [{'text': {'content': summary}}]}, "
    "'게시': {'checkbox': True}}}\n"
    "body = json.dumps(payload, ensure_ascii=False).encode('utf-8')\n"
    "req = urllib.request.Request('https://api.notion.com/v1/pages', data=body, method='POST')\n"
    "req.add_header('Authorization', 'Bearer ' + token)\n"
    "req.add_header('Notion-Version', '2022-06-28')\n"
    "req.add_header('Content-Type', 'application/json; charset=utf-8')\n"
    "with urllib.request.urlopen(req) as resp:\n"
    "    result = json.loads(resp.read().decode('utf-8'))\n"
    "    print('SUCCESS:', result['id'])\n"
    "    print('URL:', result.get('url', ''))\n"
    "\n"
    "# 2) 로컬 MD 파일에 append (로컬 실행 시에만 동작; 원격 환경에선 무시)\n"
    "try:\n"
    "    d = datetime.date.fromisoformat(today)\n"
    "    iso = d.isocalendar()\n"
    "    week_str = f'{iso[0]}-W{iso[1]:02d}'\n"
    "    days_kr = ['월', '화', '수', '목', '금', '토', '일']\n"
    "    day_name = days_kr[d.weekday()]\n"
    "    md_dir = r'C:\\Users\\foodiverse1\\카드뉴스\\\\_뉴스취합'\n"
    "    md_path = os.path.join(md_dir, f'{week_str}.md')\n"
    "    os.makedirs(md_dir, exist_ok=True)\n"
    "    block = f'\\n## {today} ({day_name})\\n\\n- [{category}] {headline}\\n'\n"
    "    with open(md_path, 'a', encoding='utf-8') as f:\n"
    "        f.write(block)\n"
    "    print(f'LOCAL MD: {md_path}')\n"
    "except Exception as e:\n"
    "    print(f'LOCAL MD skip: {e}')\n"
    "```\n\n"
    "## 규칙\n"
    "- AI 생성 수치 사용 금지. WebSearch 확인 실제 데이터만 사용.\n"
    "- 데이터 미수집 시에도 노션 등록 진행. Summary에 사유 명시.\n"
    "- 완료 후 헤드라인, 카테고리, 날짜 출력."
)

payload = {
    "job_config": {
        "ccr": {
            "environment_id": "env_0163fmAoy79ZmzioPFQBFXWY",
            "session_context": {
                "model": "claude-sonnet-4-6",
                "sources": [{"git_repository": {"url": "https://github.com/dahye3498-collab/rpa"}}],
                "allowed_tools": ["Bash", "Read", "Write", "Edit", "Glob", "Grep", "WebSearch"]
            },
            "events": [{
                "data": {
                    "uuid": "c4e7a912-3b85-4f01-9d63-a82e1f5c7b34",
                    "session_id": "",
                    "type": "user",
                    "parent_tool_use_id": None,
                    "message": {"role": "user", "content": prompt}
                }
            }]
        }
    }
}

print(json.dumps(payload, ensure_ascii=False, indent=2)[:500])
print("... payload OK")
