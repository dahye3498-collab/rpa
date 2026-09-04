import urllib.request, json, datetime, sys, os
sys.stdout.reconfigure(encoding='utf-8')

token    = os.environ['NOTION_TOKEN']   # export NOTION_TOKEN=ntn_...
news_db  = '3484fcedeeb68064be8bd499a4f9f459'
sms_db   = '3d14fcedeeb681e99fe0e4a77627423b'
today    = datetime.date.today().isoformat()

kr_title   = 'FILL_KR_HEADLINE'
kr_summary = 'FILL_KR_SUMMARY'
en_title   = 'FILL_EN_HEADLINE'
en_summary = 'FILL_EN_SUMMARY'
category   = 'FILL_CATEGORY'   # Weekly Report | Alert

SMS_CAT_MAP = {'Weekly Report': '주간종합', 'Alert': '무역이슈'}
EMOJI = {'소고기': '🐄', '돼지고기': '🐖', '가금류': '🐔', '무역이슈': '🚢', '주간종합': '📊'}
sms_category = SMS_CAT_MAP.get(category, category)
emoji = EMOJI.get(sms_category, '📢')
raw = kr_summary.split('. ')[0] if '. ' in kr_summary else kr_summary[:80]
sms_content = f"[FOODIVERSE] {emoji} {raw}"
if len(sms_content) > 90:
    sms_content = sms_content[:89] + '…'


def post_json(url, payload):
    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    req  = urllib.request.Request(url, data=body, method='POST')
    req.add_header('Authorization', 'Bearer ' + token)
    req.add_header('Notion-Version', '2022-06-28')
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode('utf-8'))


# 1) 뉴스레터 DB
news_payload = {
    'parent': {'database_id': news_db},
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
news_result = post_json('https://api.notion.com/v1/pages', news_payload)
print('뉴스레터 등록 SUCCESS:', news_result['id'])

# 2) SMS DB
sms_payload = {
    'parent': {'database_id': sms_db},
    'properties': {
        '이름':      {'title':     [{'text': {'content': f"[{today}] {sms_category} SMS"}}]},
        '날짜':      {'date':      {'start': today}},
        'SMS_내용':  {'rich_text': [{'text': {'content': sms_content}}]},
        '카테고리':  {'select':    {'name': sms_category}},
        '발송상태':  {'select':    {'name': '대기중'}},
        '글자수':    {'number':    len(sms_content)},
        '원본_제목': {'rich_text': [{'text': {'content': kr_title}}]},
    }
}
sms_result = post_json('https://api.notion.com/v1/pages', sms_payload)
print('SMS DB 등록 SUCCESS:', sms_result['id'])
print(f"\n[SMS 미리보기] ({len(sms_content)}자)\n{sms_content}")
