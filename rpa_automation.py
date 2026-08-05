import os
import time
import base64
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from openai import OpenAI
import pandas as pd

load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VISION_MODEL = os.getenv("OPENAI_VISION_MODEL", "gpt-4.1")
LOGIN_EMAIL = os.getenv("LOGIN_EMAIL")
LOGIN_PWD = os.getenv("LOGIN_PWD")
# 날짜 범위는 자동 감지 (get_start_date() 함수 참조)

# Get the directory where the script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH = os.path.join(BASE_DIR, "ref", "국제식품_상품목록.xlsx")
DB_DIR = os.path.join(BASE_DIR, "visionmeat", "database")
# Dynamic filename: YYMMDD_미트피플_데이터.xlsx
OUTPUT_FILENAME = f"{datetime.now().strftime('%y%m%d')}_미트피플_데이터.xlsx"
OUTPUT_PATH = os.path.join(DB_DIR, OUTPUT_FILENAME)

# 텍스트만 추출하는 게시판 (스크린샷+OCR 대신 텍스트 복사 → Excel 직접 저장)
TEXT_BOARDS = {"구매", "판매", "회원정보", "등업신청"}

client = OpenAI(api_key=OPENAI_API_KEY)

def log(msg):
    """
    윈도우 콘솔(cp949)에서 깨지는 이모지 등을 안전하게 제거하고 출력.
    """
    text = str(msg)
    try:
        safe = text.encode("cp949", errors="ignore").decode("cp949")
    except Exception:
        safe = text
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {safe}", flush=True)

def get_missing_dates() -> list:
    """
    visionmeat 폴더 내 YYYY-MM-DD 형식의 날짜 폴더를 스캔하여
    [가장 오래된 날짜 ~ 오늘] 범위에서 캡처가 완료되지 않은 날짜 목록을 반환합니다.

    ✅ 수정된 로직:
    - 폴더가 존재해도 실제 스크린샷(.png/.jpg)이 없으면 "미완료"로 처리.
    - 중간에 중단된 날짜도 재수집 대상이 됨.
    - 폴더가 하나도 없으면 오늘 날짜만 포함한 리스트를 반환합니다.
    """
    vision_meat_root = os.path.join(BASE_DIR, "visionmeat")
    os.makedirs(vision_meat_root, exist_ok=True)

    CAPTURE_BOARDS = ["구매", "판매", "품목표", "회원정보", "등업신청"]

    existing_dates = set()
    if os.path.isdir(vision_meat_root):
        for entry in os.listdir(vision_meat_root):
            entry_path = os.path.join(vision_meat_root, entry)
            if not os.path.isdir(entry_path):
                continue
            try:
                d = datetime.strptime(entry, "%Y-%m-%d")
            except ValueError:
                continue  # YYYY-MM-DD 형식이 아닌 폴더는 무시

            # 폴더 존재 여부가 아닌 실제 데이터 존재 여부로 판단
            has_any_data = False
            for board in CAPTURE_BOARDS:
                if board in TEXT_BOARDS:
                    # 텍스트 게시판: text_data.json 또는 excel 존재 여부
                    text_file = os.path.join(entry_path, board, "text_data", "text_data.json")
                    excel_dir = os.path.join(entry_path, board, "excel")
                    if os.path.exists(text_file):
                        has_any_data = True
                        break
                    if os.path.isdir(excel_dir) and any(f.endswith(".xlsx") for f in os.listdir(excel_dir)):
                        has_any_data = True
                        break
                else:
                    # 스크린샷 게시판 (품목표)
                    cap_dir = os.path.join(entry_path, board, "screenshots")
                    if os.path.isdir(cap_dir):
                        if any(
                            f.lower().endswith((".png", ".jpg", ".jpeg"))
                            for f in os.listdir(cap_dir)
                        ):
                            has_any_data = True
                            break

            if has_any_data:
                existing_dates.add(d.date())

    today = datetime.now().date()
    # ✅ 사용자의 요청: 오늘 기준 2일 전까지만 수집하도록 제한 (오늘 포함 총 3일)
    start_point = today - timedelta(days=2)

    if existing_dates:
        # 기존 폴더 중 가장 이른 날짜와 제한 날짜 중 더 늦은 날을 시작점으로 설정
        earliest_existing = min(existing_dates)
        actual_start = max(earliest_existing, start_point)
        
        log(f"데이터 범위 제한: {actual_start.strftime('%Y-%m-%d')} ~ {today.strftime('%Y-%m-%d')}")
        
        missing = []
        cursor = actual_start
        while cursor <= today:
            if cursor not in existing_dates:
                missing.append(datetime.combine(cursor, datetime.min.time()))
            cursor += timedelta(days=1)
    else:
        log(f"기존 폴더 없음 → 오늘({today})부터 시작합니다.")
        missing = [datetime.combine(today, datetime.min.time())]

    if missing:
        log(f"수집 누락 날짜 {len(missing)}개 발견: {[d.strftime('%Y-%m-%d') for d in missing]}")
    else:
        log("누락된 날짜가 없습니다. 모두 수집 완료 상태입니다.")

    return missing

def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def extract_data_from_image(image_path):
    log(f"AI 분석 중: {os.path.basename(image_path)}...")
    base64_image = encode_image(image_path)

    prompt = """
    이 이미지는 축산물 품목표입니다. 이미지에서 다음 정보를 추출하여 JSON 객체 형태로 반환해주세요.
    루트 키는 "products"로 하고, 그 안에 각 품목 정보를 배열로 넣어주세요.

    추출할 필드(반드시 다음 필드명만 사용하세요):
    축종, 원산지, 보관, 품목, 브랜드, 등급, EST, 평중(kg), 스펙/설명, 재고(box), 창고, 소비기한, 판매가(원), 수정일, 비고

    데이터 매핑 및 병합 셀 주의사항 (중요):
    1. 원산지, 브랜드, 축종 등 병합된 셀의 값은 반드시 해당되는 **모든 행에 똑같이 반복해서** 채워넣어야 합니다. 절대 빈칸으로 두지 마세요.
    2. '포장'(예: VP, IWP, LP) 정보가 있다면 이는 반드시 '비고' 필드에 넣으세요.
    3. '품명' 혹은 유사한 정보는 반드시 '품목' 필드에 넣으세요.
    4. '브랜드명' 혹은 유사한 정보는 반드시 '브랜드' 필드에 넣으세요.
    5. 보관은 냉장, 냉동 등으로 구분하세요.
    6. 필드명이 정확히 매칭되지 않더라도 문맥상 파악 가능한 정보를 넣어주세요.
    """

    try:
        response = client.chat.completions.create(
            model=VISION_MODEL,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]
            }],
            response_format={"type": "json_object"}
        )
        content = response.choices[0].message.content
        data = json.loads(content)
        return data.get("products", [])
    except Exception as e:
        log(f"AI analysis error: {e}")
        return []

def parse_date(date_str):
    now = datetime.now()
    if ":" in date_str:
        return now
    try:
        parts = [p.strip() for p in date_str.split('.') if p.strip()]
        if len(parts) == 3:
            year = int(parts[0])
            if year < 100:
                year += 2000
            return datetime(year, int(parts[1]), int(parts[2]))
        elif len(parts) == 2:
            return datetime(now.year, int(parts[0]), int(parts[1]))
    except:
        pass
    return now - timedelta(days=365)

# -----------------------------
# ✅ 여기부터 "캡쳐 시작" 로직을 3개 게시판으로 확장
# -----------------------------
def ensure_board(page, target_board_selector: str, direct_url: str, timeout_sec: int = 30) -> bool:
    """
    대상 게시판 메뉴를 클릭하여 이동을 보장함.
    이전 게시판 내용이 남아있지 않도록 강제 클릭 및 URL 이동을 병행.
    """
    board_detected = False
    
    log(f"게시판 이동 시도 중: {target_board_selector}")
    
    start_nav_time = time.time()
    while time.time() - start_nav_time < timeout_sec:
        try:
            # 1. 탑 레벨에서 메뉴 클릭 시도
            target_loc = page.locator(target_board_selector).first
            if target_loc.count() > 0 and target_loc.is_visible():
                target_loc.click(force=True)
                page.wait_for_timeout(3000)
                board_detected = True
                break
        except:
            pass

        # 2. 모든 프레임 내부에서 메뉴 클릭 시도
        for fr in page.frames:
            try:
                target_loc = fr.locator(target_board_selector).first
                if target_loc.count() > 0 and target_loc.is_visible():
                    log(f"프레임({fr.name}) 내 메뉴 클릭 시도: {target_board_selector}")
                    target_loc.click(force=True)
                    page.wait_for_timeout(3000)
                    board_detected = True
                    break
            except:
                continue
        
        if board_detected:
            break
        time.sleep(1.5)

    if not board_detected:
        log(f"메뉴 클릭 실패 ({target_board_selector}), 직접 URL로 이동합니다: {direct_url}")
        page.goto(direct_url)
        page.wait_for_timeout(5000)

    # iframe#down이 로드되었는지 확인 (게시글 목록 a.txt_item 존재 여부)
    try:
        # 게시판이 완전히 바뀌었는지 확인하기 위해 목록 프레임 대기
        page.wait_for_timeout(3000)
        board_frame = page.frame_locator("iframe#down")
        
        # 목록이 로딩될 때까지 대기
        board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
        
        # 실제 URL이나 게시판 제목 등으로 한번 더 검증하면 좋으나, 일단 로딩 여부로 판단
        board_detected = True
        log(f"게시판(iframe#down) 진입 확인 완료.")
    except Exception as e:
        log(f"게시판 로딩 확인 실패 (또는 게시물이 없음): {e}")
        board_detected = True 

    return board_detected

def extract_board_posts_text(board_frame, board_name: str, text_dir: str, target_date: datetime = None) -> list:
    """
    텍스트 기반 게시판(구매/판매/회원정보/등업신청)에서 게시글 텍스트를 직접 추출합니다.
    스크린샷 대신 텍스트를 복사하여 리스트로 반환합니다.
    """
    log(f"1단계: [{board_name}] 텍스트 수집을 시작합니다.")
    os.makedirs(text_dir, exist_ok=True)

    extracted_data = []
    page_num = 1
    stop_searching = False

    while not stop_searching:
        log(f"[{board_name}] 게시글 목록(페이지 {page_num}) 검사 중...")

        try:
            board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
        except:
            log(f"[{board_name}] 게시글 목록을 찾을 수 없습니다. (데이터 없음)")
            break

        rows = board_frame.locator("tr").all()
        if not rows:
            break

        consecutive_old_posts = 0
        found_target_or_older = False
        current_page_posts = []

        for row in rows:
            try:
                is_notice = row.locator(".ico_notice, .txt_notice, .txt_pill").count() > 0 or \
                            "공지" in row.inner_text() or "필독" in row.inner_text()
                if is_notice:
                    continue

                link_loc = row.locator("a.txt_item")
                if link_loc.count() > 0:
                    title = link_loc.inner_text().strip()
                    date_str = row.locator("span.tbl_txt_date").inner_text().strip()

                    p_date = parse_date(date_str)
                    collect_date = target_date.date() if target_date else datetime.now().date()

                    if p_date.date() < collect_date:
                        found_target_or_older = True
                        consecutive_old_posts += 1
                        if consecutive_old_posts >= 5:
                            log(f"[{board_name}] 과거 글 5개 초과 발견으로 중단합니다. (날짜: {date_str})")
                            stop_searching = True
                            break
                        continue

                    if p_date.date() > collect_date:
                        continue

                    found_target_or_older = True
                    consecutive_old_posts = 0

                    current_page_posts.append({
                        "title": title,
                        "date": date_str,
                    })
            except:
                continue

        if not current_page_posts and not stop_searching:
            if found_target_or_older:
                log(f"[{board_name}] 현재 페이지에 대상 날짜 글이 없습니다.")
                stop_searching = True
            else:
                log(f"[{board_name}] 대상 날짜 글이 아직 나오지 않음. 다음 페이지로 계속 탐색...")

        for post in current_page_posts:
            log(f"[{board_name}] 텍스트 추출 중: {post['title']}")
            try:
                post_link = board_frame.locator("a.txt_item").filter(has_text=post['title']).first
                post_link.click()
                time.sleep(4)

                # 게시글 본문 텍스트 추출
                content_text = ""
                content_area = board_frame.locator("#user_contents")
                if content_area.count() > 0:
                    content_text = content_area.inner_text().strip()

                # 작성자 추출 시도
                author = ""
                try:
                    for sel in [".txt_sub .txt_item", ".nick_txt", ".article_writer"]:
                        author_loc = board_frame.locator(sel).first
                        if author_loc.count() > 0 and author_loc.is_visible():
                            author = author_loc.inner_text().strip()
                            if author:
                                break
                except:
                    pass

                extracted_data.append({
                    "제목": post['title'],
                    "작성자": author,
                    "작성일": post['date'],
                    "내용": content_text,
                })
                log(f"[{board_name}] 텍스트 추출 완료: {post['title']}")

                # 목록으로 돌아가기
                list_btn = board_frame.locator("#article-list-btn").or_(
                    board_frame.get_by_role("link", name="목록", exact=True)
                ).first
                list_btn.click()
                time.sleep(3)
                board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
            except Exception as e:
                log(f"[{board_name}] 게시글 '{post['title']}' 처리 중 오류: {e}")

        if stop_searching:
            break

        # 다음 페이지 이동
        next_pg = board_frame.locator(f"a.link_num:has-text('{page_num + 1}')").first
        if next_pg.count() > 0 and next_pg.is_visible():
            log(f"[{board_name}] 다음 페이지({page_num + 1})로 이동합니다.")
            next_pg.click()
            page_num += 1
            time.sleep(5)
        else:
            break

    # JSON으로 저장 (참조/백업용)
    json_path = os.path.join(text_dir, "text_data.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(extracted_data, f, ensure_ascii=False, indent=2)

    log(f"[{board_name}] 텍스트 수집 완료: {len(extracted_data)}건")
    return extracted_data


def _read_list_rows(page):
    """
    현재 iframe#down 목록의 게시글 행을 JS로 한 번에 읽어 (title, href, date) 리스트 반환.
    공지/필독 제외. 타이밍 이슈를 피하기 위해 DOM을 한 번에 평가.
    """
    real_frame = page.frame(name="down")
    if not real_frame:
        return []
    try:
        return real_frame.evaluate("""
            () => {
                const rows = Array.from(document.querySelectorAll('tr'));
                return rows
                    .filter(row => {
                        const link = row.querySelector('a.txt_item');
                        if (!link) return false;
                        if (/ico_notice|txt_notice|txt_pill/.test(row.innerHTML)) return false;
                        const txt = row.innerText || '';
                        if (txt.includes('공지') || txt.includes('필독')) return false;
                        return true;
                    })
                    .map(row => {
                        const link = row.querySelector('a.txt_item');
                        const dateEl = row.querySelector('span.tbl_txt_date');
                        return {
                            title: link.innerText.trim(),
                            href:  link.getAttribute('href') || '',
                            date:  dateEl ? dateEl.innerText.trim() : ''
                        };
                    });
            }
        """)
    except Exception as e:
        log(f"목록 읽기 오류: {e}")
        return []


def _goto_next_page(page, board_frame, page_num) -> bool:
    """
    다음 페이지로 이동. 성공 True.
    1) 숫자 버튼(a.link_num) 클릭
    2) '다음' 그룹 버튼(button.btn_item.btn_next 등)을 실제 Frame JS로 클릭
    """
    try:
        btn = board_frame.locator(f"a.link_num:has-text('{page_num + 1}')").first
        if btn.count() > 0 and btn.is_visible():
            btn.click()
            time.sleep(3)
            return True
    except Exception:
        pass

    try:
        rf = page.frame(name="down")
        if rf:
            clicked = rf.evaluate("""
                () => {
                    const sels = ['button.btn_item.btn_next','button.btn_g_ico.btn_item.btn_next',
                                  'button[class*="btn_next"]','a.btn_next'];
                    for (const s of sels) { const el = document.querySelector(s); if (el) { el.click(); return true; } }
                    return false;
                }
            """)
            if clicked:
                time.sleep(3)
                return True
    except Exception:
        pass

    return False


def capture_recent_posts(page, board_name, vision_meat_root, start_date,
                         max_posts=None, hooks=None, max_pages=80):
    """
    [기준일 ~ 오늘] 윈도우의 게시글을 스크린샷 캡처. 2단계 방식.

      1) 목록 순회(캡처 없음): 최신순으로 페이지를 넘기며 start_date(포함) 이후 글의
         (제목·href·날짜)를 수집. start_date보다 오래된 글 5개 연속 시 목록 순회 종료.
      2) 본문 캡처: 수집한 href로 iframe#down을 직접 이동시켜 #user_contents 스크린샷.
         목록 왕복이 없어 '목록 버튼이 1페이지로 튕기는' 문제가 없음.

    각 글은 실제 작성일 폴더(<root>/YYYY-MM-DD/<board>/screenshots)에 저장.
    이미 같은 제목의 png가 있으면 스킵(중복 방지).

    - max_posts: 지정 시 그만큼만 수집/캡처(검증용).
    """
    hooks = hooks or {}
    check_pause_stop = hooks.get("check_pause_stop")
    board_frame = page.frame_locator("iframe#down")

    # ── 1단계: 목록 순회하며 대상 글 수집 ──
    log(f"[{board_name}] 1단계: 목록 순회 (기준일 {start_date} 이후 수집)")
    worklist = []
    seen_keys = set()
    consecutive_old = 0
    page_num = 1

    while page_num <= max_pages:
        if check_pause_stop:
            check_pause_stop()

        try:
            board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
        except Exception:
            log(f"[{board_name}] 목록 없음 → 순회 종료")
            break

        items = _read_list_rows(page)
        if not items:
            log(f"[{board_name}] 목록 비어있음 → 순회 종료")
            break

        stop = False
        added = 0
        for it in items:
            date_str = it.get("date", "")
            title = it.get("title", "")
            href = it.get("href", "")
            try:
                p_date = parse_date(date_str).date()
            except Exception:
                continue

            if p_date < start_date:
                consecutive_old += 1
                if consecutive_old >= 5:
                    log(f"[{board_name}] 기준일 이전 글 5개 연속 → 목록 순회 종료 (날짜: {date_str})")
                    stop = True
                    break
                continue

            consecutive_old = 0
            key = (title, date_str)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            worklist.append({"title": title, "href": href, "date": date_str, "p_date": p_date})
            added += 1
            if max_posts is not None and len(worklist) >= max_posts:
                stop = True
                break

        log(f"[{board_name}] p{page_num}: 대상 {added}건 (누적 {len(worklist)})")
        if stop:
            break

        if not _goto_next_page(page, board_frame, page_num):
            log(f"[{board_name}] 마지막 페이지(p{page_num}) → 순회 종료")
            break
        page_num += 1

    log(f"[{board_name}] 대상 글 총 {len(worklist)}건. 2단계 캡처 시작...")

    # ── 2단계: href로 본문 이동 후 스크린샷 ──
    captured_data = []
    for idx, post in enumerate(worklist, 1):
        if max_posts is not None and len(captured_data) >= max_posts:
            break
        if check_pause_stop:
            check_pause_stop()

        date_folder = post["p_date"].strftime("%Y-%m-%d")
        capture_dir = os.path.join(vision_meat_root, date_folder, board_name, "screenshots")
        os.makedirs(capture_dir, exist_ok=True)

        safe_title = "".join(c for c in post["title"] if c.isalnum() or c in (' ', '_', '-')).strip().replace(' ', '_')
        existing = [f for f in os.listdir(capture_dir) if f.startswith(safe_title) and f.endswith(".png")]
        if existing:
            log(f"[{board_name}] 이미 캡처됨 스킵: {post['title']}")
            continue

        href = post["href"]
        if not href or href.startswith("javascript"):
            log(f"[{board_name}] href 없음 스킵: {post['title']}")
            continue
        full_url = ("https://cafe.daum.net" + href) if href.startswith("/") else href

        log(f"[{board_name}] 캡처 {idx}/{len(worklist)}: {post['title']} ({date_folder})")
        try:
            rf = page.frame(name="down")
            if not rf:
                log(f"[{board_name}] iframe 접근 실패 → 스킵")
                continue
            # iframe 내부에서 본문 페이지로 이동 (조회수는 증가하지만 스크린샷 필요)
            rf.evaluate("(u) => { window.location.href = u; }", full_url)
            page.wait_for_timeout(2500)

            content_area = board_frame.locator("#user_contents")
            content_area.first.wait_for(timeout=12000)
            # 이미지 로딩 여유
            page.wait_for_timeout(1800)

            timestamp = int(time.time())
            file_name = f"{safe_title}_{timestamp}.png"
            file_path = os.path.join(capture_dir, file_name)
            content_area.first.screenshot(path=file_path)
            captured_data.append({
                "board": board_name,
                "local_path": file_path,
                "title": post["title"],
                "date": post["date"],
            })
            log(f"[{board_name}] 저장 성공: {file_name}")
        except Exception as e:
            log(f"[{board_name}] 게시글 '{post['title']}' 캡처 오류: {e}")

    log(f"[{board_name}] 캡처 완료: {len(captured_data)}건")
    return captured_data


def run_rpa(date_list=None, hooks: dict | None = None, target_boards=None, credentials=None,
            max_posts=None):
    """
    캡처 RPA를 실행합니다.

    - date_list: 대상 날짜(datetime 객체) 리스트. None이면 get_missing_dates() 사용.
    - target_boards: ["구매","판매","품목표"] 중 선택. None이면 전체.
    - max_posts: 스크린샷 게시판(품목표) 캡처 건수 제한(검증용). None이면 무제한.
    - hooks (옵션): Job Manager 등에서 전달하는 콜백 모음.
      * hooks.get(\"on_step\"): 진행 상황 보고용 콜백
          on_step(phase: str, info: dict) 형태로 호출
      * hooks.get(\"check_pause_stop\"): 일시정지/중단 제어용 콜백
          check_pause_stop() 호출 시 필요하면 내부에서 block/예외 처리
    """
    if date_list is None:
        # 0. 자동으로 누락 날짜 감지 (기존 폴더 중 공백 + 오늘까지)
        date_list = get_missing_dates()

    if not date_list:
        log("수집할 날짜가 없습니다. (이미 모든 날짜 수집 완료)")
        return

    hooks = hooks or {}
    on_step = hooks.get("on_step")
    check_pause_stop = hooks.get("check_pause_stop")

    # 카카오 로그인 정보 (credentials 우선, 없으면 환경변수 fallback)
    cred = credentials or {}
    _login_email = cred.get("login_email") or LOGIN_EMAIL
    _login_pwd = cred.get("login_pwd") or LOGIN_PWD

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    vision_meat_root = os.path.join(BASE_DIR, "visionmeat")

    # ✅ 게시판 선택자 접두사 매칭으로 보완 (ID 뒤의 숫자가 바뀔 수 있음)
    all_boards = [
        {"name": "구매", "selector": "[id^='fldlink_HoTs']", "url": "https://cafe.daum.net/_c21_/recent_bbs_list?grpid=Mbmh&fldid=HoTs"},
        {"name": "판매", "selector": "[id^='fldlink_HoUW']", "url": "https://cafe.daum.net/_c21_/recent_bbs_list?grpid=Mbmh&fldid=HoUW"},
        {"name": "품목표", "selector": "[id^='fldlink_LdED']", "url": "https://cafe.daum.net/_c21_/recent_bbs_list?grpid=Mbmh&fldid=LdED"},
        {"name": "회원정보", "selector": "[id^='fldlink_DrGV']", "url": "https://cafe.daum.net/_c21_/bbs_list?grpid=Mbmh&fldid=DrGV"},
        {"name": "등업신청", "selector": "[id^='fldlink_HoSn']", "url": "https://cafe.daum.net/_c21_/bbs_list?grpid=Mbmh&fldid=HoSn"},
    ]
    boards = [b for b in all_boards if b["name"] in target_boards] if target_boards else all_boards
    log(f"대상 게시판: {[b['name'] for b in boards]}")

    with sync_playwright() as p:
        # 1. 브라우저 초기화 (한 번만 열고 날짜별로 반복)
        is_server = os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("PORT")
        browser = p.chromium.launch(headless=bool(is_server))
        context = browser.new_context(viewport={'width': 1280, 'height': 1024})
        page = context.new_page()

        log("다음 카페 접속 중...")
        try:
            page.goto("https://cafe.daum.net/meetpeople", wait_until="networkidle", timeout=60000)
        except Exception as e:
            log(f"초기 접속 경고 (재시도): {e}")
            page.goto("https://cafe.daum.net/meetpeople", wait_until="domcontentloaded", timeout=60000)

        page.evaluate("document.body.style.zoom = '1'")

        # 2. Automated Login (Adaptive)
        # ✅✅✅ 로그인 부분: 사용자가 요청한 대로 "그대로" 유지 (아래 블록 수정 금지) ✅✅✅
        try:
            log("로그인 상태 확인 및 진행 중...")
            page.wait_for_timeout(3000)

            is_logged_in = page.locator("text='로그아웃'").count() > 0 or page.locator("a.link_logout").count() > 0

            if not is_logged_in:
                log("로그인 버튼 찾는 중 (적응형 감지)...")
                login_selectors = [
                    "#loginout",
                    "#loginBtn",
                    "a:has-text('로그인')",
                    "button:has-text('로그인')",
                    ".btn_login"
                ]

                login_found = False
                for sel in login_selectors:
                    if page.locator(sel).is_visible():
                        log(f"로그인 버튼({sel}) 감지됨. 클릭합니다.")
                        page.click(sel, force=True)
                        login_found = True
                        break

                if not login_found:
                    log("로그인 버튼 직접 감지 실패, 게이트웨이 페이지로 직접 이동합니다.")
                    page.goto("https://logins.daum.net/accounts/loginform.do?url=https%3A%2F%2Fcafe.daum.net%2Fmeetpeople")

                page.wait_for_timeout(3000)
                if "logins.daum.net" in page.url:
                    kakao_selectors = [
                        "button:has-text('카카오로 로그인')",
                        ".btn_kakao",
                        ".login__container--btn-kakao",
                        "text='카카오계정으로 로그인'"
                    ]
                    for k_sel in kakao_selectors:
                        if page.locator(k_sel).count() > 0:
                            log(f"카카오 로그인 버튼(({k_sel})) 클릭.")
                            page.click(k_sel)
                            break

                log("카카오 로그인 페이지 대기 중...")
                page.wait_for_url("**/accounts.kakao.com/login**", timeout=30000)
                page.wait_for_load_state("networkidle")

                log("아이디/비밀번호 입력 중 (로그인창 위치 보정 포함)...")
                id_selectors = ["input[name='loginId']", "input#loginId--1", "input[type='text']", "input[type='email']"]
                for i_sel in id_selectors:
                    try:
                        loc = page.locator(i_sel).first
                        if loc.is_visible(timeout=5000):
                            loc.scroll_into_view_if_needed()
                            loc.click()
                            page.fill(i_sel, _login_email)
                            break
                    except:
                        continue

                pwd_selectors = ["input[name='password']", "input#password--2", "input[type='password']"]
                for p_sel in pwd_selectors:
                    try:
                        loc = page.locator(p_sel).first
                        if loc.is_visible(timeout=5000):
                            loc.scroll_into_view_if_needed()
                            loc.click()
                            page.fill(p_sel, _login_pwd)
                            break
                    except:
                        continue

                submit_selectors = ["button.submit", "button:has-text('로그인')", ".btn_g.highlight.submit"]
                for s_sel in submit_selectors:
                    loc = page.locator(s_sel)
                    if loc.is_visible():
                        loc.scroll_into_view_if_needed()
                        loc.click()
                        break

                log("로그인 제출 완료. 대기 중...")
                page.wait_for_timeout(5000)
                
                # 로그인 성공 여부 확인 (홈페이지로 돌아왔는지)
                try:
                    page.wait_for_url("https://cafe.daum.net/meetpeople**", timeout=10000)
                    log("로그인 후 카페 홈 복귀 확인.")
                except:
                    log("로그인 후 카페 홈 복귀 대기 중...")

                if "accounts.kakao.com" in page.url or "logins.daum.net" in page.url:
                    try:
                        log("로그인창 스크롤 및 레이아웃 강제 해제 시도 중...")
                        page.evaluate("""
                            () => {
                                const css = 'html, body { overflow: auto !important; height: auto !important; position: static !important; }';
                                const style = document.createElement('style');
                                style.textContent = css;
                                document.head.append(style);
                                document.documentElement.style.setProperty('overflow', 'auto', 'important');
                                document.body.style.setProperty('overflow', 'auto', 'important');
                                const loginContainer = document.querySelector('.cont_login') || document.querySelector('#login-container');
                                if (loginContainer) loginContainer.scrollIntoView();
                            }
                        """)
                    except:
                        pass

                    log("\n" + "="*60)
                    log("!!! 주의: 봇 체크(캡차) 또는 자동 로그인 실패가 감지되었습니다. !!!")
                    log("해결한 뒤 터미널에서 [Enter]를 누르면 계속 진행합니다.")
                    log("="*60 + "\n")
                    input("완료 후 Enter를 누르세요...")
                    page.wait_for_timeout(3000)
            else:
                log("이미 로그인된 상태입니다.")

        except Exception as e:
            log(f"로그인 도중 이슈 발생 (무시하고 계속 시도): {e}")
        # ✅✅✅ 로그인 블록 끝 ✅✅✅

        # ✅ 3) 캡처
        #  - 스크린샷 게시판(품목표 등): 최신순 단일 패스로 [window_start ~ 오늘] 윈도우 수집
        #  - 텍스트 게시판(구매/판매/회원정보/등업신청): 기존 날짜별 순차 방식 유지
        all_captured = []
        window_start = min(d.date() for d in date_list)
        screenshot_boards = [b for b in boards if b["name"] not in TEXT_BOARDS]
        text_boards = [b for b in boards if b["name"] in TEXT_BOARDS]

        # 3-1) 스크린샷 게시판: 윈도우 단일 패스
        for b in screenshot_boards:
            if check_pause_stop:
                check_pause_stop()

            log(f"\n===== [{b['name']}] 스크린샷 수집 시작 (기준일 {window_start} ~ 오늘) =====")
            if on_step:
                on_step("capture_board_start", {
                    "board": b["name"],
                    "window_start": window_start.strftime("%Y-%m-%d"),
                })
            ok = ensure_board(page, b["selector"], b["url"], timeout_sec=30)
            if not ok:
                log(f"[{b['name']}] 게시판 진입 실패. 다음 게시판으로 넘어갑니다.")
                continue

            captured = capture_recent_posts(
                page, b["name"], vision_meat_root, window_start,
                max_posts=max_posts, hooks=hooks,
            )
            all_captured.extend(captured)

            if on_step:
                on_step(
                    phase="capture",
                    info={
                        "board": b["name"],
                        "captured_count": len(captured),
                        "total_captured": len(all_captured),
                    },
                )

        # 3-2) 텍스트 게시판: 날짜별 순차 방식
        total_dates = len(date_list)
        for date_idx, target_date in enumerate(date_list, 1):
            if not text_boards:
                break
            if check_pause_stop:
                check_pause_stop()

            date_str = target_date.strftime("%Y-%m-%d")
            log(f"\n{'='*60}")
            log(f"▶ [{date_str}] 텍스트 게시판 수집 ({date_idx}/{total_dates})")
            log(f"{'='*60}")

            daily_dir = os.path.join(vision_meat_root, date_str)
            os.makedirs(daily_dir, exist_ok=True)

            for board_idx, b in enumerate(text_boards, 1):
                if check_pause_stop:
                    check_pause_stop()

                log(f"\n===== [{date_str}] [{b['name']}] 게시판 수집 시작 ({board_idx}/{len(text_boards)}) =====")
                if on_step:
                    on_step("capture_board_start", {
                        "date": date_str,
                        "board": b["name"],
                        "board_index": board_idx,
                        "total_boards": len(text_boards),
                        "date_index": date_idx,
                        "total_dates": total_dates,
                    })
                ok = ensure_board(page, b["selector"], b["url"], timeout_sec=30)
                if not ok:
                    log(f"[{b['name']}] 게시판 진입 실패. 다음 게시판으로 넘어갑니다.")
                    continue

                board_frame = page.frame_locator("iframe#down")
                text_dir = os.path.join(daily_dir, b["name"], "text_data")
                text_data = extract_board_posts_text(board_frame, b["name"], text_dir, target_date=target_date)

                if text_data:
                    excel_dir = os.path.join(daily_dir, b["name"], "excel")
                    os.makedirs(excel_dir, exist_ok=True)
                    df = pd.DataFrame(text_data)
                    output_filename = f"{date_str.replace('-', '')}_{b['name']}_데이터.xlsx"
                    df.to_excel(os.path.join(excel_dir, output_filename), index=False)
                    db_dir = os.path.join(vision_meat_root, "database")
                    os.makedirs(db_dir, exist_ok=True)
                    df.to_excel(os.path.join(db_dir, output_filename), index=False)
                    log(f"[{b['name']}] Excel 저장 완료: {output_filename} ({len(text_data)}건)")

                captured_count = len(text_data)
                all_captured.extend([{"board": b["name"], "title": d["제목"], "date": d["작성일"]} for d in text_data])

                if on_step:
                    on_step(
                        phase="capture",
                        info={
                            "date": date_str,
                            "board": b["name"],
                            "captured_count": captured_count,
                            "total_captured": len(all_captured),
                        },
                    )

            log(f"[{date_str}] 텍스트 수집 완료. 누적: {len(all_captured)}개")

        browser.close()

    if not all_captured:
        log("새롭게 수집된 데이터가 없습니다.")
    else:
        log(f"총 {len(all_captured)}개의 게시글 수집 완료.")
        log("텍스트 게시판(구매/판매/회원정보/등업신청)은 Excel 저장 완료.")
        log("품목표는 batch_processor.py로 AI 분석을 진행하세요.")

if __name__ == "__main__":
    run_rpa()