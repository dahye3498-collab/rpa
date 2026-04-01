"""
rpa_members.py
회원정보 / 등업신청 게시판 전용 크롤러
- 최근 2년치 게시글을 한 번의 게시판 진입으로 페이지 순차 탐색하여 수집
- 통합 Excel 파일(시트 분리)로 저장
"""

import os
import time
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import pandas as pd

load_dotenv()

# Configuration
LOGIN_EMAIL = os.getenv("LOGIN_EMAIL")
LOGIN_PWD = os.getenv("LOGIN_PWD")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "visionmeat", "database")

# 2년 전 cutoff
CUTOFF_DATE = (datetime.now() - timedelta(days=730)).date()

BOARDS = [
    {
        "name": "회원정보",
        "selector": "[id^='fldlink_DrGV']",
        "url": "https://cafe.daum.net/_c21_/bbs_list?grpid=Mbmh&fldid=DrGV",
    },
    {
        "name": "등업신청",
        "selector": "[id^='fldlink_HoSn']",
        "url": "https://cafe.daum.net/_c21_/bbs_list?grpid=Mbmh&fldid=HoSn",
    },
]


def log(msg):
    text = str(msg)
    try:
        safe = text.encode("cp949", errors="ignore").decode("cp949")
    except Exception:
        safe = text
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {safe}", flush=True)


def parse_date(date_str):
    now = datetime.now()
    if ":" in date_str:
        return now
    try:
        parts = [p.strip() for p in date_str.split(".") if p.strip()]
        if len(parts) == 3:
            year = int(parts[0])
            if year < 100:
                year += 2000
            return datetime(year, int(parts[1]), int(parts[2]))
        elif len(parts) == 2:
            return datetime(now.year, int(parts[0]), int(parts[1]))
    except Exception:
        pass
    return now - timedelta(days=365)


def ensure_board(page, target_board_selector: str, direct_url: str, timeout_sec: int = 30) -> bool:
    board_detected = False
    log(f"게시판 이동 시도 중: {target_board_selector}")

    start_nav_time = time.time()
    while time.time() - start_nav_time < timeout_sec:
        try:
            target_loc = page.locator(target_board_selector).first
            if target_loc.count() > 0 and target_loc.is_visible():
                target_loc.click(force=True)
                page.wait_for_timeout(3000)
                board_detected = True
                break
        except Exception:
            pass

        for fr in page.frames:
            try:
                target_loc = fr.locator(target_board_selector).first
                if target_loc.count() > 0 and target_loc.is_visible():
                    target_loc.click(force=True)
                    page.wait_for_timeout(3000)
                    board_detected = True
                    break
            except Exception:
                continue

        if board_detected:
            break
        time.sleep(1.5)

    if not board_detected:
        log(f"메뉴 클릭 실패, 직접 URL로 이동: {direct_url}")
        page.goto(direct_url)
        page.wait_for_timeout(5000)

    try:
        page.wait_for_timeout(3000)
        board_frame = page.frame_locator("iframe#down")
        board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
        board_detected = True
        log("게시판(iframe#down) 진입 확인 완료.")
    except Exception:
        log("게시판 로딩 확인 실패 (또는 게시물이 없음)")
        board_detected = True

    return board_detected


# ---------------------------------------------------------------------------
# 핵심: cutoff 날짜까지 전체 페이지를 순차 탐색하며 텍스트 수집
# ---------------------------------------------------------------------------
def extract_all_posts_text(board_frame, board_name: str) -> list:
    """
    게시판의 1페이지부터 순차 탐색하며 cutoff(2년 전) 이후 글을 모두 수집한다.
    cutoff보다 오래된 글이 연속 5개 이상 나오면 탐색을 종료한다.
    """
    log(f"[{board_name}] 전체 수집 시작 (cutoff: {CUTOFF_DATE})")
    extracted = []
    seen = set()  # (제목, 날짜) 중복 방지
    page_num = 1

    while True:
        log(f"[{board_name}] 페이지 {page_num} 탐색 중...")

        try:
            board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
        except Exception:
            log(f"[{board_name}] 게시글 목록 없음. 탐색 종료.")
            break

        rows = board_frame.locator("tr").all()
        if not rows:
            break

        consecutive_old = 0
        page_posts = []

        for row in rows:
            try:
                is_notice = (
                    row.locator(".ico_notice, .txt_notice, .txt_pill").count() > 0
                    or "공지" in row.inner_text()
                    or "필독" in row.inner_text()
                )
                if is_notice:
                    continue

                link_loc = row.locator("a.txt_item")
                if link_loc.count() == 0:
                    continue

                title = link_loc.inner_text().strip()
                date_str = row.locator("span.tbl_txt_date").inner_text().strip()
                p_date = parse_date(date_str).date()

                if p_date < CUTOFF_DATE:
                    consecutive_old += 1
                    if consecutive_old >= 5:
                        log(f"[{board_name}] cutoff 이전 글 5개 연속 → 탐색 종료 (날짜: {date_str})")
                        break
                    continue

                consecutive_old = 0
                key = (title, date_str)
                if key in seen:
                    continue
                seen.add(key)

                page_posts.append({"title": title, "date": date_str})
            except Exception:
                continue

        # cutoff 도달로 중단
        if consecutive_old >= 5:
            # 이 페이지에서 수집할 글은 먼저 처리
            extracted.extend(_visit_posts(board_frame, board_name, page_posts))
            break

        extracted.extend(_visit_posts(board_frame, board_name, page_posts))

        # 다음 페이지 이동
        next_pg = board_frame.locator(f"a.link_num:has-text('{page_num + 1}')").first
        if next_pg.count() > 0 and next_pg.is_visible():
            log(f"[{board_name}] 다음 페이지({page_num + 1})로 이동")
            next_pg.click()
            page_num += 1
            time.sleep(5)
        else:
            log(f"[{board_name}] 마지막 페이지 도달 (페이지 {page_num})")
            break

    log(f"[{board_name}] 수집 완료: 총 {len(extracted)}건")
    return extracted


def _visit_posts(board_frame, board_name: str, posts: list) -> list:
    """목록에서 각 게시글에 진입하여 본문 텍스트를 추출하고 돌아온다."""
    results = []
    for post in posts:
        log(f"[{board_name}] 텍스트 추출: {post['title']}")
        try:
            post_link = board_frame.locator("a.txt_item").filter(has_text=post["title"]).first
            post_link.click()
            time.sleep(4)

            content_text = ""
            content_area = board_frame.locator("#user_contents")
            if content_area.count() > 0:
                content_text = content_area.inner_text().strip()

            author = ""
            for sel in [".txt_sub .txt_item", ".nick_txt", ".article_writer"]:
                try:
                    loc = board_frame.locator(sel).first
                    if loc.count() > 0 and loc.is_visible():
                        author = loc.inner_text().strip()
                        if author:
                            break
                except Exception:
                    pass

            results.append({
                "게시판": board_name,
                "제목": post["title"],
                "작성자": author,
                "작성일": post["date"],
                "내용": content_text,
            })

            # 목록으로 돌아가기
            list_btn = board_frame.locator("#article-list-btn").or_(
                board_frame.get_by_role("link", name="목록", exact=True)
            ).first
            list_btn.click()
            time.sleep(3)
            board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
        except Exception as e:
            log(f"[{board_name}] '{post['title']}' 처리 오류: {e}")
    return results


# ---------------------------------------------------------------------------
# 메인 실행
# ---------------------------------------------------------------------------
def run_rpa_members(credentials=None):
    cred = credentials or {}
    _login_email = cred.get("login_email") or LOGIN_EMAIL
    _login_pwd = cred.get("login_pwd") or LOGIN_PWD

    with sync_playwright() as p:
        is_server = os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("PORT")
        browser = p.chromium.launch(headless=bool(is_server))
        context = browser.new_context(viewport={"width": 1280, "height": 1024})
        page = context.new_page()

        # ── 카페 접속 ──
        log("다음 카페 접속 중...")
        try:
            page.goto("https://cafe.daum.net/meetpeople", wait_until="networkidle", timeout=60000)
        except Exception as e:
            log(f"초기 접속 경고 (재시도): {e}")
            page.goto("https://cafe.daum.net/meetpeople", wait_until="domcontentloaded", timeout=60000)

        page.evaluate("document.body.style.zoom = '1'")

        # ✅✅✅ 로그인 블록 (기존 rpa_automation.py 그대로) ✅✅✅
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
                    ".btn_login",
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
                    page.goto(
                        "https://logins.daum.net/accounts/loginform.do?url=https%3A%2F%2Fcafe.daum.net%2Fmeetpeople"
                    )

                page.wait_for_timeout(3000)
                if "logins.daum.net" in page.url:
                    kakao_selectors = [
                        "button:has-text('카카오로 로그인')",
                        ".btn_kakao",
                        ".login__container--btn-kakao",
                        "text='카카오계정으로 로그인'",
                    ]
                    for k_sel in kakao_selectors:
                        if page.locator(k_sel).count() > 0:
                            log(f"카카오 로그인 버튼({k_sel}) 클릭.")
                            page.click(k_sel)
                            break

                log("카카오 로그인 페이지 대기 중...")
                page.wait_for_url("**/accounts.kakao.com/login**", timeout=30000)
                page.wait_for_load_state("networkidle")

                log("아이디/비밀번호 입력 중...")
                id_selectors = ["input[name='loginId']", "input#loginId--1", "input[type='text']", "input[type='email']"]
                for i_sel in id_selectors:
                    try:
                        loc = page.locator(i_sel).first
                        if loc.is_visible(timeout=5000):
                            loc.scroll_into_view_if_needed()
                            loc.click()
                            page.fill(i_sel, _login_email)
                            break
                    except Exception:
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
                    except Exception:
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

                try:
                    page.wait_for_url("https://cafe.daum.net/meetpeople**", timeout=10000)
                    log("로그인 후 카페 홈 복귀 확인.")
                except Exception:
                    log("로그인 후 카페 홈 복귀 대기 중...")

                if "accounts.kakao.com" in page.url or "logins.daum.net" in page.url:
                    try:
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
                    except Exception:
                        pass

                    log("\n" + "=" * 60)
                    log("!!! 주의: 봇 체크(캡차) 또는 자동 로그인 실패가 감지되었습니다. !!!")
                    log("해결한 뒤 터미널에서 [Enter]를 누르면 계속 진행합니다.")
                    log("=" * 60 + "\n")
                    input("완료 후 Enter를 누르세요...")
                    page.wait_for_timeout(3000)
            else:
                log("이미 로그인된 상태입니다.")

        except Exception as e:
            log(f"로그인 도중 이슈 발생 (무시하고 계속 시도): {e}")
        # ✅✅✅ 로그인 블록 끝 ✅✅✅

        # ── 게시판별 수집 ──
        all_data = {}  # { board_name: [rows...] }

        for b in BOARDS:
            log(f"\n{'='*60}")
            log(f"[{b['name']}] 게시판 수집 시작")
            log(f"{'='*60}")

            ok = ensure_board(page, b["selector"], b["url"], timeout_sec=30)
            if not ok:
                log(f"[{b['name']}] 게시판 진입 실패. 스킵합니다.")
                continue

            board_frame = page.frame_locator("iframe#down")
            posts = extract_all_posts_text(board_frame, b["name"])
            all_data[b["name"]] = posts

        browser.close()

    # ── 통합 Excel 저장 ──
    os.makedirs(DB_DIR, exist_ok=True)
    output_path = os.path.join(DB_DIR, "회원데이터_통합.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for board_name in ["회원정보", "등업신청"]:
            rows = all_data.get(board_name, [])
            df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["게시판", "제목", "작성자", "작성일", "내용"])
            df.to_excel(writer, sheet_name=board_name, index=False)
            log(f"[{board_name}] 시트 저장: {len(rows)}건")

    log(f"\n통합 Excel 저장 완료: {output_path}")
    log("작업이 모두 끝났습니다.")


if __name__ == "__main__":
    run_rpa_members()
