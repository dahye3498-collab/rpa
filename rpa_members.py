"""
rpa_members.py
회원정보 / 등업신청 게시판 전용 크롤러

- 기준 파일: 회원데이터_통합_파싱완료_N.xlsx (가장 최신 버전)
- 실행마다 버전 자동 증가: _N → _N+1
- 저장 전 자동 백업 생성
- 내용이 없는 행은 재수집 대상으로 처리
"""

import os
import re
import time
import shutil
from datetime import datetime, timedelta
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import pandas as pd

load_dotenv()

LOGIN_EMAIL = os.getenv("LOGIN_EMAIL")
LOGIN_PWD   = os.getenv("LOGIN_PWD")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR   = os.path.join(BASE_DIR, "visionmeat", "database")

# 수집 cutoff (이 날짜 이전 글은 수집 안 함)
CUTOFF_DATE = datetime(2024, 3, 1).date()

# 게시판별 시작 페이지
# page25도 2017년 → 기존 2024-04 데이터가 page1~3 수준으로 추정
# resume 로직이 기존 데이터 스킵하므로 1페이지부터 시작해도 빠름
START_PAGE = {
    "회원정보": 1,
    "등업신청": 1,
}

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

# 게시판별 컬럼 정의 (파싱완료_1.xlsx 구조 기준)
COLS = {
    "회원정보": ["게시판", "제목", "작성자", "작성일", "내용",
                "이름", "회사명", "연락처1", "연락처2", "회사소개", "기타"],
    "등업신청": ["게시판", "제목", "작성자", "작성일",
                "닉네임", "성명", "회사명", "직책",
                "회사전화번호", "회사팩스번호", "핸드폰번호",
                "설립년월", "직원수", "주력브랜드/품목",
                "주사용창고", "배송가능지역", "회사소개", "회사주소"],
}

# ---------------------------------------------------------------------------
# 버전 관리
# ---------------------------------------------------------------------------
FILE_PREFIX = "회원데이터_통합_파싱완료_"

def get_latest_version() -> int:
    """DB_DIR에서 파싱완료_N.xlsx 파일 중 가장 큰 N을 반환. 없으면 0."""
    os.makedirs(DB_DIR, exist_ok=True)
    versions = []
    for f in os.listdir(DB_DIR):
        if f.startswith(FILE_PREFIX) and f.endswith(".xlsx") and ".bak" not in f:
            try:
                n = int(f.replace(FILE_PREFIX, "").replace(".xlsx", ""))
                versions.append(n)
            except ValueError:
                pass
    return max(versions) if versions else 0

def get_latest_path() -> str | None:
    v = get_latest_version()
    if v == 0:
        return None
    return os.path.join(DB_DIR, f"{FILE_PREFIX}{v}.xlsx")

def get_next_path() -> str:
    v = get_latest_version()
    return os.path.join(DB_DIR, f"{FILE_PREFIX}{v + 1}.xlsx")

# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------
def log(msg):
    text = str(msg)
    try:
        safe = text.encode("cp949", errors="ignore").decode("cp949")
    except Exception:
        safe = text
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {safe}", flush=True)


def parse_date(date_str):
    now = datetime.now()
    if ":" in str(date_str):
        return now
    try:
        parts = [p.strip() for p in str(date_str).split(".") if p.strip()]
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


def get_field(content: str, patterns: list) -> str:
    """content에서 첫 번째 매칭 패턴의 값을 반환. 줄바꿈 이후는 잘라냄."""
    for p in patterns:
        m = re.search(p, content, re.IGNORECASE | re.MULTILINE)
        if m:
            return m.group(1).strip().split("\n")[0].strip()
    return ""


def get_multiline_field(content: str, patterns: list) -> str:
    """레이블 이후 다음 빈 줄까지 여러 줄을 반환."""
    for p in patterns:
        m = re.search(p, content, re.DOTALL)
        if m:
            return m.group(1).strip()
    return ""

# ---------------------------------------------------------------------------
# 게시판별 파서
# ---------------------------------------------------------------------------
def parse_회원정보(content: str) -> dict:
    r = {c: "" for c in COLS["회원정보"]}
    if not content:
        return r
    r["이름"]    = get_field(content, [r"성명\s*:\s*(.+)", r"이름\s*:\s*(.+)", r"담당자\s*:\s*(.+)"])
    r["회사명"]  = get_field(content, [r"회사명\s*:\s*(.+)", r"현재회사\s*:\s*(.+)", r"업체명\s*:\s*(.+)"])
    r["연락처1"] = get_field(content, [r"회사전화번호\s*:\s*(.+)", r"회사전화\s*:\s*(.+)", r"대표전화\s*:\s*(.+)"])
    r["연락처2"] = get_field(content, [r"핸드폰번호\s*:\s*(.+)", r"휴대폰번호\s*:\s*(.+)", r"핸드폰\s*:\s*(.+)"])
    r["회사소개"] = get_multiline_field(content, [r"회사소개\s*:\s*(.+?)(?:\n\n|\Z)"])
    # 기타: 닉네임, 직책, 변경사항 등 나머지
    other = []
    for label, pats in [
        ("닉네임",       [r"닉네임\s*:\s*(.+)"]),
        ("직책",         [r"직책\s*:\s*(.+)"]),
        ("팩스번호",     [r"팩스번호\s*:\s*(.+)", r"회사팩스번호\s*:\s*(.+)"]),
        ("설립년월",     [r"설립년월\s*:\s*(.+)"]),
        ("직원수",       [r"직원수\s*:\s*(.+)"]),
        ("주력브랜드",   [r"주력브랜드.*?:\s*(.+)"]),
        ("회사주소",     [r"회사주소\s*:\s*(.+)"]),
        ("변경사항",     [r"정보\s*변경사항\s*:\s*(.+)"]),
    ]:
        v = get_field(content, pats)
        if v:
            other.append(f"{label}: {v}")
    r["기타"] = " | ".join(other)
    return r


def parse_등업신청(content: str) -> dict:
    r = {c: "" for c in COLS["등업신청"]}
    if not content:
        return r
    r["닉네임"]        = get_field(content, [r"닉네임\s*:\s*(.+)"])
    r["성명"]          = get_field(content, [r"성명\s*:\s*(.+)", r"이름\s*:\s*(.+)"])
    r["회사명"]        = get_field(content, [r"회사명\s*:\s*(.+)", r"업체명\s*:\s*(.+)"])
    r["직책"]          = get_field(content, [r"직책\s*:\s*(.+)"])
    r["회사전화번호"]  = get_field(content, [r"회사전화번호\s*:\s*(.+)", r"회사전화\s*:\s*(.+)", r"대표전화\s*:\s*(.+)"])
    r["회사팩스번호"]  = get_field(content, [r"회사팩스번호\s*:\s*(.+)", r"팩스\s*:\s*(.+)", r"FAX\s*:\s*(.+)"])
    r["핸드폰번호"]    = get_field(content, [r"핸드폰번호\s*:\s*(.+)", r"휴대폰번호\s*:\s*(.+)", r"핸드폰\s*:\s*(.+)"])
    r["설립년월"]      = get_field(content, [r"설립년월\s*:\s*(.+)"])
    r["직원수"]        = get_field(content, [r"직원수\s*:\s*(.+)"])
    r["주력브랜드/품목"] = get_field(content, [r"주력브랜드\s*(?:or\s*품목)?\s*:\s*(.+)", r"주력품목\s*:\s*(.+)"])
    r["주사용창고"]    = get_field(content, [r"주\s*사용\s*창고\s*:\s*(.+)", r"사용창고\s*:\s*(.+)"])
    r["배송가능지역"]  = get_field(content, [r"배송가능지역\s*:\s*(.+)", r"배송지역\s*:\s*(.+)"])
    r["회사소개"]      = get_multiline_field(content, [r"회사소개\s*:\s*(.+?)(?:\n\n|\Z)"])
    r["회사주소"]      = get_field(content, [r"회사주소\s*:\s*(.+)"])
    return r


PARSERS = {
    "회원정보": parse_회원정보,
    "등업신청": parse_등업신청,
}

# ---------------------------------------------------------------------------
# 기존 완료 데이터 키 로드 (내용 없는 행은 재수집 대상)
# ---------------------------------------------------------------------------
def load_existing_keys(board_name: str) -> set:
    path = get_latest_path()
    if not path or not os.path.exists(path):
        return set()
    try:
        df = pd.read_excel(path, sheet_name=board_name)
        title_col = next((c for c in df.columns if "제목" in str(c)), None)
        date_col  = next((c for c in df.columns if "작성일" in str(c)), None)
        if not title_col or not date_col:
            return set()
        # 등업신청은 내용 컬럼 없음 → 성명으로 완료 여부 판단
        if board_name == "등업신청":
            done_col = next((c for c in df.columns if "성명" in str(c)), None)
        else:
            done_col = next((c for c in df.columns if "내용" in str(c)), None)
        if done_col is not None:
            has_data = df[done_col].astype(str).str.strip().replace("nan", "") != ""
            df = df[has_data]
        keys = set(zip(df[title_col].astype(str), df[date_col].astype(str)))
        log(f"[{board_name}] 기존 완료 {len(keys)}건 → 재수집 스킵 적용")
        return keys
    except Exception as e:
        log(f"[{board_name}] 기존 데이터 로드 실패 (무시): {e}")
        return set()


# ---------------------------------------------------------------------------
# 게시판 진입
# ---------------------------------------------------------------------------
def ensure_board(page, selector: str, url: str, timeout_sec: int = 30) -> bool:
    detected = False
    log(f"게시판 이동 시도: {selector}")
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            loc = page.locator(selector).first
            if loc.count() > 0 and loc.is_visible():
                loc.click(force=True)
                page.wait_for_timeout(3000)
                detected = True
                break
        except Exception:
            pass
        for fr in page.frames:
            try:
                loc = fr.locator(selector).first
                if loc.count() > 0 and loc.is_visible():
                    loc.click(force=True)
                    page.wait_for_timeout(3000)
                    detected = True
                    break
            except Exception:
                continue
        if detected:
            break
        time.sleep(1.5)

    if not detected:
        log(f"메뉴 클릭 실패 → 직접 URL 이동: {url}")
        page.goto(url)
        page.wait_for_timeout(5000)

    try:
        page.wait_for_timeout(3000)
        page.frame_locator("iframe#down").locator("a.txt_item").first.wait_for(timeout=10000)
        log("게시판 진입 확인.")
    except Exception:
        log("게시판 로딩 확인 실패 (게시물 없음 가능)")
    return True


# ---------------------------------------------------------------------------
# 수집 핵심 로직 (URL 직접 이동 방식 - 페이지 버튼 클릭 불필요)
# ---------------------------------------------------------------------------
def extract_all_posts_text(page, board_url: str, board_name: str) -> list:
    """
    &page=N 파라미터로 각 페이지 URL을 직접 이동.
    버튼 클릭 방식 대신 URL 기반으로 안정적으로 페이지 순회.
    """
    log(f"[{board_name}] 수집 시작 (cutoff: {CUTOFF_DATE})")
    extracted     = []
    existing_keys = load_existing_keys(board_name)
    seen          = set()
    start = START_PAGE.get(board_name, 1)
    page_num      = start
    MAX_EMPTY_PAGES = 3  # 연속 빈 페이지 허용 횟수
    log(f"[{board_name}] {start}페이지부터 시작 (이전 페이지 스킵)")

    empty_streak = 0

    while True:
        url = f"{board_url}&page={page_num}"
        log(f"[{board_name}] 페이지 {page_num} 이동: {url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)
        except Exception as e:
            log(f"[{board_name}] 페이지 이동 오류: {e}")
            break

        board_frame = page.frame_locator("iframe#down")

        try:
            board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
        except Exception:
            empty_streak += 1
            log(f"[{board_name}] 페이지 {page_num} 목록 없음 (연속 {empty_streak}회)")
            if empty_streak >= MAX_EMPTY_PAGES:
                log(f"[{board_name}] 빈 페이지 {MAX_EMPTY_PAGES}회 연속 → 수집 종료")
                break
            page_num += 1
            continue

        empty_streak = 0
        rows = board_frame.locator("tr").all()
        if not rows:
            log(f"[{board_name}] 마지막 페이지 (페이지 {page_num})")
            break

        # ✅ 페이지 첫 번째 일반 글 날짜로 cutoff 판단 (공지 제외)
        first_date = None
        for row in rows:
            try:
                if row.locator(".ico_notice, .txt_notice, .txt_pill").count() > 0:
                    continue
                if row.locator("a.txt_item").count() == 0:
                    continue
                d = row.locator("span.tbl_txt_date").inner_text().strip()
                if d:
                    first_date = parse_date(d).date()
                    break
            except Exception:
                continue

        if first_date and first_date < CUTOFF_DATE:
            log(f"[{board_name}] 페이지 {page_num} 첫 글 날짜 {first_date} < cutoff → 수집 종료")
            break

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
                title    = link_loc.inner_text().strip()
                date_str = row.locator("span.tbl_txt_date").inner_text().strip()
                p_date   = parse_date(date_str).date()

                if p_date < CUTOFF_DATE:
                    continue

                key = (title, date_str)
                if key in seen:
                    continue
                seen.add(key)
                if key in existing_keys:
                    continue

                href = link_loc.get_attribute("href") or ""
                page_posts.append({"title": title, "date": date_str, "href": href})
            except Exception:
                continue

        if page_posts:
            log(f"[{board_name}] 페이지 {page_num}: {len(page_posts)}건 신규")
        extracted.extend(_visit_posts(page, board_frame, board_name, page_posts))

        page_num += 1
        time.sleep(3)

    log(f"[{board_name}] 신규 수집 완료: {len(extracted)}건")
    return extracted


def _fetch_post_content(page, href: str) -> tuple[str, str]:
    """
    백그라운드 JS fetch로 게시글 내용 가져오기.
    실제 페이지 방문(iframe 이동) 없이 HTTP 요청 → 조회수 미증가.
    반환: (content_text, author)
    """
    if not href or href.startswith("javascript"):
        return "", ""

    if href.startswith("/"):
        full_url = "https://cafe.daum.net" + href
    elif href.startswith("http"):
        full_url = href
    else:
        return "", ""

    try:
        html = page.evaluate(f"""
        async () => {{
            try {{
                const res = await fetch('{full_url}', {{
                    credentials: 'include',
                    headers: {{
                        'Accept': 'text/html,application/xhtml+xml',
                        'X-Requested-With': 'XMLHttpRequest'
                    }}
                }});
                return res.ok ? await res.text() : '';
            }} catch(e) {{ return ''; }}
        }}
        """)

        if not html:
            return "", ""

        # #user_contents 내부 텍스트 추출
        content_text = ""
        m = re.search(r'id=["\']user_contents["\'][^>]*>([\s\S]*?)</div>', html)
        if m:
            raw = m.group(1)
            # HTML 태그·엔티티 제거
            raw = re.sub(r'<[^>]+>', ' ', raw)
            raw = raw.replace('&nbsp;', ' ').replace('&amp;', '&') \
                     .replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"')
            content_text = re.sub(r'\s+', ' ', raw).strip()

        # 작성자 추출
        author = ""
        for pat in [r'class="nick_txt[^"]*"[^>]*>([^<]+)<',
                    r'class="txt_sub[^"]*"[^>]*>.*?class="txt_item"[^>]*>([^<]+)<']:
            am = re.search(pat, html)
            if am:
                author = am.group(1).strip()
                break

        return content_text, author

    except Exception as e:
        log(f"fetch 오류: {e}")
        return "", ""


def _visit_posts(page, board_frame, board_name: str, posts: list) -> list:
    """
    백그라운드 fetch로 게시글 내용 추출 (조회수 미증가).
    fetch 실패 시 기존 클릭 방식으로 폴백.
    """
    results = []
    parser  = PARSERS[board_name]
    cols    = COLS[board_name]

    for post in posts:
        log(f"[{board_name}] 추출(API): {post['title']}")
        content_text, author = "", ""

        # 1차: 백그라운드 fetch 시도
        if post.get("href"):
            content_text, author = _fetch_post_content(page, post["href"])

        # 2차: fetch 실패 시 기존 클릭 방식 폴백
        if not content_text:
            log(f"[{board_name}] fetch 실패 → 클릭 방식으로 폴백: {post['title']}")
            for attempt in range(1, 4):
                try:
                    board_frame.locator("a.txt_item").filter(has_text=post["title"]).first.click()
                    time.sleep(3 + attempt * 1.5)
                    ca = board_frame.locator("#user_contents")
                    if ca.count() > 0:
                        content_text = ca.inner_text().strip()
                    if not content_text and attempt < 3:
                        log(f"[{board_name}] 내용 빈값 → {attempt+1}차 재시도")
                        try:
                            board_frame.locator("#article-list-btn").or_(
                                board_frame.get_by_role("link", name="목록", exact=True)
                            ).first.click()
                            time.sleep(3)
                            board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
                        except Exception:
                            pass
                        continue
                    if not author:
                        for sel in [".txt_sub .txt_item", ".nick_txt", ".article_writer"]:
                            try:
                                loc = board_frame.locator(sel).first
                                if loc.count() > 0 and loc.is_visible():
                                    author = loc.inner_text().strip()
                                    if author: break
                            except Exception:
                                pass
                    break
                except Exception as e:
                    log(f"[{board_name}] 클릭 {attempt}차 오류: {e}")
                    time.sleep(3)
            # 클릭 방식 후 목록 복귀
            try:
                board_frame.locator("#article-list-btn").or_(
                    board_frame.get_by_role("link", name="목록", exact=True)
                ).first.click()
                time.sleep(3)
                board_frame.locator("a.txt_item").first.wait_for(timeout=10000)
            except Exception:
                pass

        if not content_text:
            log(f"[{board_name}] '{post['title']}' 최종 실패 → 빈값 저장")

        parsed = parser(content_text)
        row = {c: "" for c in cols}
        row["게시판"] = board_name
        row["제목"]   = post["title"]
        row["작성자"] = author
        row["작성일"] = post["date"]
        if "내용" in cols:
            row["내용"] = content_text
        row.update({k: v for k, v in parsed.items() if k in cols})
        results.append(row)

    return results


# ---------------------------------------------------------------------------
# 저장 (버전 증가 + 백업)
# ---------------------------------------------------------------------------
def save_excel(all_data: dict):
    os.makedirs(DB_DIR, exist_ok=True)
    latest_path = get_latest_path()
    next_path   = get_next_path()
    next_ver    = get_latest_version() + 1

    # ✅ 기존 데이터 먼저 읽기 (ExcelWriter 열기 전)
    existing = {}
    if latest_path and os.path.exists(latest_path):
        for board_name in COLS:
            try:
                existing[board_name] = pd.read_excel(latest_path, sheet_name=board_name)
            except Exception:
                existing[board_name] = pd.DataFrame(columns=COLS[board_name])
        # ✅ 백업 생성
        bak_path = latest_path.replace(".xlsx", ".bak.xlsx")
        shutil.copy2(latest_path, bak_path)
        log(f"백업 생성: {os.path.basename(bak_path)}")
    else:
        for board_name in COLS:
            existing[board_name] = pd.DataFrame(columns=COLS[board_name])

    # ✅ 새 버전 파일로 저장
    with pd.ExcelWriter(next_path, engine="openpyxl") as writer:
        for board_name in ["회원정보", "등업신청"]:
            new_rows = all_data.get(board_name, [])
            df_new   = pd.DataFrame(new_rows, columns=COLS[board_name]) if new_rows else pd.DataFrame(columns=COLS[board_name])
            df_exist = existing.get(board_name, pd.DataFrame(columns=COLS[board_name]))

            df_combined = pd.concat([df_exist, df_new], ignore_index=True)

            # 제목+작성일 기준 중복 제거 (기존 우선)
            t_col = next((c for c in df_combined.columns if "제목" in str(c)), None)
            d_col = next((c for c in df_combined.columns if "작성일" in str(c)), None)
            if t_col and d_col:
                df_combined = df_combined.drop_duplicates(subset=[t_col, d_col], keep="first")

            df_combined.to_excel(writer, sheet_name=board_name, index=False)
            log(f"[{board_name}] 기존 {len(df_exist)}건 + 신규 {len(df_new)}건 = 총 {len(df_combined)}건")

    log(f"\n저장 완료: {os.path.basename(next_path)} (v{next_ver})")
    return next_path


# ---------------------------------------------------------------------------
# 메인 실행
# ---------------------------------------------------------------------------
def run_rpa_members(credentials=None):
    cred = credentials or {}
    _login_email = cred.get("login_email") or LOGIN_EMAIL
    _login_pwd   = cred.get("login_pwd")   or LOGIN_PWD

    log(f"기준 파일: {os.path.basename(get_latest_path()) if get_latest_path() else '없음 (첫 실행)'}")
    log(f"저장 예정: {os.path.basename(get_next_path())}")

    with sync_playwright() as p:
        is_server = os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("PORT")
        browser   = p.chromium.launch(headless=bool(is_server))
        context   = browser.new_context(viewport={"width": 1280, "height": 1024})
        page      = context.new_page()

        log("다음 카페 접속 중...")
        try:
            page.goto("https://cafe.daum.net/meetpeople", wait_until="networkidle", timeout=60000)
        except Exception as e:
            log(f"접속 재시도: {e}")
            page.goto("https://cafe.daum.net/meetpeople", wait_until="domcontentloaded", timeout=60000)

        page.evaluate("document.body.style.zoom = '1'")

        # ✅✅✅ 로그인 블록 (수정 금지) ✅✅✅
        try:
            log("로그인 상태 확인 중...")
            page.wait_for_timeout(3000)
            is_logged_in = page.locator("text='로그아웃'").count() > 0 or page.locator("a.link_logout").count() > 0

            if not is_logged_in:
                login_found = False
                for sel in ["#loginout", "#loginBtn", "a:has-text('로그인')", "button:has-text('로그인')", ".btn_login"]:
                    if page.locator(sel).is_visible():
                        page.click(sel, force=True)
                        login_found = True
                        break
                if not login_found:
                    page.goto("https://logins.daum.net/accounts/loginform.do?url=https%3A%2F%2Fcafe.daum.net%2Fmeetpeople")

                page.wait_for_timeout(3000)
                if "logins.daum.net" in page.url:
                    for k_sel in ["button:has-text('카카오로 로그인')", ".btn_kakao", ".login__container--btn-kakao", "text='카카오계정으로 로그인'"]:
                        if page.locator(k_sel).count() > 0:
                            page.click(k_sel)
                            break

                page.wait_for_url("**/accounts.kakao.com/login**", timeout=30000)
                page.wait_for_load_state("networkidle")

                for i_sel in ["input[name='loginId']", "input#loginId--1", "input[type='text']", "input[type='email']"]:
                    try:
                        loc = page.locator(i_sel).first
                        if loc.is_visible(timeout=5000):
                            loc.scroll_into_view_if_needed()
                            loc.click()
                            page.fill(i_sel, _login_email)
                            break
                    except Exception:
                        continue

                for p_sel in ["input[name='password']", "input#password--2", "input[type='password']"]:
                    try:
                        loc = page.locator(p_sel).first
                        if loc.is_visible(timeout=5000):
                            loc.scroll_into_view_if_needed()
                            loc.click()
                            page.fill(p_sel, _login_pwd)
                            break
                    except Exception:
                        continue

                for s_sel in ["button.submit", "button:has-text('로그인')", ".btn_g.highlight.submit"]:
                    loc = page.locator(s_sel)
                    if loc.is_visible():
                        loc.scroll_into_view_if_needed()
                        loc.click()
                        break

                page.wait_for_timeout(5000)
                try:
                    page.wait_for_url("https://cafe.daum.net/meetpeople**", timeout=10000)
                except Exception:
                    pass

                if "accounts.kakao.com" in page.url or "logins.daum.net" in page.url:
                    try:
                        page.evaluate("""() => {
                            const s = document.createElement('style');
                            s.textContent = 'html,body{overflow:auto!important;height:auto!important;position:static!important;}';
                            document.head.append(s);
                        }""")
                    except Exception:
                        pass
                    log("\n" + "=" * 60)
                    log("!!! 캡차/로그인 실패 감지. 해결 후 Enter를 눌러주세요. !!!")
                    log("=" * 60 + "\n")
                    input("완료 후 Enter...")
                    page.wait_for_timeout(3000)
            else:
                log("이미 로그인된 상태입니다.")
        except Exception as e:
            log(f"로그인 이슈 (계속 진행): {e}")
        # ✅✅✅ 로그인 블록 끝 ✅✅✅

        all_data = {}
        try:
            for b in BOARDS:
                log(f"\n{'='*60}\n[{b['name']}] 게시판 수집 시작\n{'='*60}")
                rows = extract_all_posts_text(page, b["url"], b["name"])
                all_data[b["name"]] = rows
                # ✅ 게시판 하나 끝날 때마다 즉시 저장
                log(f"[{b['name']}] 수집 완료 → 저장 중...")
                save_excel(all_data)
        except KeyboardInterrupt:
            log("\n[!] 중단 감지 → 수집된 데이터 저장 중...")
            # all_data에 현재까지 완료된 게시판 데이터가 있으면 저장
            any_rows = any(len(v) > 0 for v in all_data.values())
            if any_rows:
                try:
                    browser.close()
                except Exception:
                    pass
                save_excel(all_data)
            else:
                log("저장할 신규 데이터 없음.")
            return
        finally:
            try:
                browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    run_rpa_members()
