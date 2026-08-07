import os
import time
import base64
import json
import pandas as pd
try:
    import tkinter as tk
    from tkinter import ttk, messagebox
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from openai import OpenAI

try:
    from PIL import Image
    from io import BytesIO
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False

load_dotenv()

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VISION_MODEL = os.getenv("OPENAI_VISION_MODEL", "gpt-4.1")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "visionmeat", "database")
MAPPING_FILE = os.path.join(BASE_DIR, "ref", "유사어_매핑.xlsx")
SOURCE_MAPPING_FILE = os.path.join(BASE_DIR, "[운영] 자동변환_소스데이터.xlsx")

RESIZE_IMAGES = os.getenv("RESIZE_IMAGES", "0").strip() == "1"
MAX_IMG_SIZE = int(os.getenv("MAX_IMG_SIZE", "1280"))

# 품목표 세로로 긴 이미지 자동 타일 분할 OCR 설정
# (한 장 통째 OCR 시 API가 이미지를 축소해 환각·행 누락이 생기는 문제 방지)
OCR_TILE_THRESHOLD = int(os.getenv("OCR_TILE_THRESHOLD", "1400"))  # 이 높이(px) 초과 시 분할
OCR_TILE_HEIGHT = int(os.getenv("OCR_TILE_HEIGHT", "1500"))        # 밴드 1개 높이
OCR_TILE_OVERLAP = int(os.getenv("OCR_TILE_OVERLAP", "300"))       # 밴드 간 겹침(행 잘림 방지)

# 텍스트만 추출하는 게시판 (RPA 단계에서 이미 Excel 저장 완료, OCR 불필요)
TEXT_BOARDS = {"구매", "판매", "회원정보", "등업신청"}

client = OpenAI(api_key=OPENAI_API_KEY)


# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────
def log(msg):
    text = str(msg)
    try:
        safe = text.encode("cp949", errors="ignore").decode("cp949")
    except Exception:
        safe = text
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {safe}", flush=True)


def get_mime_by_ext(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".png":
        return "image/png"
    if ext in [".jpg", ".jpeg"]:
        return "image/jpeg"
    return "image/png"


def encode_image(image_path: str) -> str:
    if RESIZE_IMAGES and PIL_AVAILABLE:
        try:
            img = Image.open(image_path)
            img.thumbnail((MAX_IMG_SIZE, MAX_IMG_SIZE))
            buf = BytesIO()
            ext = os.path.splitext(image_path)[1].lower()
            if ext in [".jpg", ".jpeg"]:
                img = img.convert("RGB")
                img.save(buf, format="JPEG", quality=85)
            else:
                img.save(buf, format="PNG", optimize=True)
            return base64.b64encode(buf.getvalue()).decode("utf-8")
        except Exception as e:
            log(f"⚠️ 리사이즈 실패(원본 인코딩으로 진행): {e}")

    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def call_gpt_with_retry(messages, max_retry=3, base_sleep=2):
    last_err = None
    for attempt in range(1, max_retry + 1):
        try:
            return client.chat.completions.create(
                model=VISION_MODEL,
                messages=messages,
                response_format={"type": "json_object"},
            )
        except Exception as e:
            last_err = e
            log(f"❌ GPT 호출 실패 ({attempt}/{max_retry}) : {e}")
            time.sleep(base_sleep * attempt)
    log(f"❌ GPT 최종 실패: {last_err}")
    return None


# ─────────────────────────────────────────────
# SynonymMapper
# ─────────────────────────────────────────────
class SynonymMapper:
    def __init__(self, mapping_file: str, source_file: str = None):
        self.mapping_dict = {}
        if source_file:
            self.load_source_data(source_file)
        self.load_mapping_file(mapping_file)

    def load_source_data(self, file_path: str):
        """[운영] 자동변환_소스데이터.xlsx 로드: 시트별로 표준명→유사어 매핑 구축"""
        SHEET_TO_CATEGORY = {
            "브랜드명_한글사용": "브랜드",
            "품목명_한글사용": "품목",
            "창고명_한글사용": "창고",
            "축종_한글사용": "축종",
        }
        try:
            if not os.path.exists(file_path):
                log(f"소스데이터 파일 없음: {file_path}")
                return
            xls = pd.ExcelFile(file_path)
            total = 0
            for sheet_name, category in SHEET_TO_CATEGORY.items():
                if sheet_name not in xls.sheet_names:
                    continue
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                if category not in self.mapping_dict:
                    self.mapping_dict[category] = {}
                for _, row in df.iterrows():
                    values = [str(v).strip() for v in row if pd.notna(v) and str(v).strip()]
                    if not values:
                        continue
                    standard = values[0]
                    # 표준명 자체도 매핑에 등록 (소문자 → 표준명)
                    self.mapping_dict[category][standard.lower()] = standard
                    for synonym in values[1:]:
                        self.mapping_dict[category][synonym.lower()] = standard
                        total += 1
            log(f"소스데이터 매핑 로드 완료: {total}개 유사어")
        except Exception as e:
            log(f"소스데이터 로드 오류: {e}")

    def load_mapping_file(self, file_path: str):
        try:
            if os.path.exists(file_path):
                df = pd.read_excel(file_path)
                if "분류" not in df.columns or "원본값" not in df.columns or "표준값" not in df.columns:
                    log(f"❌ 매핑 파일 컬럼이 올바르지 않습니다: {file_path}")
                    return
                for _, row in df.iterrows():
                    category = str(row.get("분류", "")).strip()
                    original = str(row.get("원본값", "")).strip().lower()
                    standard = str(row.get("표준값", "")).strip()
                    if category and original and standard:
                        if category not in self.mapping_dict:
                            self.mapping_dict[category] = {}
                        self.mapping_dict[category][original] = standard
                log(f"유사어 매핑 로드 완료: {len(self.mapping_dict)}개 분류")
            else:
                log(f"유사어 매핑 파일을 찾을 수 없습니다: {file_path}")
        except Exception as e:
            log(f"유사어 매핑 파일 로드 오류: {e}")

    def normalize_value(self, category: str, value: str) -> str:
        if not value or pd.isna(value):
            return ""
        value_str = str(value).strip()
        if not value_str:
            return ""
        value_lower = value_str.lower()
        if category in self.mapping_dict:
            return self.mapping_dict[category].get(value_lower, value_str)
        return value_str

    def update_mapping(self, category: str, original: str, standard: str):
        if not category or not original or not standard:
            return
        original_lower = original.strip().lower()
        standard_trimmed = standard.strip()

        if category not in self.mapping_dict:
            self.mapping_dict[category] = {}
        if self.mapping_dict[category].get(original_lower) == standard_trimmed:
            return
        self.mapping_dict[category][original_lower] = standard_trimmed
        log(f"🆕 새 매핑 등록: [{category}] {original} -> {standard_trimmed}")

        try:
            file_path = MAPPING_FILE
            if os.path.exists(file_path):
                df = pd.read_excel(file_path)
            else:
                df = pd.DataFrame(columns=["분류", "원본값", "표준값"])
            if "원본값" not in df.columns:
                df["원본값"] = ""
            if "분류" not in df.columns:
                df["분류"] = ""
            if "표준값" not in df.columns:
                df["표준값"] = ""
            df["원본값"] = df["원본값"].fillna("").astype(str)
            mask = (df["분류"] == category) & (df["원본값"].str.lower() == original_lower)
            if mask.any():
                df.loc[mask, "표준값"] = standard_trimmed
            else:
                new_row = pd.DataFrame([{"분류": category, "원본값": original, "표준값": standard_trimmed}])
                df = pd.concat([df, new_row], ignore_index=True)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            df.to_excel(file_path, index=False)
            log(f"💾 매핑 파일 업데이트 완료: {file_path}")
        except Exception as e:
            log(f"❌ 매핑 파일 저장 오류: {e}")


# ─────────────────────────────────────────────
# Prompts  ← 핵심 개선 영역
# ─────────────────────────────────────────────
def _build_prompt(board_type: str) -> tuple[str, str]:
    """
    (prompt_text, response_key) 반환.
    board_type별로 GPT에게 정확히 어떤 JSON 구조를 요구할지 정의.
    """

    # ── 1. 품목표 ─────────────────────────────────────────────────────────────
    if board_type == "품목표":
        prompt = """
이 이미지는 축산물 품목표(재고/가격 리스트)입니다.
표에서 각 행(품목)을 개별 JSON 객체로 추출하여 아래 형식으로 반환하세요.

{
  "products": [
    {
      "축종": "소/돼지/닭/양 중 하나",
      "원산지": "호주/미국/뉴질랜드/브라질/국내 등",
      "보관": "냉동/냉장",
      "품목": "부위명 (예: 삼겹살, 목심, 갈비 등)",
      "브랜드": "브랜드명 (예: 엑셀, IBP, 스위프트 등)",
      "등급": "등급 정보 (없으면 빈 문자열)",
      "EST": "EST 번호 (없으면 빈 문자열)",
      "평중_kg": "평균 중량 숫자 (없으면 빈 문자열)",
      "스펙_설명": "규격·포장·트리밍 등 상세 설명",
      "재고_box": "재고 수량 숫자 (없으면 빈 문자열)",
      "창고": "보관 창고명",
      "소비기한": "YYYY-MM-DD 또는 원문 그대로",
      "판매가_원": "판매 단가 숫자 (없으면 빈 문자열)",
      "수정일": "날짜 (없으면 빈 문자열)",
      "비고": "VP/IWP 등 포장 방식, 기타 특이사항"
    }
  ]
}

추출 규칙:
1. 축종·원산지·브랜드·창고 등 병합 셀 값은 해당 그룹의 모든 행에 반드시 반복 적용하세요.
   특히 창고(보관위치)가 여러 행에 걸쳐 병합된 경우 빈칸으로 두지 말고 각 행에 같은 값을 채우세요.
2. 표의 모든 행을 하나도 빠짐없이 추출하세요. 요약·생략·중복 제거를 하지 마세요.
   먼저 표의 품목 행 수를 센 뒤, products 배열의 길이가 그 수와 일치하도록 하세요.
3. 셀에 보이는 값을 그대로(verbatim) 추출하세요. 품목명은 '품목'에, 브랜드명은 '브랜드'에 분리하세요.
4. 창고 컬럼과 등급·EST(넘버) 컬럼을 혼동하지 마세요. 숫자/영문코드(예: 86M, 208A)는 창고가 아니라 EST입니다.
5. 포장 방식(VP, IWP 등)은 '비고'에 넣으세요.
6. 숫자 필드(재고, 판매가, 평중)는 단위 없이 숫자만 넣으세요.
7. 값을 확인할 수 없으면 빈 문자열("")로 두세요. (단, 병합된 창고·원산지·브랜드는 규칙1에 따라 채웁니다.)
"""
        return prompt, "products"

    # ── 2. 등업신청 ──────────────────────────────────────────────────────────
    elif board_type == "등업신청":
        prompt = """
이 이미지는 축산물 커뮤니티의 '등업신청' 게시글 캡처본입니다.
이미지에서 보이는 모든 정보를 아래 JSON 형식으로 정확하게 추출하세요.

{
  "등업신청_info": {
    "등업구분": "정회원/우수회원 등 신청 등급 (게시글 제목에서 추출)",
    "닉네임": "커뮤니티 닉네임",
    "성명": "실명",
    "회사명": "회사 또는 사업체명",
    "직책": "직함 (예: 대표, 부장, 대리 등)",
    "회사전화번호": "대표 전화번호",
    "회사팩스번호": "팩스 번호",
    "핸드폰번호": "휴대폰 번호",
    "설립년월": "회사 설립 연월 (예: 2015-09)",
    "직원수": "직원 수 숫자",
    "주력브랜드_품목": "취급 브랜드 및 품목 전체 목록 (원문 그대로)",
    "주사용창고": "주로 사용하는 냉동/냉장 창고명",
    "배송가능지역": "배송 가능 지역",
    "회사주소": "사업장 주소 (하단 별도 기재된 경우 포함)",
    "회사소개": "회사 소개 문구 전체"
  }
}

추출 규칙:
1. 각 필드는 반드시 분리 추출하세요. 절대 한 필드에 합치지 마세요.
2. 닉네임이 제목란·상단에 표기된 경우에도 '닉네임' 필드에 넣으세요.
3. 회사주소가 하단 별도 영역에 있어도 반드시 추출하세요.
4. '주력브랜드_품목'은 원산지별·축종별 브랜드·품목 목록 전체를 줄바꿈 포함 그대로 유지하세요.
5. 값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, "등업신청_info"

    # ── 3. 회원정보 ──────────────────────────────────────────────────────────
    elif board_type == "회원정보":
        prompt = """
이 이미지는 축산물 커뮤니티 '회원정보' 게시글 캡처본입니다.
이미지에서 보이는 모든 정보를 아래 JSON 형식으로 정확하게 추출하세요.

{
  "회원정보_info": {
    "닉네임": "커뮤니티 닉네임",
    "성명": "실명",
    "회사명": "회사 또는 사업체명",
    "직책": "직함",
    "회사전화번호": "대표 전화번호",
    "회사팩스번호": "팩스 번호",
    "핸드폰번호": "휴대폰 번호",
    "설립년월": "회사 설립 연월",
    "직원수": "직원 수 숫자",
    "주력브랜드_품목": "취급 브랜드 및 품목 전체 목록 (원문 그대로)",
    "주사용창고": "주로 사용하는 냉동/냉장 창고명",
    "배송가능지역": "배송 가능 지역",
    "회사주소": "사업장 주소",
    "회사소개": "회사 소개 문구 전체",
    "가입일": "커뮤니티 가입일 (있는 경우)",
    "등급": "현재 회원 등급 (있는 경우)"
  }
}

추출 규칙:
1. 각 필드는 반드시 분리 추출하세요. 절대 한 필드에 합치지 마세요.
2. '주력브랜드_품목'은 원산지별·축종별 브랜드·품목 목록 전체를 줄바꿈 포함 그대로 유지하세요.
3. 값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, "회원정보_info"

    # ── 4. 구매 ──────────────────────────────────────────────────────────────
    elif board_type == "구매":
        prompt = """
이 이미지는 축산물 커뮤니티의 '구매(구합니다)' 게시글 캡처본입니다.
게시글에 여러 품목이 있으면 품목마다 별도 객체로 분리하여 배열로 반환하세요.
각 필드에는 반드시 하나의 값만 넣으세요. 절대 콤마로 여러 값을 합치지 마세요.

{
  "구매_items": [
    {
      "제목": "게시글 제목",
      "작성자": "작성자 닉네임 또는 성명",
      "작성일": "YYYY-MM-DD (불가시 원문)",
      "축종": "소/돼지/닭/양 중 하나만",
      "원산지": "호주/미국/국내 등 하나만",
      "브랜드": "브랜드명 하나만 (무관이면 빈 문자열)",
      "품목": "부위 또는 품목명 하나만",
      "등급": "등급 하나만 (없으면 빈 문자열)",
      "수량_box": "숫자만 (단위 제외, 없으면 빈 문자열)",
      "희망단가": "숫자만 (없으면 빈 문자열)",
      "희망납기일": "날짜 (없으면 빈 문자열)",
      "보관": "냉동 또는 냉장 (없으면 빈 문자열)",
      "업체명": "구매자 회사명",
      "담당자": "담당자 이름",
      "연락처": "전화 또는 핸드폰",
      "비고": "기타 요청사항, 스펙, 특이조건"
    }
  ]
}

추출 규칙 (매우 중요):
1. 품목이 여러 개면 품목마다 별도 행으로 분리하세요 (예: 삼겹살 1행, 목심 1행).
2. 각 행의 모든 필드에는 값이 하나만 들어가야 합니다. 콤마(,)로 여러 값을 합치지 마세요.
3. 공통 정보(작성자, 업체명, 연락처 등)는 모든 행에 동일하게 반복하세요.
4. 수량·단가는 단위 없이 숫자만 넣으세요.
5. 값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, "구매_items"

    # ── 5. 판매 ──────────────────────────────────────────────────────────────
    elif board_type == "판매":
        prompt = """
이 이미지는 축산물 커뮤니티의 '판매(팝니다)' 게시글 캡처본입니다.
게시글에 여러 품목이 있으면 품목마다 별도 객체로 분리하여 배열로 반환하세요.
각 필드에는 반드시 하나의 값만 넣으세요. 절대 콤마로 여러 값을 합치지 마세요.

{
  "판매_items": [
    {
      "제목": "게시글 제목",
      "작성자": "작성자 닉네임 또는 성명",
      "작성일": "YYYY-MM-DD (불가시 원문)",
      "축종": "소/돼지/닭/양 중 하나만",
      "원산지": "호주/미국/국내 등 하나만",
      "브랜드": "브랜드명 하나만",
      "품목": "부위 또는 품목명 하나만",
      "등급": "등급 하나만 (없으면 빈 문자열)",
      "EST": "EST 번호 (없으면 빈 문자열)",
      "수량_box": "숫자만 (단위 제외)",
      "판매단가": "숫자만 (없으면 빈 문자열)",
      "평중_kg": "숫자만 (없으면 빈 문자열)",
      "소비기한": "날짜 또는 원문",
      "보관": "냉동 또는 냉장",
      "창고": "보관 창고명",
      "업체명": "판매자 회사명",
      "담당자": "담당자 이름",
      "연락처": "연락처",
      "비고": "포장 방식(VP/IWP 등), 기타 특이사항"
    }
  ]
}

추출 규칙 (매우 중요):
1. 품목이 여러 개면 품목마다 별도 행으로 분리하세요 (예: 삼겹살 1행, 목심 1행).
2. 각 행의 모든 필드에는 값이 하나만 들어가야 합니다. 콤마(,)로 여러 값을 합치지 마세요.
3. 공통 정보(작성자, 업체명, 연락처 등)는 모든 행에 동일하게 반복하세요.
4. 수량·단가·평중은 단위 없이 숫자만 넣으세요.
5. 포장 방식(VP, IWP 등)은 '비고'에 넣으세요.
6. 값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, "판매_items"

    # ── 기타 (fallback) ───────────────────────────────────────────────────────
    else:
        prompt = f"""
이 이미지는 축산물 커뮤니티의 '{board_type}' 게시글 캡처본입니다.
이미지에서 보이는 모든 정보를 아래 JSON 형식으로 추출하세요.

{{
  "{board_type}_info": {{
    "제목": "...",
    "작성자": "...",
    "작성일": "...",
    "축종": "...",
    "원산지": "...",
    "브랜드": "...",
    "품목": "...",
    "등급": "...",
    "수량_box": "...",
    "업체명": "...",
    "담당자": "...",
    "연락처": "...",
    "비고": "..."
  }}
}}

값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, f"{board_type}_info"


# ─────────────────────────────────────────────
# 유사어 매핑 대상 필드 정의 (board_type별)
# ─────────────────────────────────────────────
FIELD_MAPPING_BY_BOARD: dict[str, dict[str, str]] = {
    "품목표": {
        "축종": "축종",
        "원산지": "원산지",
        "품목": "품목",
        "브랜드": "브랜드",
        "창고": "창고",
        "보관": "보관",
    },
    "구매": {
        "브랜드": "브랜드",
        "품목": "품목",
        "등급": "등급",
        "원산지": "원산지",
        "축종": "축종",
        "보관": "보관",
    },
    "판매": {
        "브랜드": "브랜드",
        "품목": "품목",
        "등급": "등급",
        "원산지": "원산지",
        "축종": "축종",
        "창고": "창고",
        "보관": "보관",
    },
    "등업신청": {
        "주사용창고": "창고",
        "배송가능지역": "배송지역",
    },
    "회원정보": {
        "주사용창고": "창고",
        "배송가능지역": "배송지역",
        "등급": "회원등급",
    },
}


# ─────────────────────────────────────────────
# GUI
# ─────────────────────────────────────────────
class DataReviewGUI:
    def __init__(self, all_data: list, mapper: SynonymMapper):
        self.all_items = all_data
        self.mapper = mapper
        self.current_index = 0

        self.root = tk.Tk()
        self.root.title(f"통합 데이터 검토 및 수정 - 총 {len(all_data)}개 항목")
        self.root.geometry("1000x800")

        self.canvas = None
        self.setup_ui()
        self.load_current_item()

    def setup_ui(self):
        nav_frame = ttk.Frame(self.root)
        nav_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Button(nav_frame, text="◀ 이전", command=self.prev_item).pack(side=tk.LEFT)
        ttk.Button(nav_frame, text="다음 ▶", command=self.next_item).pack(side=tk.LEFT, padx=5)

        self.progress_label = ttk.Label(nav_frame, text="", font=("Arial", 12, "bold"))
        self.progress_label.pack(side=tk.LEFT, padx=20)

        ttk.Button(nav_frame, text="💾 임시 저장", command=self.save_bulk_data).pack(side=tk.RIGHT)
        ttk.Button(nav_frame, text="✅ 모든 분석 완료 및 매핑 업데이트", command=self.finish).pack(side=tk.RIGHT, padx=5)

        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(nav_frame, variable=self.progress_var, maximum=len(self.all_items))
        self.progress_bar.pack(fill=tk.X, padx=10, pady=5)

        self.info_label = ttk.Label(self.root, text="", font=("Arial", 10, "italic"), foreground="gray")
        self.info_label.pack(fill=tk.X, padx=15)

        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.canvas = tk.Canvas(main_frame)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)

        self.scrollable_frame.bind(
            "<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.entry_widgets = {}

    def load_current_item(self):
        if not self.all_items or self.current_index >= len(self.all_items):
            return

        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()

        self.entry_widgets = {}
        entry_obj = self.all_items[self.current_index]
        current_item = entry_obj["data"]
        board_type = entry_obj["board_type"]

        self.progress_label.config(text=f"{self.current_index + 1} / {len(self.all_items)}")
        self.progress_var.set(self.current_index + 1)
        self.info_label.config(text=f"게시판: {board_type} | 날짜: {current_item.get('수집일', 'N/A')}")

        if "파일명" in current_item:
            ttk.Label(
                self.scrollable_frame,
                text=f"📄 {current_item['파일명']}",
                font=("Arial", 12, "bold"),
                foreground="blue",
            ).grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=(0, 10))

        row = 1
        display_keys = [k for k in current_item.keys() if not k.startswith("_") and k not in ["파일명", "수집일"]]

        for key in display_keys:
            value = current_item.get(key, "")
            ttk.Label(self.scrollable_frame, text=f"{key}:", font=("Arial", 10, "bold")).grid(
                row=row, column=0, sticky="nw", padx=5, pady=5
            )
            content = str(value) if value else ""
            lines = max(1, min(6, content.count("\n") + 1, len(content) // 50 + 1))
            text_widget = tk.Text(self.scrollable_frame, height=lines, width=65, wrap=tk.WORD)
            text_widget.grid(row=row, column=1, sticky="ew", padx=5, pady=5)
            text_widget.insert("1.0", content)
            self.entry_widgets[key] = text_widget
            row += 1

        ttk.Label(self.scrollable_frame, text="---", foreground="lightgray").grid(
            row=row, column=0, columnspan=2
        )
        row += 1
        ttk.Label(self.scrollable_frame, text="수집일:", font=("Arial", 9)).grid(row=row, column=0, sticky="e")
        ttk.Label(self.scrollable_frame, text=current_item.get("수집일", ""), foreground="gray").grid(
            row=row, column=1, sticky="w"
        )

        self.scrollable_frame.columnconfigure(1, weight=1)
        self.scrollable_frame.update_idletasks()
        if self.canvas:
            self.canvas.yview_moveto(0)

    def prev_item(self):
        if self.current_index > 0:
            self.save_current_item()
            self.current_index -= 1
            self.load_current_item()

    def next_item(self):
        if self.current_index < len(self.all_items) - 1:
            self.save_current_item()
            self.current_index += 1
            self.load_current_item()

    def save_current_item(self):
        if not self.entry_widgets or self.current_index >= len(self.all_items):
            return
        current_data = self.all_items[self.current_index]["data"]
        for key, widget in self.entry_widgets.items():
            current_data[key] = widget.get("1.0", tk.END).strip()

    def save_bulk_data(self, notify=True):
        self.save_current_item()
        path_to_data = {}
        for entry in self.all_items:
            path = entry["output_path"]
            path_to_data.setdefault(path, [])
            clean_item = {k: v for k, v in entry["data"].items() if not k.startswith("_")}
            path_to_data[path].append(clean_item)

        try:
            for path, data_list in path_to_data.items():
                df = pd.DataFrame(data_list)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                df.to_excel(path, index=False)
                filename = os.path.basename(path)
                os.makedirs(DB_DIR, exist_ok=True)
                df.to_excel(os.path.join(DB_DIR, filename), index=False)
            if notify:
                log(f"💾 {len(path_to_data)}개 파일 일괄 저장 완료.")
                messagebox.showinfo("저장 완료", f"총 {len(path_to_data)}개의 엑셀 파일이 업데이트되었습니다.")
        except Exception as e:
            if notify:
                messagebox.showerror("저장 오류", f"저장 중 오류가 발생했습니다:\n{str(e)}")
            log(f"❌ 저장 오류: {e}")

    def finish(self):
        self.save_current_item()
        learn_fields = list(
            set(f for fm in FIELD_MAPPING_BY_BOARD.values() for f in fm.keys())
        )
        learn_count = 0
        log("🧠 사용자의 수정 사항을 학습 중...")
        for entry in self.all_items:
            data = entry["data"]
            raw_extracts = data.get("_raw_extract", {}) or {}
            for field in learn_fields:
                if field in data and field in raw_extracts:
                    raw_val = str(raw_extracts.get(field, "")).strip()
                    user_val = str(data.get(field, "")).strip()
                    if raw_val and user_val and raw_val != user_val:
                        # 카테고리명은 board_type의 매핑에서 찾음
                        board = entry["board_type"]
                        category = FIELD_MAPPING_BY_BOARD.get(board, {}).get(field, field)
                        if self.mapper.normalize_value(category, raw_val) != user_val:
                            self.mapper.update_mapping(category, raw_val, user_val)
                            learn_count += 1

        self.save_bulk_data(notify=False)
        log(f"🎯 학습 완료: {learn_count}개 매핑 추가/업데이트됨")
        if learn_count > 0:
            messagebox.showinfo(
                "학습 완료",
                f"사용자의 수정 사항 {learn_count}건을 매핑 파일에 반영했습니다.\n다음 분석부터는 이 규칙이 자동으로 적용됩니다.",
            )
        self.root.quit()
        self.root.destroy()

    def show(self):
        self.root.mainloop()


# ─────────────────────────────────────────────
# Core extractor
# ─────────────────────────────────────────────
def _ocr_from_b64(base64_image: str, mime: str, board_type: str):
    """base64 이미지 1장을 board_type 스키마로 구조화 추출."""
    prompt, response_key = _build_prompt(board_type)
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{base64_image}", "detail": "high"}},
            ],
        }
    ]
    try:
        response = call_gpt_with_retry(messages, max_retry=3)
        if not response:
            return {}
        content = response.choices[0].message.content
        data = json.loads(content)
        return data.get(response_key, {})
    except json.JSONDecodeError as e:
        log(f"JSON 파싱 실패: {e}")
        return {}
    except Exception as e:
        log(f"AI analysis error: {e}")
        return {}


def _row_key(r: dict) -> tuple:
    """행 동일성 판단용 키 (타일 경계 중복 제거)."""
    return tuple(str(r.get(k, "")).strip() for k in ("품목", "등급", "EST", "브랜드", "창고", "평중_kg"))


def _merge_product_tiles(tile_lists: list) -> list:
    """
    타일별 추출 결과를 순서대로 병합하되, 겹침(overlap) 구간에서 생긴
    경계 중복만 제거한다. (표 내부의 정상적인 반복 행은 보존)
    """
    merged = []
    for rows in tile_lists:
        if not isinstance(rows, list) or not rows:
            continue
        if not merged:
            merged.extend(rows)
            continue
        tail_keys = [_row_key(r) for r in merged[-12:]]
        skip = 0
        for r in rows:
            if _row_key(r) in tail_keys:
                skip += 1
            else:
                break
        merged.extend(rows[skip:])
    return merged


def _extract_products_tiled(image_path: str) -> list:
    """
    세로로 긴 품목표 이미지를 겹침 밴드로 분할해 각각 OCR 후 병합.
    (한 장 통째 OCR 시 API가 이미지를 축소해 발생하는 환각·행 누락 방지)
    """
    try:
        im = Image.open(image_path)
    except Exception:
        return _ocr_from_b64(encode_image(image_path), get_mime_by_ext(image_path), "품목표")

    w, h = im.size
    band, ov = OCR_TILE_HEIGHT, OCR_TILE_OVERLAP
    bounds = []
    y = 0
    while y < h:
        y2 = min(h, y + band)
        bounds.append((y, y2))
        if y2 >= h:
            break
        y += max(1, band - ov)

    log(f"AI 분석(타일 {len(bounds)}개) [품목표]: {os.path.basename(image_path)} (높이 {h}px)...")
    tile_results = []
    for (y1, y2) in bounds:
        crop = im.crop((0, y1, w, y2))
        buf = BytesIO()
        crop.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        rows = _ocr_from_b64(b64, "image/png", "품목표")
        tile_results.append(rows if isinstance(rows, list) else [])

    return _merge_product_tiles(tile_results)


def extract_data_from_image(image_path: str, board_type: str = "구매") -> dict | list:
    """
    이미지에서 board_type에 맞는 구조화 데이터 추출.
    품목표(행 리스트)이고 이미지가 길면 자동으로 타일 분할 OCR을 수행한다.
    """
    log(f"AI 분석 중 [{board_type}]: {os.path.basename(image_path)}...")

    if board_type == "품목표" and PIL_AVAILABLE:
        try:
            with Image.open(image_path) as _im:
                _tall = _im.height > OCR_TILE_THRESHOLD
        except Exception:
            _tall = False
        if _tall:
            return _extract_products_tiled(image_path)

    return _ocr_from_b64(encode_image(image_path), get_mime_by_ext(image_path), board_type)


def _build_text_prompt(board_type: str) -> tuple[str, str]:
    """
    텍스트 기반 게시판용 프롬프트를 반환합니다.
    (prompt_text, response_key)
    """

    if board_type == "구매":
        prompt = """
아래는 축산물 커뮤니티의 '구매(구합니다)' 게시글 텍스트입니다.
게시글에 여러 품목이 있으면 품목마다 별도 객체로 분리하여 배열로 반환하세요.
각 필드에는 반드시 하나의 값만 넣으세요. 절대 콤마로 여러 값을 합치지 마세요.

{
  "구매_items": [
    {
      "축종": "소/돼지/닭/양 중 하나만",
      "원산지": "호주/미국/국내 등 하나만",
      "브랜드": "브랜드명 하나만 (무관이면 빈 문자열)",
      "품목": "부위 또는 품목명 하나만",
      "등급": "등급 하나만 (없으면 빈 문자열)",
      "수량_box": "숫자만 (단위 제외, 없으면 빈 문자열)",
      "희망단가": "숫자만 (없으면 빈 문자열)",
      "희망납기일": "날짜 (없으면 빈 문자열)",
      "보관": "냉동 또는 냉장 (없으면 빈 문자열)",
      "업체명": "구매자 회사명",
      "담당자": "담당자 이름",
      "연락처": "전화 또는 핸드폰",
      "비고": "기타 요청사항, 스펙, 특이조건"
    }
  ]
}

추출 규칙 (매우 중요):
1. 품목이 여러 개면 품목마다 별도 행으로 분리하세요 (예: 삼겹살 1행, 목심 1행).
2. 각 행의 모든 필드에는 값이 하나만 들어가야 합니다. 콤마(,)로 여러 값을 합치지 마세요.
3. 공통 정보(업체명, 연락처, 담당자 등)는 모든 행에 동일하게 반복하세요.
4. 수량·단가는 단위 없이 숫자만 넣으세요.
5. 값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, "구매_items"

    elif board_type == "판매":
        prompt = """
아래는 축산물 커뮤니티의 '판매(팝니다)' 게시글 텍스트입니다.
게시글에 여러 품목이 있으면 품목마다 별도 객체로 분리하여 배열로 반환하세요.
각 필드에는 반드시 하나의 값만 넣으세요. 절대 콤마로 여러 값을 합치지 마세요.

{
  "판매_items": [
    {
      "축종": "소/돼지/닭/양 중 하나만",
      "원산지": "호주/미국/국내 등 하나만",
      "브랜드": "브랜드명 하나만",
      "품목": "부위 또는 품목명 하나만",
      "등급": "등급 하나만 (없으면 빈 문자열)",
      "EST": "EST 번호 (없으면 빈 문자열)",
      "수량_box": "숫자만 (단위 제외)",
      "판매단가": "숫자만 (없으면 빈 문자열)",
      "평중_kg": "숫자만 (없으면 빈 문자열)",
      "소비기한": "날짜 또는 원문",
      "보관": "냉동 또는 냉장",
      "창고": "보관 창고명",
      "업체명": "판매자 회사명",
      "담당자": "담당자 이름",
      "연락처": "연락처",
      "비고": "포장 방식(VP/IWP 등), 기타 특이사항"
    }
  ]
}

추출 규칙 (매우 중요):
1. 품목이 여러 개면 품목마다 별도 행으로 분리하세요 (예: 삼겹살 1행, 목심 1행).
2. 각 행의 모든 필드에는 값이 하나만 들어가야 합니다. 콤마(,)로 여러 값을 합치지 마세요.
3. 공통 정보(업체명, 연락처, 담당자 등)는 모든 행에 동일하게 반복하세요.
4. 수량·단가·평중은 단위 없이 숫자만 넣으세요.
5. 포장 방식(VP, IWP 등)은 '비고'에 넣으세요.
6. 값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, "판매_items"

    elif board_type == "회원정보":
        prompt = """
아래는 축산물 커뮤니티 '회원정보' 게시글 텍스트입니다.
텍스트에서 다음 정보를 추출하여 JSON 형식으로 반환하세요.

{
  "회원정보_info": {
    "닉네임": "커뮤니티 닉네임",
    "성명": "실명",
    "회사명": "회사 또는 사업체명",
    "직책": "직함",
    "회사전화번호": "대표 전화번호",
    "회사팩스번호": "팩스 번호",
    "핸드폰번호": "휴대폰 번호",
    "설립년월": "회사 설립 연월",
    "직원수": "직원 수 숫자",
    "주력브랜드_품목": "취급 브랜드 및 품목 전체 목록 (원문 그대로)",
    "주사용창고": "주로 사용하는 냉동/냉장 창고명",
    "배송가능지역": "배송 가능 지역",
    "회사주소": "사업장 주소",
    "회사소개": "회사 소개 문구 전체",
    "가입일": "커뮤니티 가입일 (있는 경우)",
    "등급": "현재 회원 등급 (있는 경우)"
  }
}

추출 규칙:
1. 각 필드는 반드시 분리 추출하세요. 절대 한 필드에 합치지 마세요.
2. '주력브랜드_품목'은 원산지별·축종별 브랜드·품목 목록 전체를 줄바꿈 포함 그대로 유지하세요.
3. 값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, "회원정보_info"

    elif board_type == "등업신청":
        prompt = """
아래는 축산물 커뮤니티의 '등업신청' 게시글 텍스트입니다.
텍스트에서 다음 정보를 추출하여 JSON 형식으로 반환하세요.

{
  "등업신청_info": {
    "등업구분": "정회원/우수회원 등 신청 등급",
    "닉네임": "커뮤니티 닉네임",
    "성명": "실명",
    "회사명": "회사 또는 사업체명",
    "직책": "직함 (예: 대표, 부장, 대리 등)",
    "회사전화번호": "대표 전화번호",
    "회사팩스번호": "팩스 번호",
    "핸드폰번호": "휴대폰 번호",
    "설립년월": "회사 설립 연월 (예: 2015-09)",
    "직원수": "직원 수 숫자",
    "주력브랜드_품목": "취급 브랜드 및 품목 전체 목록 (원문 그대로)",
    "주사용창고": "주로 사용하는 냉동/냉장 창고명",
    "배송가능지역": "배송 가능 지역",
    "회사주소": "사업장 주소",
    "회사소개": "회사 소개 문구 전체"
  }
}

추출 규칙:
1. 각 필드는 반드시 분리 추출하세요. 절대 한 필드에 합치지 마세요.
2. '주력브랜드_품목'은 원산지별·축종별 브랜드·품목 목록 전체를 줄바꿈 포함 그대로 유지하세요.
3. 값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, "등업신청_info"

    else:
        prompt = f"""
아래는 축산물 커뮤니티의 '{board_type}' 게시글 텍스트입니다.
텍스트에서 모든 정보를 아래 JSON 형식으로 추출하세요.

{{
  "{board_type}_info": {{
    "축종": "...",
    "원산지": "...",
    "브랜드": "...",
    "품목": "...",
    "등급": "...",
    "수량_box": "...",
    "업체명": "...",
    "담당자": "...",
    "연락처": "...",
    "비고": "..."
  }}
}}

값이 없는 필드는 빈 문자열("")로 두세요.
"""
        return prompt, f"{board_type}_info"


def extract_data_from_text(title: str, content: str, board_type: str, author: str = "", date_str: str = "") -> dict | list:
    """텍스트 게시글에서 board_type에 맞는 구조화된 데이터 추출. 구매/판매는 list 반환."""
    log(f"AI 텍스트 분석 중 [{board_type}]: {title[:30]}...")

    prompt_template, response_key = _build_text_prompt(board_type)

    user_text = f"""{prompt_template}

--- 게시글 정보 ---
제목: {title}
작성자: {author}
작성일: {date_str}

--- 본문 ---
{content}
"""

    messages = [
        {
            "role": "user",
            "content": user_text,
        }
    ]

    try:
        response = call_gpt_with_retry(messages, max_retry=3)
        if not response:
            return []

        resp_content = response.choices[0].message.content
        data = json.loads(resp_content)
        result = data.get(response_key, [])

        # 제목/작성자/작성일 보충
        def _fill_common(item: dict):
            if "제목" not in item or not item["제목"]:
                item["제목"] = title
            if "작성자" not in item or not item["작성자"]:
                item["작성자"] = author
            if "작성일" not in item or not item["작성일"]:
                item["작성일"] = date_str

        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict):
                    _fill_common(item)
            return result
        elif isinstance(result, dict):
            _fill_common(result)
            return [result]
        return []
    except json.JSONDecodeError as e:
        log(f"JSON 파싱 실패: {e}")
        return []
    except Exception as e:
        log(f"AI 텍스트 분석 오류: {e}")
        return []


# ─────────────────────────────────────────────
# Synonym mapping
# ─────────────────────────────────────────────
def apply_synonym_mapping(data: list, mapper: SynonymMapper, board_type: str) -> list:
    """유사어 매핑 적용 및 학습용 원본값 보존"""
    log("🔄 유사어 변환 적용 중...")
    field_mapping = FIELD_MAPPING_BY_BOARD.get(board_type, {})

    processed_data = []
    conversion_count = 0

    for item in data:
        processed_item = item.copy()
        raw_extracts = {}

        for field, category in field_mapping.items():
            if field in processed_item:
                original_value = str(processed_item.get(field, "")).strip()
                raw_extracts[field] = original_value
                normalized_value = mapper.normalize_value(category, original_value)
                processed_item[field] = normalized_value
                if original_value != normalized_value:
                    conversion_count += 1

        processed_item["_raw_extract"] = raw_extracts
        processed_data.append(processed_item)

    log(f"🎯 총 {conversion_count}개 항목 변환 완료")
    return processed_data


# ─────────────────────────────────────────────
# Processor
# ─────────────────────────────────────────────
def run_enhanced_processor(
    target_date=None,
    mapper=None,
    hooks: dict | None = None,
    target_boards=None,
) -> list:
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    daily_dir = os.path.join(BASE_DIR, "visionmeat", target_date)
    if not os.path.exists(daily_dir):
        return []

    hooks = hooks or {}
    on_step = hooks.get("on_step")
    check_pause_stop = hooks.get("check_pause_stop")

    collected_board_data = []
    _all_boards = ["구매", "판매", "품목표", "회원정보", "등업신청"]
    active_boards = [b for b in _all_boards if b in target_boards] if target_boards else _all_boards

    for board in active_boards:
        if check_pause_stop:
            check_pause_stop()

        # 텍스트 게시판: text_data.json → GPT 구조화 파싱
        if board in TEXT_BOARDS:
            board_dir = os.path.join(daily_dir, board)
            text_json = os.path.join(board_dir, "text_data", "text_data.json")
            excel_dir = os.path.join(board_dir, "excel")

            if not os.path.exists(text_json):
                log(f"[{target_date}] [{board}] text_data.json 없음. 건너뜁니다.")
                continue

            # 이미 구조화된 Excel이 있으면 건너뜀
            output_filename = f"{target_date.replace('-', '')}_{board}_데이터.xlsx"
            if os.path.exists(os.path.join(excel_dir, output_filename)):
                log(f"[{target_date}] [{board}] 이미 처리됨. 건너뜁니다.")
                continue

            os.makedirs(excel_dir, exist_ok=True)

            with open(text_json, "r", encoding="utf-8") as f:
                text_posts = json.load(f)

            if not text_posts:
                log(f"[{target_date}] [{board}] 텍스트 데이터 0건. 건너뜁니다.")
                continue

            log(f"[{target_date}] [{board}] 텍스트 구조화 분석 시작... ({len(text_posts)}건)")

            if on_step:
                on_step("ocr_start_board", {"date": target_date, "board": board, "total_images": len(text_posts)})

            extracted_items = []
            max_workers = int(os.getenv("OCR_WORKERS", "5"))

            def _text_worker(post, _board=board, _on=on_step, _d=target_date, _t=len(text_posts)):
                title = post.get("제목", "")
                content = post.get("내용", "")
                author = post.get("작성자", "")
                date_s = post.get("작성일", "")
                if _on:
                    _on("ocr_image_start", {"date": _d, "board": _board, "filename": title[:30], "total": _t})
                return title, extract_data_from_text(title, content, _board, author=author, date_str=date_s)

            completed = 0
            total_posts = len(text_posts)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(_text_worker, post): post for post in text_posts}
                for fut in as_completed(futures):
                    if check_pause_stop:
                        check_pause_stop()
                    completed += 1
                    post = futures[fut]
                    log(f"[{target_date}] [{board}] {completed}/{total_posts} 완료 ({post.get('제목', '')[:30]})")

                    if on_step:
                        on_step("ocr_image_done", {
                            "date": target_date, "board": board,
                            "filename": post.get("제목", "")[:30], "completed": completed, "total": total_posts,
                        })

                    try:
                        title, info = fut.result()
                        if info:
                            items = info if isinstance(info, list) else [info]
                            for item in items:
                                if isinstance(item, dict):
                                    item["수집일"] = target_date
                                    extracted_items.append(item)
                    except Exception as e:
                        log(f"텍스트 분석 오류 ({post.get('제목', '')}): {e}")

            if extracted_items:
                normalized = apply_synonym_mapping(extracted_items, mapper, board)
                output_path = os.path.join(excel_dir, output_filename)
                for item in normalized:
                    collected_board_data.append({"data": item, "board_type": board, "output_path": output_path})

            continue

        board_dir = os.path.join(daily_dir, board)
        capture_dir = os.path.join(board_dir, "screenshots")
        excel_dir = os.path.join(board_dir, "excel")

        if not os.path.exists(capture_dir):
            continue

        os.makedirs(excel_dir, exist_ok=True)

        png_files = sorted([f for f in os.listdir(capture_dir) if f.lower().endswith(".png")])
        jpg_files = sorted([f for f in os.listdir(capture_dir) if f.lower().endswith((".jpg", ".jpeg"))])
        img_files = png_files + jpg_files

        if not img_files:
            continue

        max_workers = int(os.getenv("OCR_WORKERS", "5"))
        log(f"🤖 [{target_date}] [{board}] AI 분석 시작... ({len(img_files)}개 / 병렬 {max_workers}개)")

        if on_step:
            on_step("ocr_start_board", {"date": target_date, "board": board, "total_images": len(img_files)})

        extracted_items = []
        completed = 0
        total_imgs = len(img_files)

        def _ocr_worker(fn, _cap=capture_dir, _board=board, _on=on_step, _d=target_date, _t=total_imgs):
            if _on:
                _on("ocr_image_start", {"date": _d, "board": _board, "filename": fn, "total": _t})
            path = os.path.join(_cap, fn)
            return fn, extract_data_from_image(path, board_type=_board)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_ocr_worker, fn): fn for fn in img_files}

            for fut in as_completed(futures):
                if check_pause_stop:
                    check_pause_stop()

                completed += 1
                fn = futures[fut]
                log(f"📊 [{target_date}] [{board}] {completed}/{total_imgs} 완료 ({fn})")

                if on_step:
                    on_step("ocr_image_done", {
                        "date": target_date, "board": board,
                        "filename": fn, "completed": completed, "total": total_imgs,
                    })

                try:
                    _, info = fut.result()
                    if info:
                        if isinstance(info, list):
                            for item in info:
                                item["파일명"] = fn
                                item["수집일"] = target_date
                                extracted_items.append(item)
                        elif isinstance(info, dict):
                            info["파일명"] = fn
                            info["수집일"] = target_date
                            extracted_items.append(info)
                except Exception as e:
                    log(f"❌ 분석 오류 ({fn}): {e}")

        if extracted_items:
            normalized = apply_synonym_mapping(extracted_items, mapper, board)
            output_filename = f"{target_date.replace('-', '')}_{board}_데이터.xlsx"
            output_path = os.path.join(excel_dir, output_filename)
            for item in normalized:
                collected_board_data.append({"data": item, "board_type": board, "output_path": output_path})

    return collected_board_data


def run_enhanced_batch_all(
    show_gui=True,
    force_reprocess=False,
    target_dates=None,
    hooks: dict | None = None,
    target_boards=None,
):
    vision_meat_root = os.path.join(BASE_DIR, "visionmeat")
    if not os.path.isdir(vision_meat_root):
        log("❌ visionmeat 폴더가 존재하지 않습니다.")
        return

    hooks = hooks or {}
    on_step = hooks.get("on_step")
    check_pause_stop = hooks.get("check_pause_stop")

    mapper = SynonymMapper(MAPPING_FILE, source_file=SOURCE_MAPPING_FILE)

    _all_boards = ["구매", "판매", "품목표", "회원정보", "등업신청"]
    active_boards = [b for b in _all_boards if b in target_boards] if target_boards else _all_boards
    log(f"대상 게시판: {active_boards}")

    pending_dates = []
    today_dt = datetime.now()

    if target_dates is not None:
        pending_dates = list(target_dates)
    else:
        for entry in sorted(os.listdir(vision_meat_root)):
            entry_path = os.path.join(vision_meat_root, entry)
            if not os.path.isdir(entry_path):
                continue
            try:
                folder_date = datetime.strptime(entry, "%Y-%m-%d")
                if not force_reprocess and (today_dt - folder_date).days > 7:
                    continue
            except Exception:
                continue

            needs_processing = False
            for board in active_boards:
                e_dir = os.path.join(entry_path, board, "excel")
                output_filename = f"{entry.replace('-', '')}_{board}_데이터.xlsx"

                if board in TEXT_BOARDS:
                    # 텍스트 게시판: text_data.json이 있지만 구조화 Excel이 없으면 처리 필요
                    text_json = os.path.join(entry_path, board, "text_data", "text_data.json")
                    if not os.path.exists(text_json):
                        continue
                    if force_reprocess:
                        needs_processing = True
                        break
                    if not os.path.exists(os.path.join(e_dir, output_filename)):
                        needs_processing = True
                        break
                    continue

                s_dir = os.path.join(entry_path, board, "screenshots")
                if not os.path.isdir(s_dir):
                    continue
                has_imgs = any(
                    f.lower().endswith((".png", ".jpg", ".jpeg")) for f in os.listdir(s_dir)
                )
                if not has_imgs:
                    continue
                if force_reprocess:
                    needs_processing = True
                    break
                if not os.path.exists(os.path.join(e_dir, output_filename)):
                    needs_processing = True
                    break

            if needs_processing:
                pending_dates.append(entry)

    if not pending_dates:
        log("✅ 처리할 날짜가 없습니다.")
        return

    global_all_data = []

    for idx, date_str in enumerate(pending_dates, 1):
        if check_pause_stop:
            check_pause_stop()

        log(f"\n🚀 [{date_str}] 분석 프로세스 시작... ({idx}/{len(pending_dates)})")
        if on_step:
            on_step("ocr_date_start", {"date": date_str, "index": idx, "total_dates": len(pending_dates)})

        board_data_list = run_enhanced_processor(
            target_date=date_str, mapper=mapper, hooks=hooks, target_boards=target_boards
        )
        global_all_data.extend(board_data_list)

        if not show_gui and board_data_list:
            path_to_data = {}
            for entry in board_data_list:
                path = entry["output_path"]
                path_to_data.setdefault(path, [])
                clean_item = {k: v for k, v in entry["data"].items() if not k.startswith("_")}
                path_to_data[path].append(clean_item)
            for path, data_list in path_to_data.items():
                df = pd.DataFrame(data_list)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                df.to_excel(path, index=False)
                filename = os.path.basename(path)
                os.makedirs(DB_DIR, exist_ok=True)
                df.to_excel(os.path.join(DB_DIR, filename), index=False)
            log(f"💾 [{date_str}] Excel 저장 완료")

        if on_step:
            on_step("ocr_date_done", {"date": date_str, "index": idx, "total_dates": len(pending_dates)})

    if not global_all_data:
        log("⚠️ 추출된 데이터가 없습니다.")
        return

    if show_gui:
        log(f"\n🖥️ 모든 분석 완료. 통합 GUI 검토를 시작합니다... (총 {len(global_all_data)}개 항목)")
        DataReviewGUI(global_all_data, mapper).show()

    log("\n🎉 분석 및 처리가 모두 완료되었습니다!")


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("실행 모드를 선택하세요:")
    print("1. 🖥️  신규 데이터 검토 (GUI) - 미처리된 날짜만 검토하며 수정")
    print("2. ⚡ 신규 데이터 자동 저장 - 미처리된 날짜를 변환 후 바로 저장")
    print("3. 🔄 모든 과거 데이터 재처리 (GUI) - 이미 완료된 날짜도 다시 검토")
    print("=" * 50)
    choice = input("선택 (1, 2, 3): ").strip()
    if choice == "3":
        run_enhanced_batch_all(show_gui=True, force_reprocess=True)
    else:
        run_enhanced_batch_all(show_gui=(choice != "2"), force_reprocess=False)
