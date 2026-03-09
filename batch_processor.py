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

# Optional (cost saving): image downscale before sending to GPT
try:
    from PIL import Image
    from io import BytesIO
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False

load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VISION_MODEL = os.getenv("OPENAI_VISION_MODEL", "gpt-4.1")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "visionmeat", "database")
MAPPING_FILE = os.path.join(BASE_DIR, "ref", "유사어_매핑.xlsx")

# Optional switches
# - If "1" : downscale images to reduce tokens/cost (requires pillow)
RESIZE_IMAGES = os.getenv("RESIZE_IMAGES", "0").strip() == "1"
MAX_IMG_SIZE = int(os.getenv("MAX_IMG_SIZE", "1280"))

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

def get_mime_by_ext(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".png":
        return "image/png"
    if ext in [".jpg", ".jpeg"]:
        return "image/jpeg"
    # default
    return "image/png"

def encode_image(image_path: str) -> str:
    """
    Base64 encode image.
    If RESIZE_IMAGES=1 and pillow is available, downscale to reduce tokens/cost.
    """
    if RESIZE_IMAGES and PIL_AVAILABLE:
        try:
            img = Image.open(image_path)
            img.thumbnail((MAX_IMG_SIZE, MAX_IMG_SIZE))
            buf = BytesIO()

            # keep format consistent with file extension if possible
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
    """
    Simple retry wrapper for OpenAI call with linear backoff.
    """
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

class SynonymMapper:
    def __init__(self, mapping_file: str):
        self.mapping_dict = {}
        self.load_mapping_file(mapping_file)

    def load_mapping_file(self, file_path: str):
        """유사어 매핑 파일을 로드"""
        try:
            if os.path.exists(file_path):
                df = pd.read_excel(file_path)

                # defensive
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
        """값을 표준값으로 변환"""
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
        """새로운 매핑을 추가하거나 업데이트하고 파일로 저장"""
        if not category or not original or not standard:
            return

        original_lower = original.strip().lower()
        standard_trimmed = standard.strip()

        # 1) Memory update
        if category not in self.mapping_dict:
            self.mapping_dict[category] = {}

        if self.mapping_dict[category].get(original_lower) == standard_trimmed:
            return

        self.mapping_dict[category][original_lower] = standard_trimmed
        log(f"🆕 새 매핑 등록: [{category}] {original} -> {standard_trimmed}")

        # 2) File update
        try:
            file_path = MAPPING_FILE
            if os.path.exists(file_path):
                df = pd.read_excel(file_path)
            else:
                df = pd.DataFrame(columns=["분류", "원본값", "표준값"])

            # defensive: NaN guard
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

class DataReviewGUI:
    def __init__(self, all_data: list, mapper: SynonymMapper):
        """
        all_data: [{'data': item_dict, 'board_type': str, 'output_path': str}, ...]
        """
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

        self.scrollable_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
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
            title_label = ttk.Label(
                self.scrollable_frame,
                text=f"📄 {current_item['파일명']}",
                font=("Arial", 12, "bold"),
                foreground="blue",
            )
            title_label.grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=(0, 10))

        row = 1
        display_keys = [k for k in current_item.keys() if not k.startswith("_") and k not in ["파일명", "수집일"]]

        for key in display_keys:
            value = current_item.get(key, "")
            ttk.Label(self.scrollable_frame, text=f"{key}:", font=("Arial", 10, "bold")).grid(
                row=row, column=0, sticky="nw", padx=5, pady=5
            )

            content = str(value) if value else ""
            lines = max(1, min(5, content.count("\n") + 1, len(content) // 50 + 1))

            text_widget = tk.Text(self.scrollable_frame, height=lines, width=65, wrap=tk.WORD)
            text_widget.grid(row=row, column=1, sticky="ew", padx=5, pady=5)
            text_widget.insert("1.0", content)

            self.entry_widgets[key] = text_widget
            row += 1

        ttk.Label(self.scrollable_frame, text="---", foreground="lightgray").grid(row=row, column=0, columnspan=2)
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
            value = widget.get("1.0", tk.END).strip()
            current_data[key] = value

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
                db_path = os.path.join(DB_DIR, filename)
                df.to_excel(db_path, index=False)

            if notify:
                log(f"💾 {len(path_to_data)}개 파일 일괄 저장 완료.")
                messagebox.showinfo("저장 완료", f"총 {len(path_to_data)}개의 엑셀 파일이 업데이트되었습니다.")
        except Exception as e:
            if notify:
                messagebox.showerror("저장 오류", f"저장 중 오류가 발생했습니다:\n{str(e)}")
            log(f"❌ 저장 오류: {e}")

    def finish(self):
        self.save_current_item()

        categories_to_learn = ["브랜드", "품목", "축종", "원산지", "창고", "보관", "등급"]
        learn_count = 0

        log("🧠 사용자의 수정 사항을 학습 중...")
        for entry in self.all_items:
            data = entry["data"]
            raw_extracts = data.get("_raw_extract", {}) or {}

            for field in categories_to_learn:
                if field in data and field in raw_extracts:
                    raw_val = str(raw_extracts.get(field, "")).strip()
                    user_val = str(data.get(field, "")).strip()

                    if raw_val and user_val and raw_val != user_val:
                        if self.mapper.normalize_value(field, raw_val) != user_val:
                            self.mapper.update_mapping(field, raw_val, user_val)
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

def extract_data_from_image(image_path, board_type="구매"):
    """이미지에서 데이터 추출"""
    log(f"AI 분석 중 [{board_type}]: {os.path.basename(image_path)}...")
    base64_image = encode_image(image_path)
    mime = get_mime_by_ext(image_path)

    if board_type == "품목표":
        prompt = """
이 이미지는 축산물 품목표입니다. 이미지에서 다음 정보를 추출하여 JSON 객체 형태로 반환해주세요.
루트 키는 "products"로 하고, 그 안에 각 품목 정보를 배열로 넣어주세요.

추출할 필드:
축종, 원산지, 보관, 품목, 브랜드, 등급, EST, 평중(kg), 스펙/설명, 재고(box), 창고, 소비기한, 판매가(원), 수정일, 비고

주의사항:
1. 원산지, 브랜드 등 병합된 셀의 값은 모든 행에 반복해서 채우세요.
2. '포장'(VP, IWP 등) 정보는 '비고'에 넣으세요.
3. 품명은 '품목'에, 브랜드명은 '브랜드'에 넣으세요.
"""
        response_key = "products"
    elif board_type in ("회원정보", "등업신청"):
        prompt = f"""
이 이미지는 '{board_type}' 게시판의 게시글 캡처본입니다. 이미지에서 보이는 모든 정보를 추출하여 JSON 객체 형태로 반환해주세요.

JSON 형식:
{{
  "{board_type}_info": {{
    "제목": "...",
    "작성자": "...",
    "작성일": "...",
    "내용": "...",
    "비고": "..."
  }}
}}

추출 지침:
1. 제목, 작성자, 작성일 등 기본 정보를 우선 추출하세요.
2. 본문 내용 전체를 '내용' 필드에 넣으세요.
3. 기타 추가 정보(첨부파일, 연락처 등)는 '비고'에 넣으세요.
"""
        response_key = f"{board_type}_info"
    else:
        prompt = f"""
이 이미지는 축산물 {board_type} 게시판의 게시글 캡처본입니다. 이미지에서 다음 정보를 추출하여 JSON 객체 형태로 반환해주세요.

JSON 형식:
{{
  "{board_type}_info": {{
    "브랜드": "...",
    "품목": "...",
    "등급": "...",
    "수량": "...",
    "업체명": "...",
    "담당자": "...",
    "연락처": "...",
    "비고": "..."
  }}
}}

추출 지침:
1. 브랜드, 품목, 등급, 수량, 업체명, 담당자, 연락처 정보를 우선적으로 추출하세요.
2. 등급 정보가 본문에 있다면 '등급' 필드에 넣고, 없으면 빈칸으로 두세요.
3. 기타 정보(단가, 희망일 등)는 '비고'에 콤마(,)로 구분하여 넣으세요.
"""
        response_key = f"{board_type}_info"

    try:
        messages = [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{base64_image}"}},
            ],
        }]

        response = call_gpt_with_retry(messages, max_retry=3)
        if not response:
            return {}

        try:
            content = response.choices[0].message.content
            data = json.loads(content)
        except Exception as e:
            log(f"JSON 파싱 실패: {e}")
            return {}

        return data.get(response_key, {})
    except Exception as e:
        log(f"AI analysis error: {e}")
        return {}

def apply_synonym_mapping(data: list, mapper: SynonymMapper, board_type: str) -> list:
    """유사어 매핑 적용 및 학습용 원본값 보존"""
    log("🔄 유사어 변환 적용 중...")

    if board_type == "품목표":
        field_mapping = {"축종": "축종", "원산지": "원산지", "품목": "품목", "브랜드": "브랜드", "창고": "창고", "보관": "보관"}
    else:
        field_mapping = {"브랜드": "브랜드", "품목": "품목", "등급": "등급", "업체명": "업체명"}

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

def run_enhanced_processor(target_date=None, mapper=None, hooks: dict | None = None, target_boards=None) -> list:
    """분석만 수행하고 통합 GUI용 데이터 리스트를 반환합니다.

    hooks (옵션):
      - hooks.get("on_step"): 진행 상황 보고 콜백
          on_step(phase: str, info: dict)
      - hooks.get("check_pause_stop"): 일시정지/중단 제어 콜백
          check_pause_stop()
    - target_boards: ["구매","판매","품목표"] 중 선택. None이면 전체.
    """
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
        board_dir = os.path.join(daily_dir, board)
        capture_dir = os.path.join(board_dir, "screenshots")
        excel_dir = os.path.join(board_dir, "excel")

        if not os.path.exists(capture_dir):
            continue

        os.makedirs(excel_dir, exist_ok=True)

        png_files = sorted([f for f in os.listdir(capture_dir) if f.lower().endswith(".png")])
        jpg_files = sorted([f for f in os.listdir(capture_dir) if f.lower().endswith(".jpg") or f.lower().endswith(".jpeg")])
        img_files = png_files + jpg_files

        if not img_files:
            continue

        # 병렬 워커 수: 환경변수 OCR_WORKERS (기본 5)
        max_workers = int(os.getenv("OCR_WORKERS", "5"))

        log(f"🤖 [{target_date}] [{board}] AI 분석 시작... ({len(img_files)}개 / 병렬 {max_workers}개)")
        if on_step:
            on_step(
                phase="ocr_start_board",
                info={
                    "date": target_date,
                    "board": board,
                    "total_images": len(img_files),
                },
            )
        extracted_items = []
        completed = 0
        total_imgs = len(img_files)

        def _ocr_worker(fn, _cap=capture_dir, _board=board, _on=on_step, _d=target_date, _t=total_imgs):
            """단일 이미지 OCR 작업 (스레드풀에서 실행). 시작 시점을 콜백으로 보고."""
            if _on:
                _on("ocr_image_start", {"date": _d, "board": _board, "filename": fn, "total": _t})
            path = os.path.join(_cap, fn)
            return fn, extract_data_from_image(path, board_type=_board)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_ocr_worker, fn): fn for fn in img_files}

            for fut in as_completed(futures):
                # 결과 수집 사이에 일시정지/중단 신호 확인
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

def run_enhanced_batch_all(show_gui=True, force_reprocess=False, target_dates=None, hooks: dict | None = None, target_boards=None):
    """모든 날짜 분석 후 마지막에 단 한 번 GUI 호출

    - target_dates: 특정 날짜(문자열 'YYYY-MM-DD' 리스트)만 처리하고 싶을 때 사용.
    - target_boards: ["구매","판매","품목표"] 중 선택. None이면 전체.
    - hooks: Job Manager에서 전달하는 진행률/중단 제어 콜백 딕셔너리.
    """
    vision_meat_root = os.path.join(BASE_DIR, "visionmeat")
    if not os.path.isdir(vision_meat_root):
        log("❌ visionmeat 폴더가 존재하지 않습니다.")
        return

    hooks = hooks or {}
    on_step = hooks.get("on_step")
    check_pause_stop = hooks.get("check_pause_stop")

    mapper = SynonymMapper(MAPPING_FILE)

    _all_boards = ["구매", "판매", "품목표", "회원정보", "등업신청"]
    active_boards = [b for b in _all_boards if b in target_boards] if target_boards else _all_boards
    log(f"대상 게시판: {active_boards}")
    pending_dates = []
    today_dt = datetime.now()

    if target_dates is not None:
        # 외부에서 이미 날짜 리스트를 정해준 경우 그대로 사용
        pending_dates = list(target_dates)
    else:
        for entry in sorted(os.listdir(vision_meat_root)):
            entry_path = os.path.join(vision_meat_root, entry)
            if not os.path.isdir(entry_path):
                continue
            try:
                folder_date = datetime.strptime(entry, "%Y-%m-%d")
                # ✅ 너무 먼 과거(7일 이전)는 미처리 목록에서 제외 (성능 최적화)
                if not force_reprocess and (today_dt - folder_date).days > 7:
                    continue
            except Exception:
                continue

            needs_processing = False
            for board in active_boards:
                s_dir = os.path.join(entry_path, board, "screenshots")
                e_dir = os.path.join(entry_path, board, "excel")

                if not os.path.isdir(s_dir):
                    continue

                # screenshots 존재 여부 체크
                has_imgs = any(
                    f.lower().endswith((".png", ".jpg", ".jpeg")) for f in os.listdir(s_dir)
                )
                if not has_imgs:
                    continue

                if force_reprocess:
                    needs_processing = True
                    break

                # ✅ "excel 폴더 존재"가 아니라, "기대 결과 파일 존재"로 판단
                output_filename = f"{entry.replace('-', '')}_{board}_데이터.xlsx"
                expected_file = os.path.join(e_dir, output_filename)
                if not os.path.exists(expected_file):
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
            on_step(
                phase="ocr_date_start",
                info={
                    "date": date_str,
                    "index": idx,
                    "total_dates": len(pending_dates),
                },
            )

        board_data_list = run_enhanced_processor(target_date=date_str, mapper=mapper, hooks=hooks, target_boards=target_boards)
        global_all_data.extend(board_data_list)

        # ✅ show_gui=False 모드: 날짜별로 즉시 저장 (현황 테이블 실시간 반영)
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
            on_step("ocr_date_done", {
                "date": date_str,
                "index": idx,
                "total_dates": len(pending_dates),
            })

    if not global_all_data:
        log("⚠️ 추출된 데이터가 없습니다.")
        return

    if show_gui:
        log(f"\n🖥️ 모든 분석 완료. 통합 GUI 검토를 시작합니다... (총 {len(global_all_data)}개 항목)")
        review_gui = DataReviewGUI(global_all_data, mapper)
        review_gui.show()
    else:
        # 날짜별 즉시 저장이 완료됐으므로 추가 저장 불필요
        pass

    log("\n🎉 분석 및 처리가 모두 완료되었습니다!")

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
        show_gui = choice != "2"
        run_enhanced_batch_all(show_gui=show_gui, force_reprocess=False)