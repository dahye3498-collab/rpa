"""
job_manager.py – Thread-safe singleton that:
  • Runs the RPA + OCR pipeline in a background daemon thread.
  • Broadcasts SSE events to ALL connected web clients via per-client queues.
  • Supports pause / stop signals through threading.Event.
  • Provides smart resume state by scanning the filesystem.
"""

import queue
import threading
import time
import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
from datetime import datetime

import rpa_automation
import batch_processor


class StopSignal(RuntimeError):
    pass


@dataclass
class ProgressState:
    phase: str = "idle"
    message: str = ""
    current_date: Optional[str] = None
    current_board: Optional[str] = None
    current_index: int = 0
    total_in_scope: int = 0
    total_dates: int = 0
    current_date_index: int = 0
    pct: float = 0.0           # 전체 진행률 0-100
    # 이미지별 상세 진행
    img_current: int = 0       # 완료된 이미지 수
    img_total: int = 0         # 현재 게시판 전체 이미지 수
    img_active: List[str] = None   # 현재 분석 중인 파일명 목록

    def __post_init__(self):
        if self.img_active is None:
            self.img_active = []


class JobManager:
    """전역 싱글톤 – 모든 웹 클라이언트가 같은 인스턴스를 공유"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None

        # pause_event: set=일시정지중, clear=실행중
        self._pause_event = threading.Event()
        # stop_event: set=중단요청
        self._stop_event = threading.Event()

        self.state: str = "IDLE"   # IDLE | RUNNING | PAUSED | STOPPING
        self.job_mode: str = ""    # full | rpa_only | batch_only
        self.progress: ProgressState = ProgressState()
        self.logs: List[str] = []

        # SSE 클라이언트별 큐
        self._sse_queues: List[queue.Queue] = []
        self._sse_lock = threading.Lock()

    # ── SSE 구독 관리 ────────────────────────────────────────
    def subscribe(self) -> "queue.Queue[str]":
        q: queue.Queue = queue.Queue(maxsize=500)
        with self._sse_lock:
            self._sse_queues.append(q)
        return q

    def unsubscribe(self, q: "queue.Queue[str]") -> None:
        with self._sse_lock:
            try:
                self._sse_queues.remove(q)
            except ValueError:
                pass

    def _broadcast(self, data: Dict[str, Any]) -> None:
        msg = json.dumps(data, ensure_ascii=False)
        with self._sse_lock:
            dead = []
            for q in self._sse_queues:
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    dead.append(q)
            for q in dead:
                try:
                    self._sse_queues.remove(q)
                except ValueError:
                    pass

    def get_snapshot(self) -> Dict[str, Any]:
        """신규 접속 클라이언트에게 전송할 전체 상태 스냅샷"""
        with self._lock:
            return {
                "type": "snapshot",
                "state": self.state,
                "job_mode": self.job_mode,
                "progress": asdict(self.progress),
                "logs": self.logs[-100:],
            }

    # ── 로깅 ─────────────────────────────────────────────────
    def _log(self, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        with self._lock:
            self.logs.append(line)
            if len(self.logs) > 500:
                self.logs = self.logs[-500:]
        self._broadcast({"type": "log", "line": line})

    # ── 내부 상태 변경 ────────────────────────────────────────
    def _set_state(self, new_state: str) -> None:
        with self._lock:
            self.state = new_state
        self._broadcast({"type": "state", "state": new_state})

    def _set_progress(self, **kwargs: Any) -> None:
        with self._lock:
            for k, v in kwargs.items():
                setattr(self.progress, k, v)
            snap = asdict(self.progress)
        self._broadcast({"type": "progress", "progress": snap})

    # ── 외부 제어 메서드 ──────────────────────────────────────
    def start(self, mode: str = "full", date_list=None, target_boards=None, credentials=None) -> bool:
        """
        작업 시작. 이미 실행/일시정지 상태면 False 반환.
        mode: "full" | "rpa_only" | "batch_only"
        date_list: ['YYYY-MM-DD', ...] 지정 시 해당 날짜만 처리. None이면 자동 감지.
        target_boards: ["구매","판매","품목표"] 중 선택. None이면 전체.
        credentials: {"login_email": "...", "login_pwd": "..."} 카카오 로그인 정보.
        """
        with self._lock:
            if self.state not in ("IDLE",):
                return False
            self._pause_event.clear()
            self._stop_event.clear()
            self.progress = ProgressState()
            self.state = "RUNNING"
            self.job_mode = mode

        self._thread = threading.Thread(
            target=self._run_pipeline,
            args=(mode, date_list, target_boards, credentials),
            daemon=True,
            name="pipeline-worker",
        )
        self._thread.start()
        self._broadcast({"type": "state", "state": "RUNNING", "job_mode": mode})
        date_hint = f" | 지정 날짜 {len(date_list)}개" if date_list else " | 자동 감지"
        board_hint = f" | 게시판: {','.join(target_boards)}" if target_boards else ""
        self._log(f"작업 시작 (mode={mode}{date_hint}{board_hint})")
        return True

    def pause(self) -> None:
        if self.state == "RUNNING":
            self._pause_event.set()
            self._set_state("PAUSED")
            self._log("⏸ 작업 일시정지됨")

    def resume(self) -> None:
        if self.state == "PAUSED":
            self._pause_event.clear()
            self._set_state("RUNNING")
            self._log("▶ 작업 재개됨")

    def stop(self) -> None:
        if self.state in ("RUNNING", "PAUSED"):
            self._stop_event.set()
            self._pause_event.clear()   # 일시정지 중이면 해제
            self._set_state("STOPPING")
            self._log("⏹ 중단 신호 전송 - 현재 작업 완료 후 중단됩니다...")

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "state": self.state,
                "job_mode": self.job_mode,
                "progress": asdict(self.progress),
            }

    def get_logs(self, limit: int = 200) -> List[str]:
        with self._lock:
            return self.logs[-limit:]

    # ── 파이프라인 훅 ─────────────────────────────────────────
    def _check_pause_stop(self) -> None:
        """RPA/OCR 워커 스레드가 주기적으로 호출하는 제어 콜백."""
        if self._stop_event.is_set():
            raise StopSignal("STOP_REQUESTED")
        # 일시정지 상태면 여기서 블록
        while self._pause_event.is_set() and not self._stop_event.is_set():
            time.sleep(0.2)
        if self._stop_event.is_set():
            raise StopSignal("STOP_REQUESTED")

    def _on_step(self, phase: str, info: Dict[str, Any]) -> None:
        """RPA/OCR 진행 상황 콜백."""
        msg = ""
        kwargs: Dict[str, Any] = {}

        if phase == "capture_date_start":
            idx   = info.get("index", 0)
            total = max(info.get("total", 1), 1)
            pct   = round((idx - 1) / total * 100, 1)
            msg   = f"[캡처] 날짜 {idx}/{total} – {info.get('date')} 수집 시작"
            kwargs = dict(
                phase="capturing",
                current_date=info.get("date"),
                current_board=None,
                current_date_index=idx,
                total_dates=total,
                pct=pct,
                message=msg,
            )

        elif phase == "capture_board_start":
            di = info.get("date_index", 0)
            td = max(info.get("total_dates", 1), 1)
            bi = info.get("board_index", 0)
            tb = max(info.get("total_boards", 1), 1)
            pct = round(((di - 1) + (bi - 1) / tb) / td * 100, 1)
            msg = (
                f"[캡처] 날짜 {di}/{td} | {info.get('board')} 게시판 {bi}/{tb} 수집 중"
            )
            kwargs = dict(
                phase="capturing",
                current_date=info.get("date"),
                current_board=info.get("board"),
                current_date_index=di,
                total_dates=td,
                current_index=bi,
                total_in_scope=tb,
                pct=pct,
                message=msg,
            )

        elif phase == "capture":
            total_cap = info.get("total_captured", 0)
            msg = (
                f"[캡처] {info.get('date')} / {info.get('board')}"
                f" – 누적 {total_cap}개 완료"
            )
            kwargs = dict(
                phase="capturing",
                current_date=info.get("date"),
                current_board=info.get("board"),
                current_index=info.get("captured_count", 0),
                total_in_scope=max(total_cap, 1),
                message=msg,
            )

        elif phase == "ocr_date_start":
            idx = info.get("index", 0)
            total = max(info.get("total_dates", 1), 1)
            pct = round((idx - 1) / total * 100, 1)
            msg = f"[OCR] {info.get('date')} ({idx}/{total}) 분석 시작"
            kwargs = dict(
                phase="ocr",
                current_date=info.get("date"),
                current_board=None,
                current_index=idx,
                total_in_scope=0,
                current_date_index=idx,
                total_dates=total,
                pct=pct,
                message=msg,
            )

        elif phase == "ocr_date_done":
            idx = info.get("index", 0)
            total = max(info.get("total_dates", 1), 1)
            pct = round(idx / total * 100, 1)
            msg = f"[OCR] {info.get('date')} ({idx}/{total}) 완료 💾 저장됨"
            kwargs = dict(
                pct=pct,
                current_date_index=idx,
                total_dates=total,
                img_current=0,
                img_total=0,
                img_active=[],
                message=msg,
            )

        elif phase == "ocr_start_board":
            total_imgs = info.get("total_images", 0)
            msg = (
                f"[OCR] {info.get('date')} / {info.get('board')}"
                f" – 이미지 {total_imgs}개 분석 시작"
            )
            kwargs = dict(
                phase="ocr",
                current_board=info.get("board"),
                current_index=0,
                total_in_scope=total_imgs,
                img_current=0,
                img_total=total_imgs,
                img_active=[],
                message=msg,
            )

        elif phase == "ocr_image_start":
            fn = info.get("filename", "")
            # in-flight 목록에 추가 (thread-safe는 _set_progress 내부 락으로 처리)
            with self._lock:
                active = list(self.progress.img_active or [])
                if fn not in active:
                    active.append(fn)
            # 별도 broadcast: 시작 알림만 (progress 업데이트 포함)
            kwargs = dict(img_active=active)
            # 로그: 간결하게 파일명만
            msg = f"  ▷ OCR 시작: {fn}"

        elif phase == "ocr_image_done":
            completed = info.get("completed", 0)
            total = info.get("total", 1)
            fn = info.get("filename", "")
            # in-flight 목록에서 제거
            with self._lock:
                active = [x for x in (self.progress.img_active or []) if x != fn]
            kwargs = dict(
                img_current=completed,
                img_total=total,
                img_active=active,
                current_index=completed,
                total_in_scope=total,
                message=f"[OCR] {info.get('board')} {completed}/{total} 완료",
            )
            msg = f"  ✓ OCR 완료: {fn}  ({completed}/{total})"

        if kwargs:
            self._set_progress(**kwargs)
        if msg:
            self._log(msg)

    # ── 파이프라인 실행 ───────────────────────────────────────
    def _run_pipeline(self, mode: str, date_list=None, target_boards=None, credentials=None) -> None:
        """
        date_list: ['YYYY-MM-DD', ...] 지정 시 해당 날짜만 처리.
                   None이면 자동 감지 (get_missing_dates).
        target_boards: ["구매","판매","품목표"] 중 선택. None이면 전체.
        credentials: {"login_email": "...", "login_pwd": "..."} 카카오 로그인 정보.
        """
        from datetime import datetime as _dt
        hooks = {
            "on_step": self._on_step,
            "check_pause_stop": self._check_pause_stop,
        }
        try:
            # ── 1단계: RPA 캡처 ──────────────────────────────
            if mode in ("full", "rpa_only"):
                if date_list:
                    rpa_dates = [_dt.strptime(d, "%Y-%m-%d") for d in date_list]
                    self._log(f"지정 날짜 {len(rpa_dates)}개 캡처: {date_list[0]} ~ {date_list[-1]}")
                else:
                    self._set_progress(phase="capturing", message="누락 날짜 계산 중...")
                    rpa_dates = rpa_automation.get_missing_dates()

                if not rpa_dates:
                    self._log("캡처할 날짜가 없어 건너뜁니다.")
                else:
                    self._set_progress(
                        phase="capturing",
                        total_dates=len(rpa_dates),
                        message=f"{len(rpa_dates)}개 날짜 캡처 시작",
                    )
                    rpa_automation.run_rpa(date_list=rpa_dates, hooks=hooks, target_boards=target_boards, credentials=credentials)

            if mode == "rpa_only":
                self._set_progress(phase="finished", message="캡처 완료", pct=100.0)
                self._set_state("IDLE")
                self._log("✅ RPA 캡처 완료")
                return

            # ── 2단계: OCR 배치 ──────────────────────────────
            self._set_progress(phase="ocr", message="OCR 분석 대상 날짜 스캔 중...")
            batch_processor.run_enhanced_batch_all(
                show_gui=False,
                force_reprocess=bool(date_list),   # 지정 날짜면 이미 처리된 것도 재처리
                target_dates=date_list,             # None이면 전체 자동 스캔
                hooks=hooks,
                target_boards=target_boards,
            )

            self._set_progress(phase="finished", message="모든 작업 완료", pct=100.0)
            self._set_state("IDLE")
            self._log("✅ 모든 작업이 완료되었습니다!")

        except StopSignal:
            self._log("⏹ 사용자 요청으로 작업 중단됨")
            self._set_progress(phase="idle", message="중단됨")
            self._set_state("IDLE")
        except Exception as e:
            import traceback
            self._log(f"❌ 예기치 못한 오류: {e}")
            self._log(traceback.format_exc())
            self._set_progress(phase="error", message=str(e))
            self._set_state("IDLE")

    # ── 스마트 재개 상태 조회 ─────────────────────────────────
    def get_resume_state(self) -> Dict[str, Any]:
        """
        파일시스템을 스캔해 날짜별 RPA/OCR 완료 여부를 반환.
        웹 UI의 '스마트 재개 현황' 테이블에 표시.
        """
        import os
        BASE_DIR = os.path.dirname(os.path.abspath(batch_processor.__file__))
        root = os.path.join(BASE_DIR, "visionmeat")
        boards = ["구매", "판매", "품목표", "회원정보", "등업신청"]
        result: Dict[str, Any] = {}

        if not os.path.isdir(root):
            return result

        for entry in sorted(os.listdir(root)):
            entry_path = os.path.join(root, entry)
            if not os.path.isdir(entry_path):
                continue
            try:
                datetime.strptime(entry, "%Y-%m-%d")
            except ValueError:
                continue

            date_info: Dict[str, Any] = {}
            for board in boards:
                s_dir = os.path.join(entry_path, board, "screenshots")
                e_dir = os.path.join(entry_path, board, "excel")
                expected_excel = os.path.join(
                    e_dir, f"{entry.replace('-', '')}_{board}_데이터.xlsx"
                )
                dir_exists = os.path.isdir(s_dir)
                has_screenshots = dir_exists and any(
                    f.lower().endswith((".png", ".jpg", ".jpeg"))
                    for f in os.listdir(s_dir)
                ) if dir_exists else False
                has_ocr = os.path.exists(expected_excel)
                date_info[board] = {
                    "rpa_done": has_screenshots,
                    "ocr_done": has_ocr,
                    "no_posts": dir_exists and not has_screenshots,
                }
            result[entry] = date_info

        return result


# 전역 싱글톤
job_manager = JobManager()
