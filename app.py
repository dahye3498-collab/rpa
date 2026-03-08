"""
app.py – Flask 웹 서버
VisionMeat RPA/OCR 제어 패널

실행:
    python app.py
    또는
    flask --app app run --host 0.0.0.0 --port 5000 --no-debugger

접속: http://localhost:5000  (같은 LAN의 다른 PC: http://서버IP:5000)
"""

import json
from flask import Flask, render_template, Response, request, jsonify
from flask_cors import CORS

from job_manager import job_manager

app = Flask(__name__)
CORS(app)


# ── 페이지 ───────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── SSE 스트림 ───────────────────────────────────────────────

@app.route("/api/stream")
def sse_stream():
    """
    Server-Sent Events 엔드포인트.
    각 브라우저 탭마다 독립적인 큐를 할당해 상태 변화를 실시간 push.
    """
    q = job_manager.subscribe()
    snapshot = job_manager.get_snapshot()

    def generate():
        try:
            # 접속 즉시 현재 전체 상태를 전송
            yield f"data: {json.dumps(snapshot, ensure_ascii=False)}\n\n"
            while True:
                try:
                    msg = q.get(timeout=20)   # 20초마다 heartbeat
                    yield f"data: {msg}\n\n"
                except Exception:
                    # 연결 유지용 heartbeat (SSE comment)
                    yield ": heartbeat\n\n"
        finally:
            job_manager.unsubscribe(q)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # nginx 버퍼링 비활성화
            "Connection": "keep-alive",
        },
    )


# ── 제어 API ─────────────────────────────────────────────────

@app.route("/api/run", methods=["POST"])
def api_run():
    from datetime import datetime, timedelta
    data = request.get_json(silent=True) or {}
    mode = data.get("mode", "full")
    if mode not in ("full", "rpa_only", "batch_only"):
        return jsonify({"ok": False, "message": f"알 수 없는 mode: {mode}"}), 400

    # 날짜 직접 지정 (date_from ~ date_to 범위를 리스트로 변환)
    date_list = None
    date_from = data.get("date_from", "").strip()
    date_to   = data.get("date_to",   "").strip()
    if date_from and date_to:
        try:
            d0 = datetime.strptime(date_from, "%Y-%m-%d")
            d1 = datetime.strptime(date_to,   "%Y-%m-%d")
            if d0 > d1:
                return jsonify({"ok": False, "message": "시작일이 종료일보다 늦습니다."}), 400
            if (d1 - d0).days > 90:
                return jsonify({"ok": False, "message": "최대 90일 범위까지 지정 가능합니다."}), 400
            date_list = []
            cur = d0
            while cur <= d1:
                date_list.append(cur.strftime("%Y-%m-%d"))
                cur += timedelta(days=1)
        except ValueError:
            return jsonify({"ok": False, "message": "날짜 형식 오류 (YYYY-MM-DD)"}), 400

    # 게시판 선택 (boards: ["구매","판매","품목표"] 중 선택)
    valid_boards = {"구매", "판매", "품목표", "회원정보", "등업신청"}
    boards_raw = data.get("boards", [])
    target_boards = [b for b in boards_raw if b in valid_boards] or None

    ok = job_manager.start(mode=mode, date_list=date_list, target_boards=target_boards)
    if not ok:
        return jsonify({"ok": False, "message": "이미 작업이 실행 중입니다."}), 400
    return jsonify({"ok": True})


@app.route("/api/pause", methods=["POST"])
def api_pause():
    job_manager.pause()
    return jsonify({"ok": True})


@app.route("/api/resume", methods=["POST"])
def api_resume():
    job_manager.resume()
    return jsonify({"ok": True})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    job_manager.stop()
    return jsonify({"ok": True})


@app.route("/api/status")
def api_status():
    return jsonify(job_manager.status())


@app.route("/api/logs")
def api_logs():
    return jsonify(job_manager.get_logs())


@app.route("/api/resume_state")
def api_resume_state():
    """파일시스템 기반 날짜별 완료 현황 반환."""
    return jsonify(job_manager.get_resume_state())


# ── 서버 실행 ────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  VisionMeat RPA/OCR 제어 패널")
    print("  접속 주소: http://localhost:5000")
    print("  (같은 LAN에서) http://<이 PC의 IP>:5000")
    print("=" * 60)
    # threaded=True: SSE 연결마다 별도 스레드
    # use_reloader=False: 리로더가 파이프라인 스레드를 이중 기동하지 않도록
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True,
        use_reloader=False,
    )
