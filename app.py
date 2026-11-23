"""Flask Web 层：五端口虚拟交换机实验台。

直接运行:  python app.py        （监听 0.0.0.0:8000，threaded=True）
容器运行:  见 Dockerfile / docker-compose.yml
"""

from flask import Flask, jsonify, render_template, request

from switch import Switch


def create_app(switch=None):
    app = Flask(__name__)
    sw = switch if switch is not None else Switch()

    def body():
        return request.get_json(silent=True) or {}

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/state")
    def api_state():
        return jsonify(sw.state())

    @app.post("/api/frame")
    def api_frame():
        data = body()
        try:
            return jsonify(sw.send_frame(
                data.get("port"), data.get("src", ""),
                data.get("dst", ""), data.get("payload", "")))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @app.post("/api/port/<int:port_id>/vlan")
    def api_set_vlan(port_id):
        try:
            return jsonify(sw.set_vlan(port_id, body().get("vlan")))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @app.post("/api/port/<int:port_id>/pause")
    def api_pause(port_id):
        try:
            return jsonify(sw.pause(port_id))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @app.post("/api/port/<int:port_id>/release")
    def api_release(port_id):
        try:
            return jsonify(sw.release(port_id))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @app.post("/api/reset")
    def api_reset():
        return jsonify(sw.reset())

    # ---------- 双链路聚合演练 ----------
    @app.post("/api/drill/start")
    def api_drill_start():
        return jsonify(sw.start_drill())

    @app.post("/api/drill/stop")
    def api_drill_stop():
        return jsonify(sw.stop_drill())

    @app.post("/api/port/<int:port_id>/link")
    def api_set_link(port_id):
        try:
            return jsonify(sw.set_link(port_id, bool(body().get("down"))))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    return app


app = create_app()

if __name__ == "__main__":
    # threaded=True：同一端口的改配与放行等请求可并发到达，
    # 由 Switch 内部的锁保证各自原子生效。
    app.run(host="0.0.0.0", port=8000, threaded=True)
