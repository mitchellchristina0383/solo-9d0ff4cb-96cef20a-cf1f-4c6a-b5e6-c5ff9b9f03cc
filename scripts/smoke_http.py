"""端到端冒烟：起真实 HTTP 服务，跑「同址学习」「带积压改配」「并发改配+放行」。

用法:  python scripts/smoke_http.py        （从仓库根目录运行）
"""

import json
import os
import sys
import threading
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from werkzeug.serving import make_server

from app import create_app
from switch import Switch

BASE = "http://127.0.0.1:8765"
M1, M2, M3, M4 = ("02:00:00:00:00:0%d" % i for i in (1, 2, 3, 4))

passed = failed = 0


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}  {detail}")


def req(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def post(path, payload=None):
    return req("POST", path, payload)


def state():
    return req("GET", "/api/state")[1]


def port(st, pid):
    return next(p for p in st["ports"] if p["id"] == pid)


def scenario_same_address_learning():
    print("\n== 场景一：同址学习（学习 / 迁移 / 跨 VLAN 独立） ==")
    post("/api/reset")
    _, r = post("/api/frame", {"port": 1, "src": M1, "dst": M2})
    check("首帧未知目的泛洪", r["action"] == "flood" and sorted(r["delivered"]) == [2, 3, 4, 5], r)
    st = state()
    check("vlan1 学到 M1->端口1", st["mac_tables"].get("1", {}).get(M1) == 1, st["mac_tables"])

    post("/api/frame", {"port": 2, "src": M1, "dst": M3})
    st = state()
    check("同址 M1 出现于端口2，表项迁移", st["mac_tables"]["1"][M1] == 2, st["mac_tables"])

    _, r = post("/api/frame", {"port": 3, "src": M3, "dst": M1})
    check("已知目的按表转发只到端口2", r["action"] == "forward" and r["delivered"] == [2], r)

    post("/api/port/3/vlan", {"vlan": 20})
    post("/api/frame", {"port": 3, "src": M1, "dst": M4})
    st = state()
    check("vlan20 独立学到同址 M1->端口3", st["mac_tables"].get("20", {}).get(M1) == 3, st["mac_tables"])
    check("vlan1 的 M1 表项不受影响", st["mac_tables"]["1"][M1] == 2, st["mac_tables"])

    post("/api/reset")
    post("/api/port/3/vlan", {"vlan": 20})
    post("/api/port/4/vlan", {"vlan": 20})
    _, r = post("/api/frame", {"port": 1, "src": M1, "dst": M2})
    check("未知单播只泛洪本 VLAN（不到 vlan20 的端口3、4）",
          r["action"] == "flood" and sorted(r["delivered"]) == [2, 5], r)


def scenario_backlog_reconfig():
    print("\n== 场景二：带积压改配 ==")
    post("/api/reset")
    post("/api/port/2/pause")
    post("/api/port/4/pause")
    post("/api/frame", {"port": 2, "src": M2, "dst": M1})          # M2 学到端口2
    post("/api/frame", {"port": 1, "src": M1, "dst": M2})          # 已知目的 -> 积压端口2
    post("/api/frame", {"port": 1, "src": M1, "dst": M3})          # 未知 -> 泛洪，2、4 积压
    st = state()
    check("改配前：端口2积压2帧、端口4积压2帧",
          port(st, 2)["queued"] == 2 and port(st, 4)["queued"] == 2,
          (port(st, 2)["queued"], port(st, 4)["queued"]))

    _, r = post("/api/port/2/vlan", {"vlan": 20})
    check("改配生效：端口2积压2帧被丢弃", r["dropped_queue"] == 2, r)
    check("改配生效：指向端口2的学习项失效",
          {"vlan": 1, "mac": M2} in r["purged_entries"], r)
    st = state()
    check("端口2队列已空", port(st, 2)["queued"] == 0)
    check("vlan1 中 M2 表项已删", M2 not in st["mac_tables"].get("1", {}), st["mac_tables"])
    check("其他端口（4）积压未受影响", port(st, 4)["queued"] == 2)

    _, r = post("/api/port/2/release")
    check("改配后放行端口2：无帧可交付", r["delivered"] == 0, r)
    post("/api/port/2/vlan", {"vlan": 1})                          # 切回原 VLAN
    st = state()
    _, r = post("/api/port/2/release")
    check("切回原 VLAN 仍按空队列处理", port(st, 2)["queued"] == 0 and r["delivered"] == 0, r)

    _, r = post("/api/port/4/release")
    check("其他端口积压照常交付（端口4交付2帧）", r["delivered"] == 2, r)

    _, r = post("/api/frame", {"port": 1, "src": M1, "dst": M2})
    check("旧学习项失效后 M2 重新按未知目的泛洪", r["action"] == "flood", r)


def scenario_concurrent():
    print("\n== 场景三：同一端口并发改配与放行（含暂停、发帧） ==")
    post("/api/reset")
    errors = []

    def hammer(fn, n=150):
        def run():
            for i in range(n):
                try:
                    fn(i)
                except Exception as e:                              # noqa: BLE001
                    errors.append(repr(e))
        return run

    threads = [
        threading.Thread(target=hammer(lambda i: post("/api/port/2/vlan", {"vlan": 10 + i % 3}))),
        threading.Thread(target=hammer(lambda i: post("/api/port/2/release"))),
        threading.Thread(target=hammer(lambda i: post("/api/port/2/pause"))),
        threading.Thread(target=hammer(lambda i: post("/api/frame",
                                                      {"port": 1, "src": M1, "dst": M2}))),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("并发请求全部正常响应（无 500/异常）", not errors, errors[:3])
    st = state()
    ok = True
    for vlan, table in st["mac_tables"].items():
        for mac, pid in table.items():
            p = port(st, pid)
            if str(p["vlan"]) != str(vlan):
                ok = False
    check("不变式：学习项都指向当前处于该 VLAN 的端口", ok, st["mac_tables"])
    ok = all(p["queued"] == 0 or p["paused"] for p in st["ports"])
    check("不变式：有积压的端口必处于暂停态", ok,
          [(p["id"], p["queued"], p["paused"]) for p in st["ports"]])


def main():
    import logging
    logging.getLogger("werkzeug").setLevel(logging.ERROR)   # 只保留 PASS/FAIL 输出
    sw = Switch()
    server = make_server("127.0.0.1", 8765, create_app(sw), threaded=True)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        scenario_same_address_learning()
        scenario_backlog_reconfig()
        scenario_concurrent()
    finally:
        server.shutdown()
    print(f"\n结果: {passed} 通过, {failed} 失败")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
