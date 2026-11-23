"""五端口 VLAN 虚拟交换机核心转发模型。

线程模型：所有公开操作都持有同一把可重入锁，因此
  - 同一端口的「改配 VLAN」与「放行积压」可以从两个 HTTP 请求并发发起，
    二者被串行化，各自原子生效，不会交错；
  - 改配一旦生效（锁内同一段临界区），指向该口的学习项与该口的待发
    队列同时失效；之后即使切回原 VLAN，队列也已是空的。
"""

from __future__ import annotations

import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field

NUM_PORTS = 5
MIN_VLAN, MAX_VLAN = 1, 4094
RECEIVED_LIMIT = 500   # 每端口已收帧历史上限（防内存膨胀）
LOG_LIMIT = 300        # 事件日志上限

_MAC_RE = re.compile(r"^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")


def normalize_mac(mac):
    """归一化为小写冒号分隔形式；非法则抛 ValueError。"""
    if not isinstance(mac, str):
        raise ValueError("MAC 地址必须是字符串")
    mac = mac.strip().lower().replace("-", ":")
    if re.fullmatch(r"([0-9a-f]{4}\.){2}[0-9a-f]{4}", mac):  # aabb.ccdd.eeff
        mac = mac.replace(".", "")
    if re.fullmatch(r"[0-9a-f]{12}", mac):                   # aabbccddeeff
        mac = ":".join(mac[i:i + 2] for i in range(0, 12, 2))
    if not _MAC_RE.match(mac):
        raise ValueError(f"非法 MAC 地址: {mac!r}")
    return mac


def is_unicast(mac):
    """I/G 位为 0 即单播。"""
    return (int(mac.split(":", 1)[0], 16) & 0x01) == 0


def validate_vlan(vlan):
    try:
        vlan = int(vlan)
    except (TypeError, ValueError):
        raise ValueError(f"VLAN 必须是整数: {vlan!r}")
    if not MIN_VLAN <= vlan <= MAX_VLAN:
        raise ValueError(f"VLAN 必须在 {MIN_VLAN}..{MAX_VLAN} 之间: {vlan}")
    return vlan


@dataclass
class Frame:
    seq: int
    src: str
    dst: str
    payload: str
    ingress: int
    vlan: int

    def to_dict(self):
        return {
            "seq": self.seq, "src": self.src, "dst": self.dst,
            "payload": self.payload, "ingress": self.ingress, "vlan": self.vlan,
        }


@dataclass
class Port:
    id: int
    vlan: int = 1
    paused: bool = False
    # 积压队列：暂停期间已决定从本口发出、尚未放行的帧
    queue: deque = field(default_factory=deque)
    # 已交付（本口实际收到）的帧
    received: deque = field(default_factory=lambda: deque(maxlen=RECEIVED_LIMIT))
    received_total: int = 0
    dropped_total: int = 0   # 改配时被丢弃的积压帧累计

    def to_dict(self):
        return {
            "id": self.id,
            "vlan": self.vlan,
            "paused": self.paused,
            "queued": len(self.queue),
            "queue": [f.to_dict() for f in self.queue],
            "received_count": self.received_total,
            "received": [f.to_dict() for f in self.received],
            "dropped_total": self.dropped_total,
        }


class Switch:
    def __init__(self, num_ports=NUM_PORTS):
        self._lock = threading.RLock()
        self.ports = {i: Port(id=i) for i in range(1, num_ports + 1)}
        self.mac_tables = {}          # vlan(int) -> {mac: port_id}，各 VLAN 独立学习
        self._seq = 0
        self.log = deque(maxlen=LOG_LIMIT)

    # ---------- 内部工具（调用方须已持锁） ----------
    def _port(self, port_id):
        try:
            port_id = int(port_id)
        except (TypeError, ValueError):
            raise ValueError(f"非法端口号: {port_id!r}")
        if port_id not in self.ports:
            raise ValueError(f"端口不存在: {port_id}（有效范围 1..{len(self.ports)}）")
        return self.ports[port_id]

    def _log(self, msg):
        self.log.appendleft({"t": time.strftime("%H:%M:%S"), "msg": msg})

    # ---------- 帧发送 / 学习 / 转发 ----------
    def send_frame(self, port_id, src, dst, payload=""):
        """从 port_id 注入一帧单播帧。返回转发结果。"""
        with self._lock:
            port = self._port(port_id)
            src = normalize_mac(src)
            dst = normalize_mac(dst)
            if not is_unicast(src):
                raise ValueError("源 MAC 必须是单播地址")
            if not is_unicast(dst):
                raise ValueError("目的 MAC 必须是单播地址（本实验台只发单播帧）")
            payload = (payload or "")[:2000]
            vlan = port.vlan
            self._seq += 1
            frame = Frame(self._seq, src, dst, payload, port_id, vlan)

            # 1) 源地址学习：各 VLAN 一张独立表；同址再学习会刷新/迁移表项
            table = self.mac_tables.setdefault(vlan, {})
            prev = table.get(src)
            table[src] = port_id
            if prev is None:
                learn_note = f"学习 {src} -> 端口 {port_id} (vlan {vlan})"
            elif prev != port_id:
                learn_note = f"{src} 迁移 端口 {prev} -> {port_id} (vlan {vlan})"
            else:
                learn_note = None

            # 2) 转发决策：已知目的按表转发；未知目的只泛洪到本 VLAN 其他端口
            target = table.get(dst)
            if target is None:
                targets = [p for p in self.ports.values()
                           if p.id != port_id and p.vlan == vlan]
                action = "flood"
            elif target == port_id:
                targets = []          # 目的就在入端口，过滤不转
                action = "filter"
            else:
                targets = [self.ports[target]]
                action = "forward"

            # 3) 出端口：暂停则积压，否则交付
            delivered, queued = [], []
            for p in targets:
                if p.paused:
                    p.queue.append(frame)
                    queued.append(p.id)
                else:
                    p.received.append(frame)
                    p.received_total += 1
                    delivered.append(p.id)

            parts = [f"帧#{frame.seq} {src} -> {dst} 入端口{port_id} vlan{vlan}: {action}"]
            if delivered:
                parts.append(f"已交付 {delivered}")
            if queued:
                parts.append(f"积压于 {queued}")
            if learn_note:
                parts.append(learn_note)
            self._log("; ".join(parts))
            return {
                "frame": frame.to_dict(), "action": action,
                "delivered": delivered, "queued": queued,
            }

    # ---------- 改配 VLAN ----------
    def set_vlan(self, port_id, vlan):
        """改配端口 VLAN。生效瞬间（同一临界区内）：
        1) 所有 VLAN 表中指向本口的学习项全部失效；
        2) 本口待发队列全部丢弃（之后切回原 VLAN 也是空队列）。
        其他端口的积压不受影响。
        """
        vlan = validate_vlan(vlan)
        with self._lock:
            port = self._port(port_id)
            old = port.vlan
            if old == vlan:
                self._log(f"端口{port_id}: VLAN 未变化（{vlan}），不发生改配")
                return {"port": port_id, "old_vlan": old, "vlan": vlan,
                        "changed": False, "purged_entries": [], "dropped_queue": 0}
            port.vlan = vlan
            # (1) 清除指向本口的学习项（跨所有 VLAN）
            purged = []
            for v, table in self.mac_tables.items():
                for mac in [m for m, p in table.items() if p == port_id]:
                    del table[mac]
                    purged.append({"vlan": v, "mac": mac})
            # (2) 丢弃本口积压
            dropped = len(port.queue)
            if dropped:
                port.queue.clear()
                port.dropped_total += dropped
            self._log(
                f"端口{port_id}: vlan {old} -> {vlan}; "
                f"失效学习项 {len(purged)} 条; 丢弃积压 {dropped} 帧"
            )
            return {"port": port_id, "old_vlan": old, "vlan": vlan,
                    "changed": True, "purged_entries": purged,
                    "dropped_queue": dropped}

    # ---------- 暂停 / 放行 ----------
    def pause(self, port_id):
        """暂停本口出帧：之后发往本口的帧进入积压队列。"""
        with self._lock:
            port = self._port(port_id)
            port.paused = True
            self._log(f"端口{port_id}: 暂停出帧（当前积压 {len(port.queue)} 帧）")
            return {"port": port_id, "paused": True}

    def release(self, port_id):
        """放行本口：按 FIFO 交付全部积压帧，恢复直接转发。"""
        with self._lock:
            port = self._port(port_id)
            port.paused = False
            n = 0
            while port.queue:
                frame = port.queue.popleft()
                port.received.append(frame)
                port.received_total += 1
                n += 1
            self._log(f"端口{port_id}: 放行出帧，交付积压 {n} 帧")
            return {"port": port_id, "paused": False, "delivered": n}

    # ---------- 状态 ----------
    def state(self):
        with self._lock:
            return {
                "ports": [self.ports[i].to_dict() for i in sorted(self.ports)],
                "mac_tables": {str(v): dict(t)
                               for v, t in sorted(self.mac_tables.items())},
                "log": list(self.log),
            }

    def reset(self):
        with self._lock:
            self.ports = {i: Port(id=i) for i in self.ports}
            self.mac_tables.clear()
            self._seq = 0
            self.log.clear()
            self._log("交换机已复位")
            return {"ok": True}

    # ---------- 不变式（供测试/冒烟断言） ----------
    def check_invariants(self):
        with self._lock:
            # 学习项只能指向当前处于该 VLAN 的端口
            for v, table in self.mac_tables.items():
                for mac, pid in table.items():
                    assert self.ports[pid].vlan == v, \
                        f"vlan{v} 表项 {mac}->端口{pid}，但端口{pid}在 vlan{self.ports[pid].vlan}"
            for p in self.ports.values():
                # 有积压的端口必然处于暂停态（放行会清空队列，改配会丢弃队列）
                assert not p.queue or p.paused, f"端口{p.id} 未暂停却有积压"
                # 积压帧的 VLAN 必等于端口当前 VLAN（改配已清空旧队列）
                for f in p.queue:
                    assert f.vlan == p.vlan, \
                        f"端口{p.id} 积压帧 vlan{f.vlan} != 端口 vlan{p.vlan}"
            return True
