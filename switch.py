"""五端口 VLAN 虚拟交换机核心转发模型（含双链路聚合演练）。

线程模型：所有公开操作都持有同一把可重入锁，因此
  - 同一端口的「改配 VLAN」与「放行积压」可以从两个 HTTP 请求并发发起，
    二者被串行化，各自原子生效，不会交错；
  - 改配一旦生效（锁内同一段临界区），指向该口的学习项与该口的待发
    队列同时失效；之后即使切回原 VLAN，队列也已是空的。

双链路演练（start_drill）：复位整机后把端口 4、5 绑定成固定在 VLAN 1 的
逻辑聚合口 lag1，模拟两条线接同一台设备：
  - 从任一成员口进来的帧，源 MAC 都学到聚合口 lag1（不学到物理成员）；
  - 组内收到的帧只向组外转发，绝不回注另一个成员（hairpin 抑制）；
  - 组外泛洪到聚合口时只发一份，由哈希选定唯一成员口；
  - 发往聚合口的单播按 (源 MAC, 目的 MAC) 哈希在两条线上分流，同一流
    固定走同一条成员线；
  - 网页可断开任一成员链路：断链后只用存活成员收发，学习项仍指向 lag1；
    断链成员的待发帧按原帧号并入存活成员队列，存活成员暂停时继续等，
    已经交付的帧留在 received 里、绝不重发。
"""

from __future__ import annotations

import re
import threading
import time
import zlib
from collections import deque
from dataclasses import dataclass, field

NUM_PORTS = 5
MIN_VLAN, MAX_VLAN = 1, 4094
RECEIVED_LIMIT = 500   # 每端口已收帧历史上限（防内存膨胀）
LOG_LIMIT = 300        # 事件日志上限

LAG_ID = "lag1"                 # 聚合口在 MAC 表中的逻辑端口标识
LAG_MEMBERS = (4, 5)            # 聚合组成员（演练开始时绑定）
LAG_VLAN = 1                    # 聚合口固定 VLAN

_MAC_RE = re.compile(r"^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")

# 转发目标列表中的「聚合口端点」哨兵：实际出端口由哈希在存活成员中选
_LAG_ENDPOINT = object()


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
    dropped_total: int = 0   # 改配/解散聚合组时被丢弃的积压帧累计
    # 双链路演练：非 None 表示本口是某聚合口成员；link_down 模拟该线断开
    agg_member: str | None = None
    link_down: bool = False

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
            "agg_member": self.agg_member,
            "link_down": self.link_down,
        }


@dataclass
class AggGroup:
    """聚合口（链路聚合组）：成员物理口共享一个逻辑端口标识。"""

    id: str = LAG_ID
    vlan: int = LAG_VLAN
    members: tuple = LAG_MEMBERS
    down: set = field(default_factory=set)      # 被网页模拟断开的成员口
    dropped_total: int = 0                      # 全员断线时发往聚合口被丢的帧

    def active_members(self):
        """当前存活（未断线）成员，按口号升序，保证哈希取线稳定。"""
        return tuple(p for p in self.members if p not in self.down)

    def to_dict(self):
        return {
            "id": self.id,
            "vlan": self.vlan,
            "members": list(self.members),
            "down": sorted(self.down),
            "active": list(self.active_members()),
            "dropped_total": self.dropped_total,
        }


class Switch:
    def __init__(self, num_ports=NUM_PORTS):
        self._lock = threading.RLock()
        self.ports = {i: Port(id=i) for i in range(1, num_ports + 1)}
        self.mac_tables = {}          # vlan(int) -> {mac: 端口标识}，各 VLAN 独立学习
        self._seq = 0
        self.log = deque(maxlen=LOG_LIMIT)
        self.agg = None               # 演练进行中时为 AggGroup，否则 None

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

    @staticmethod
    def _ep_name(endpoint):
        """MAC 表项/日志里逻辑端口的可读名。"""
        return f"聚合口 {endpoint}" if endpoint == LAG_ID else f"端口 {endpoint}"

    def _lag_pick_member(self, frame):
        """发往聚合口的帧按 (源 MAC, 目的 MAC) 哈希选存活成员。

        成员列表固定升序，同一 (src, dst) 永远取到同一条线；
        两条线都断时返回 None（调用方丢帧并计数）。
        """
        active = self.agg.active_members()
        if not active:
            return None
        h = zlib.crc32(f"{frame.src}{frame.dst}".encode()) % len(active)
        return active[h]

    def _deliver(self, port, frame, delivered, queued):
        """出端口交付：暂停（且链路存活）则积压，否则实际交付。"""
        if port.paused:
            port.queue.append(frame)
            queued.append(port.id)
        else:
            port.received.append(frame)
            port.received_total += 1
            delivered.append(port.id)

    def _merge_queue(self, src_port, dst_port):
        """把 src_port 的待发帧并入 dst_port，按原帧号统一排序。

        返回 (并入帧数, 目标口未暂停而即时交付的帧数)。
        已在 src_port 交付过的帧留在 received 中，不参与、不重发。
        """
        moved = len(src_port.queue)
        merged = sorted(list(src_port.queue) + list(dst_port.queue),
                        key=lambda f: f.seq)
        src_port.queue.clear()
        dst_port.queue = deque(merged)
        flushed = 0
        if not dst_port.paused:
            while dst_port.queue:
                frame = dst_port.queue.popleft()
                dst_port.received.append(frame)
                dst_port.received_total += 1
                flushed += 1
        return moved, flushed

    # ---------- 帧发送 / 学习 / 转发 ----------
    def send_frame(self, port_id, src, dst, payload=""):
        """从 port_id 注入一帧单播帧。返回转发结果。"""
        with self._lock:
            ingress_port = self._port(port_id)
            if self.agg and ingress_port.id in self.agg.members and ingress_port.link_down:
                raise ValueError(f"端口{port_id} 链路已断开，不能注入帧")
            src = normalize_mac(src)
            dst = normalize_mac(dst)
            if not is_unicast(src):
                raise ValueError("源 MAC 必须是单播地址")
            if not is_unicast(dst):
                raise ValueError("目的 MAC 必须是单播地址（本实验台只发单播帧）")
            payload = (payload or "")[:2000]
            vlan = ingress_port.vlan
            self._seq += 1
            frame = Frame(self._seq, src, dst, payload, port_id, vlan)

            # 入端口的逻辑标识：聚合成员口入帧一律算聚合口
            in_lag = bool(self.agg and ingress_port.id in self.agg.members)
            ingress_logical = LAG_ID if in_lag else ingress_port.id

            # 1) 源地址学习：各 VLAN 一张独立表；同址再学习会刷新/迁移表项
            table = self.mac_tables.setdefault(vlan, {})
            prev = table.get(src)
            table[src] = ingress_logical
            if prev is None:
                learn_note = f"学习 {src} -> {self._ep_name(ingress_logical)} (vlan {vlan})"
            elif prev != ingress_logical:
                learn_note = (f"{src} 迁移 {self._ep_name(prev)} -> "
                              f"{self._ep_name(ingress_logical)} (vlan {vlan})")
            else:
                learn_note = None

            # 2) 转发决策：已知目的按表转发；未知目的只泛洪到本 VLAN 其他端口
            member_set = set(self.agg.members) if self.agg else set()
            target = table.get(dst)
            lag_egress = None        # 实际选中的聚合成员口（未走聚合口则为 None）
            lag_dropped = False
            if target is None:
                # 泛洪：组外物理口 + 至多一个聚合口端点（组内入帧不回注聚合组）
                endpoints = [p for p in self.ports.values()
                             if p.id != port_id
                             and p.id not in member_set
                             and p.vlan == vlan]
                if self.agg and self.agg.vlan == vlan and not in_lag:
                    endpoints.append(_LAG_ENDPOINT)
                action = "flood"
            elif target == ingress_logical:
                endpoints = []       # 目的就在入端口（含同一聚合组），过滤不转
                action = "filter"
            else:
                endpoints = ([_LAG_ENDPOINT] if target == LAG_ID
                             else [self.ports[target]])
                action = "forward"

            # 3) 出端口：聚合端点先哈希选成员；暂停则积压，否则交付
            delivered, queued = [], []
            for ep in endpoints:
                if ep is _LAG_ENDPOINT:
                    member_id = self._lag_pick_member(frame)
                    lag_egress = member_id
                    if member_id is None:
                        self.agg.dropped_total += 1
                        lag_dropped = True
                        continue
                    self._deliver(self.ports[member_id], frame, delivered, queued)
                else:
                    self._deliver(ep, frame, delivered, queued)
            if lag_dropped and not delivered and not queued:
                # 聚合口双线均断且无其他出端口：整帧丢弃；
                # 泛洪场景中其他物理口仍正常泛洪时，动作仍记为 flood
                action = "drop"

            parts = [f"帧#{frame.seq} {src} -> {dst} 入端口{port_id} vlan{vlan}: {action}"]
            if delivered:
                parts.append(f"已交付 {delivered}")
            if queued:
                parts.append(f"积压于 {queued}")
            if self.agg and any(ep is _LAG_ENDPOINT for ep in endpoints):
                if lag_egress is None:
                    parts.append(f"聚合口 {LAG_ID} 两条线均断，副本丢弃")
                else:
                    parts.append(f"聚合口分流到端口{lag_egress}")
            if learn_note:
                parts.append(learn_note)
            self._log("; ".join(parts))
            return {
                "frame": frame.to_dict(), "action": action,
                "delivered": delivered, "queued": queued,
                "lag_egress": lag_egress, "lag_dropped": lag_dropped,
            }

    # ---------- 改配 VLAN ----------
    def set_vlan(self, port_id, vlan):
        """改配端口 VLAN。生效瞬间（同一临界区内）：
        1) 所有 VLAN 表中指向本口的学习项全部失效；
        2) 本口待发队列全部丢弃（之后切回原 VLAN 也是空队列）。
        其他端口的积压不受影响。聚合组成员在演练期间锁定 VLAN，禁止改配。
        """
        vlan = validate_vlan(vlan)
        with self._lock:
            port = self._port(port_id)
            if self.agg and port.id in self.agg.members:
                raise ValueError(
                    f"端口{port_id} 是聚合口 {LAG_ID} 成员，演练期间固定 "
                    f"VLAN {self.agg.vlan}，不能改配")
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
            if port.link_down:
                raise ValueError(f"端口{port_id} 链路已断开，不能暂停出帧")
            port.paused = True
            self._log(f"端口{port_id}: 暂停出帧（当前积压 {len(port.queue)} 帧）")
            return {"port": port_id, "paused": True}

    def release(self, port_id):
        """放行本口：按 FIFO 交付全部积压帧，恢复直接转发。"""
        with self._lock:
            port = self._port(port_id)
            if port.link_down:
                raise ValueError(f"端口{port_id} 链路已断开，不能放行出帧")
            port.paused = False
            n = 0
            while port.queue:
                frame = port.queue.popleft()
                port.received.append(frame)
                port.received_total += 1
                n += 1
            self._log(f"端口{port_id}: 放行出帧，交付积压 {n} 帧")
            return {"port": port_id, "paused": False, "delivered": n}

    # ---------- 双链路演练 ----------
    def start_drill(self):
        """开演：复位交换机，把 4、5 号口绑成固定在 VLAN 1 的聚合口 lag1。"""
        with self._lock:
            self.reset()
            self.agg = AggGroup()
            for pid in self.agg.members:
                member = self.ports[pid]
                member.agg_member = self.agg.id
                member.vlan = self.agg.vlan
            self._log(
                f"双链路演练开始：端口{self.agg.members[0]}、{self.agg.members[1]} "
                f"绑定为聚合口 {LAG_ID}（固定 VLAN {LAG_VLAN}），模拟两条线接同一台设备"
            )
            return {"ok": True, "agg": self.agg.to_dict()}

    def stop_drill(self):
        """收演：拆除聚合组，成员口恢复为普通物理口。

        指向聚合口的学习项失效，成员口残留积压按丢弃处理（与改配语义一致）。
        """
        with self._lock:
            if self.agg is None:
                self._log("双链路演练未开始，无需结束")
                return {"ok": False, "purged_entries": [], "dropped_queue": 0}
            agg = self.agg
            purged = []
            for v, table in self.mac_tables.items():
                for mac in [m for m, ep in table.items() if ep == LAG_ID]:
                    del table[mac]
                    purged.append({"vlan": v, "mac": mac})
            dropped = 0
            for pid in agg.members:
                member = self.ports[pid]
                dropped += len(member.queue)
                if member.queue:
                    member.dropped_total += len(member.queue)
                    member.queue.clear()
                member.agg_member = None
                member.link_down = False
                member.paused = False
            self.agg = None
            self._log(
                f"双链路演练结束：聚合口 {LAG_ID} 拆除；失效学习项 {len(purged)} 条，"
                f"丢弃成员积压 {dropped} 帧，端口 {agg.members[0]}、{agg.members[1]} 恢复普通口"
            )
            return {"ok": True, "purged_entries": purged, "dropped_queue": dropped}

    def set_link(self, port_id, down):
        """模拟聚合成员链路通断。

        断线（down=True）：
          - 之后只用存活成员收发，MAC 学习项仍指向聚合口 lag1；
          - 断链成员的待发帧按原帧号并入存活成员队列；存活成员若处于暂停，
            并入后继续等，放行时按帧号 FIFO 交付；已交付帧不重发；
          - 两条线都断（防御场景）时队列保留在断链口，发往聚合口的帧直接丢弃计数。
        恢复（down=False）：成员重新参与哈希分流；双断恢复时另一条断链口的
        残留队列同样按帧号并入本口。
        """
        with self._lock:
            port = self._port(port_id)
            if self.agg is None or port.id not in self.agg.members:
                raise ValueError(f"端口{port_id} 不是聚合口 {LAG_ID} 成员，不能演练断链")
            down = bool(down)
            if port.link_down == down:
                self._log(f"端口{port_id} 链路已是{'断开' if down else '连通'}状态，无变化")
                return {"port": port_id, "link_down": down, "merged": 0, "flushed": 0,
                        "agg": self.agg.to_dict()}
            if down:
                port.link_down = True
                port.paused = False
                self.agg.down.add(port.id)
                survivors = [self.ports[p] for p in self.agg.active_members()]
                merged = flushed = 0
                if survivors:
                    survivor = survivors[0]
                    merged, flushed = self._merge_queue(port, survivor)
                    self._log(
                        f"端口{port.id} 链路断开：聚合口仅存活端口{survivor.id} 收发，"
                        f"学习项仍指向 {LAG_ID}；断链口 {merged} 帧待发按帧号并入"
                        f"端口{survivor.id}（即时交付 {flushed} 帧，已交付帧不重发）"
                    )
                else:
                    self._log(f"端口{port.id} 链路断开：聚合口两条线均断，"
                              f"待发帧 {len(port.queue)} 保留，新帧将丢弃")
                return {"port": port_id, "link_down": True, "merged": merged,
                        "flushed": flushed, "agg": self.agg.to_dict()}

            # 先并入仍断线成员的残留队列，再宣告本口恢复，保证存活列表准确
            merged = flushed = 0
            for other_id in self.agg.members:
                other = self.ports[other_id]
                if other_id != port.id and other.link_down and other.queue:
                    merged, flushed = self._merge_queue(other, port)
            port.link_down = False
            self.agg.down.discard(port.id)
            self._log(
                f"端口{port.id} 链路恢复：存活成员 {list(self.agg.active_members())}，"
                f"恢复双线哈希分流；并入残留待发 {merged} 帧（即时交付 {flushed} 帧）"
            )
            return {"port": port_id, "link_down": False, "merged": merged,
                    "flushed": flushed, "agg": self.agg.to_dict()}

    # ---------- 状态 ----------
    def state(self):
        with self._lock:
            return {
                "ports": [self.ports[i].to_dict() for i in sorted(self.ports)],
                "mac_tables": {str(v): dict(t)
                               for v, t in sorted(self.mac_tables.items())},
                "agg": self.agg.to_dict() if self.agg else None,
                "log": list(self.log),
            }

    def reset(self):
        with self._lock:
            self.ports = {i: Port(id=i) for i in self.ports}
            self.mac_tables.clear()
            self._seq = 0
            self.agg = None
            self.log.clear()
            self._log("交换机已复位")
            return {"ok": True}

    # ---------- 不变式（供测试/冒烟断言） ----------
    def check_invariants(self):
        with self._lock:
            # 学习项只能指向当前处于该 VLAN 的端口（或同 VLAN 的聚合口）
            for v, table in self.mac_tables.items():
                for mac, endpoint in table.items():
                    if endpoint == LAG_ID:
                        assert self.agg is not None, \
                            f"vlan{v} 表项 {mac} 指向聚合口，但演练已结束"
                        assert self.agg.vlan == v, \
                            f"vlan{v} 表项 {mac} 指向 vlan{self.agg.vlan} 的聚合口"
                    else:
                        p = self.ports[endpoint]
                        assert p.vlan == v, \
                            f"vlan{v} 表项 {mac}->端口{endpoint}，但端口{endpoint}在 vlan{p.vlan}"
                        assert not (self.agg and endpoint in self.agg.members), \
                            f"vlan{v} 表项 {mac} 误学到聚合成员口{endpoint}（应学到 {LAG_ID}）"
            active = set(self.agg.active_members()) if self.agg else set()
            for p in self.ports.values():
                is_member = bool(self.agg and p.id in self.agg.members)
                if is_member:
                    if p.link_down:
                        # 单断时断链口队列必已并入存活成员；只有双断才允许留队
                        assert not p.queue or not active, \
                            f"断链端口{p.id} 仍有存活成员却残留积压 {len(p.queue)} 帧"
                    else:
                        assert not p.queue or p.paused, f"端口{p.id} 未暂停却有积压"
                    for f in p.queue:
                        assert f.vlan == self.agg.vlan, \
                            f"聚合成员端口{p.id} 积压帧 vlan{f.vlan} != 聚合口 vlan{self.agg.vlan}"
                else:
                    # 有积压的端口必然处于暂停态（放行会清空队列，改配会丢弃队列）
                    assert not p.queue or p.paused, f"端口{p.id} 未暂停却有积压"
                    # 积压帧的 VLAN 必等于端口当前 VLAN（改配已清空旧队列）
                    for f in p.queue:
                        assert f.vlan == p.vlan, \
                            f"端口{p.id} 积压帧 vlan{f.vlan} != 端口 vlan{p.vlan}"
            return True
