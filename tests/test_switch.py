"""核心模型测试：同址学习、泛洪/转发、带积压改配、并发改配与放行。"""

import threading
import unittest

from switch import Switch, normalize_mac

M1 = "02:00:00:00:00:01"
M2 = "02:00:00:00:00:02"
M3 = "02:00:00:00:00:03"
M4 = "02:00:00:00:00:04"


def recv_macs(sw, pid):
    return [(f.src, f.dst) for f in sw.ports[pid].received]


class TestLearning(unittest.TestCase):
    """同址学习：同一 MAC 的学习、迁移与跨 VLAN 独立。"""

    def test_same_address_learning_and_move(self):
        sw = Switch()
        sw.send_frame(1, M1, M2)                      # M1 首次出现在端口1
        self.assertEqual(sw.mac_tables[1][M1], 1)
        sw.send_frame(2, M1, M3)                      # 同一 M1 出现在端口2 -> 迁移
        self.assertEqual(sw.mac_tables[1][M1], 2)
        # 已知目的按表转发：从端口3发向 M1，只应到端口2
        r = sw.send_frame(3, M3, M1)
        self.assertEqual(r["action"], "forward")
        self.assertEqual(r["delivered"], [2])

    def test_same_address_learned_independently_per_vlan(self):
        sw = Switch()
        sw.set_vlan(3, 20)
        sw.send_frame(1, M1, M2)                      # vlan1: M1 -> 端口1
        sw.send_frame(3, M1, M2)                      # vlan20: 同址 M1 -> 端口3
        self.assertEqual(sw.mac_tables[1][M1], 1)
        self.assertEqual(sw.mac_tables[20][M1], 3)    # 各 VLAN 表互不影响

    def test_known_unicast_not_flooded(self):
        sw = Switch()
        sw.send_frame(2, M2, M1)                      # 学习 M2 -> 端口2（此帧泛洪）
        r = sw.send_frame(1, M1, M2)                  # 已知目的
        self.assertEqual(r["action"], "forward")
        self.assertEqual(r["delivered"], [2])
        self.assertEqual(r["queued"], [])
        # 端口3/4/5 只收到过第一帧泛洪，没有收到第二帧已知单播
        for pid in (3, 4, 5):
            self.assertEqual(recv_macs(sw, pid), [(M2, M1)])
        self.assertEqual(recv_macs(sw, 2), [(M1, M2)])

    def test_unknown_unicast_floods_only_same_vlan(self):
        sw = Switch()
        sw.set_vlan(3, 20)
        sw.set_vlan(4, 20)
        r = sw.send_frame(1, M1, M2)                  # vlan1 未知目的
        self.assertEqual(r["action"], "flood")
        self.assertEqual(sorted(r["delivered"]), [2, 5])   # 只到本 VLAN 其他端口
        self.assertEqual(recv_macs(sw, 3), [])
        self.assertEqual(recv_macs(sw, 4), [])

    def test_destination_on_ingress_port_is_filtered(self):
        sw = Switch()
        sw.send_frame(1, M1, M2)                      # M1 -> 端口1
        r = sw.send_frame(1, M2, M1)                  # 目的 M1 就在入端口
        self.assertEqual(r["action"], "filter")
        self.assertEqual(r["delivered"], [])

    def test_invalid_input_rejected(self):
        sw = Switch()
        with self.assertRaises(ValueError):
            sw.send_frame(1, "not-a-mac", M2)
        with self.assertRaises(ValueError):
            sw.send_frame(1, M1, "ff:ff:ff:ff:ff:ff")  # 只发单播
        with self.assertRaises(ValueError):
            sw.send_frame(9, M1, M2)
        with self.assertRaises(ValueError):
            sw.set_vlan(1, 5000)
        self.assertEqual(normalize_mac("AA-BB-CC-DD-EE-FF"), "aa:bb:cc:dd:ee:ff")


class TestBacklogReconfig(unittest.TestCase):
    """带积压改配：学习项与待发帧同时失效，切回仍空，他口积压照常。"""

    def setUp(self):
        self.sw = Switch()
        # 端口2、4 暂停出帧
        self.sw.pause(2)
        self.sw.pause(4)
        # 让 M2 被学习到端口2（暂停不影响入方向学习）
        self.sw.send_frame(2, M2, M1)
        # 已知目的帧 -> 积压在端口2
        self.sw.send_frame(1, M1, M2)
        # 未知目的帧 -> 泛洪，端口2、4 各积压一帧，端口3、5 直接收到
        self.sw.send_frame(1, M1, M3)

    def test_backlog_exists_before_reconfig(self):
        # 端口2积压：已知单播 f2 + 泛洪 f3；端口4积压：泛洪 f1 + 泛洪 f3
        self.assertEqual(len(self.sw.ports[2].queue), 2)
        self.assertEqual(len(self.sw.ports[4].queue), 2)
        self.assertEqual(len(recv_macs(self.sw, 3)), 2)    # 未暂停口照常收 f1、f3
        self.assertEqual(self.sw.mac_tables[1][M2], 2)

    def test_reconfig_invalidates_entries_and_queue_atomically(self):
        r = self.sw.set_vlan(2, 20)
        self.assertTrue(r["changed"])
        self.assertEqual(r["dropped_queue"], 2)            # 待发帧失效
        self.assertIn({"vlan": 1, "mac": M2}, r["purged_entries"])  # 学习项失效
        self.assertEqual(len(self.sw.ports[2].queue), 0)
        self.assertNotIn(M2, self.sw.mac_tables.get(1, {}))
        # 其他端口积压不受影响
        self.assertEqual(len(self.sw.ports[4].queue), 2)

    def test_switch_back_to_original_vlan_still_empty(self):
        self.sw.set_vlan(2, 20)
        self.sw.set_vlan(2, 1)                             # 切回原 VLAN
        self.assertEqual(len(self.sw.ports[2].queue), 0)   # 仍按空队列处理
        r = self.sw.release(2)
        self.assertEqual(r["delivered"], 0)
        self.assertEqual(recv_macs(self.sw, 2), [])

    def test_other_ports_backlog_delivered_normally(self):
        self.sw.set_vlan(2, 20)
        r = self.sw.release(4)                             # 端口4 积压照常交付
        self.assertEqual(r["delivered"], 2)
        self.assertEqual(len(recv_macs(self.sw, 4)), 2)

    def test_after_reconfig_old_destination_is_unknown_again(self):
        self.sw.set_vlan(2, 20)
        r = self.sw.send_frame(1, M1, M2)                  # M2 表项已失效
        self.assertEqual(r["action"], "flood")             # 重新按未知目的泛洪
        self.assertNotIn(2, r["delivered"] + r["queued"])  # 且端口2已不在 vlan1

    def test_same_vlan_reapply_is_noop(self):
        r = self.sw.set_vlan(2, 1)                         # VLAN 未变化
        self.assertFalse(r["changed"])
        self.assertEqual(len(self.sw.ports[2].queue), 2)   # 积压保留


class TestConcurrency(unittest.TestCase):
    """同一端口的改配与放行并发：各自原子、不破坏不变式。"""

    def test_concurrent_reconfig_release_pause_send(self):
        sw = Switch()
        errors = []

        def loop(fn):
            def run():
                for i in range(300):
                    try:
                        fn(i)
                    except Exception as e:                 # noqa: BLE001
                        errors.append(e)
            return run

        threads = [
            threading.Thread(target=loop(lambda i: sw.set_vlan(2, 10 + (i % 3)))),
            threading.Thread(target=loop(lambda i: sw.release(2))),
            threading.Thread(target=loop(lambda i: sw.pause(2))),
            threading.Thread(target=loop(lambda i: sw.send_frame(1, M1, M2))),
            threading.Thread(target=loop(lambda i: sw.send_frame(2, M2, M1))),
            threading.Thread(target=loop(lambda i: sw.state())),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertTrue(sw.check_invariants())


if __name__ == "__main__":
    unittest.main()
