"""双链路聚合演练测试：聚合口学习、组内抑制、哈希分流、带积压断链。"""

import unittest
import zlib

from switch import LAG_ID, Switch

M1 = "02:00:00:00:00:01"   # 聚合口对端设备（从 4/5 号口注入）
M2 = "02:00:00:00:00:02"   # 普通口侧主机（从 1 号口注入）
M3 = "02:00:00:00:00:03"
M4 = "02:00:00:00:00:04"   # M2->M4 这个流确定哈希到端口 4


def hash_member(src, dst, members=(4, 5)):
    """与 Switch._lag_pick_member 相同的选线规则，供测试构造期望。"""
    return members[zlib.crc32(f"{src}{dst}".encode()) % len(members)]


def recv_macs(sw, pid):
    return [(f.src, f.dst) for f in sw.ports[pid].received]


def queued_seqs(sw, pid):
    return [f.seq for f in sw.ports[pid].queue]


class TestLagDrillSetup(unittest.TestCase):
    def test_start_resets_and_binds_members(self):
        sw = Switch()
        sw.send_frame(1, M1, M2)
        r = sw.start_drill()
        self.assertTrue(r["ok"])
        self.assertEqual(r["agg"]["id"], LAG_ID)
        self.assertEqual(r["agg"]["members"], [4, 5])
        self.assertEqual(r["agg"]["vlan"], 1)
        self.assertEqual(r["agg"]["active"], [4, 5])
        self.assertEqual(sw.mac_tables, {})                 # 复位清表
        for pid in (4, 5):
            p = sw.ports[pid]
            self.assertEqual(p.agg_member, LAG_ID)
            self.assertFalse(p.link_down)
            self.assertEqual(p.vlan, 1)
        self.assertTrue(sw.check_invariants())

    def test_reset_clears_drill(self):
        sw = Switch()
        sw.start_drill()
        sw.reset()
        self.assertIsNone(sw.agg)
        self.assertIsNone(sw.ports[4].agg_member)
        self.assertFalse(sw.ports[5].link_down)

    def test_member_vlan_change_rejected(self):
        sw = Switch()
        sw.start_drill()
        with self.assertRaises(ValueError):
            sw.set_vlan(4, 20)
        with self.assertRaises(ValueError):
            sw.set_vlan(5, 20)
        # 非成员口仍可改配
        sw.set_vlan(3, 20)
        self.assertEqual(sw.ports[3].vlan, 20)

    def test_link_toggle_only_for_members(self):
        sw = Switch()
        with self.assertRaises(ValueError):
            sw.set_link(1, True)
        sw.start_drill()
        with self.assertRaises(ValueError):
            sw.set_link(3, True)
        # 重复设置同一状态不报错、无副作用
        r = sw.set_link(4, False)
        self.assertFalse(r["link_down"])
        self.assertEqual(r["merged"], 0)


class TestLagLearning(unittest.TestCase):
    def setUp(self):
        self.sw = Switch()
        self.sw.start_drill()

    def test_source_learned_on_agg_from_either_member(self):
        # 从成员口 4 注入：源 MAC 学到 lag1，不是物理口 4
        self.sw.send_frame(4, M1, M2)
        self.assertEqual(self.sw.mac_tables[1][M1], LAG_ID)
        # 从另一成员口 5 注入同一源：仍指向 lag1（不迁移到物理口）
        self.sw.send_frame(5, M1, M3)
        self.assertEqual(self.sw.mac_tables[1][M1], LAG_ID)

    def test_known_unicast_to_agg_uses_hash_member(self):
        self.sw.send_frame(4, M1, M2)                      # M1 -> lag1
        r = self.sw.send_frame(1, M2, M1)                  # 已知目的 -> 聚合口
        self.assertEqual(r["action"], "forward")
        chosen = r["lag_egress"]
        self.assertEqual(chosen, hash_member(M2, M1))      # 与哈希规则一致
        self.assertIn(chosen, (4, 5))
        # 组外只向聚合口发一份：只有哈希选中的成员收到
        self.assertEqual(r["delivered"], [chosen])
        other = 5 if chosen == 4 else 4
        self.assertEqual(recv_macs(self.sw, other), [])
        self.assertEqual(recv_macs(self.sw, chosen), [(M2, M1)])

    def test_same_flow_pinned_to_same_member(self):
        self.sw.send_frame(4, M1, M2)
        chosen = [self.sw.send_frame(1, M2, M1)["lag_egress"] for _ in range(5)]
        self.assertEqual(set(chosen), {chosen[0]})          # 同流固定一条线
        self.assertEqual(chosen[0], hash_member(M2, M1))

    def test_other_flows_can_split_across_members(self):
        # 不同 (源,目的) 即不同流；逐流学习后发已知单播，两条线都应被用到
        used = set()
        for i in (1, 3, 4, 5, 6, 7, 8, 9, 10, 11):
            dst = f"02:00:00:00:00:{i:02x}"
            self.sw.send_frame(4, dst, M2)                 # dst 从聚合口学入 lag1
            r = self.sw.send_frame(1, M2, dst)
            self.assertEqual(r["action"], "forward")       # 目的已知 -> 聚合口单播
            self.assertEqual(r["lag_egress"], hash_member(M2, dst))
            used.add(r["lag_egress"])
        self.assertEqual(used, {4, 5})                      # 两条线都被用到

    def test_flood_sends_single_copy_to_agg(self):
        r = self.sw.send_frame(1, M2, M3)                   # 未知目的泛洪
        self.assertEqual(r["action"], "flood")
        # 聚合口只发一份（一个成员），组外端口 2、3 各一份
        agg_hits = [p for p in r["delivered"] if p in (4, 5)]
        self.assertEqual(len(agg_hits), 1)
        self.assertEqual(agg_hits[0], hash_member(M2, M3))
        self.assertEqual(sorted(r["delivered"]), sorted([2, 3, agg_hits[0]]))
        other = 5 if agg_hits[0] == 4 else 4
        self.assertEqual(len(self.sw.ports[other].received), 0)

    def test_ingress_from_member_not_flooded_back_into_group(self):
        # 从成员口 4 注入未知目的帧：泛洪只到组外 1/2/3，不回注成员口 5
        r = self.sw.send_frame(4, M1, M3)
        self.assertEqual(r["action"], "flood")
        self.assertEqual(sorted(r["delivered"]), [1, 2, 3])
        self.assertEqual(len(self.sw.ports[5].received), 0)
        self.assertEqual(len(self.sw.ports[4].received), 0)

    def test_destination_on_ingress_agg_is_filtered(self):
        self.sw.send_frame(4, M1, M2)                      # M1 -> lag1
        r = self.sw.send_frame(5, M3, M1)                  # 目的在同一聚合组
        self.assertEqual(r["action"], "filter")
        self.assertEqual(r["delivered"], [])
        self.assertEqual(r["lag_egress"], None)

    def test_inject_on_down_member_rejected(self):
        self.sw.set_link(4, True)
        with self.assertRaises(ValueError):
            self.sw.send_frame(4, M1, M2)


class TestLagLinkDownWithBacklog(unittest.TestCase):
    def setUp(self):
        self.sw = Switch()
        self.sw.start_drill()
        # M1（对端）从聚合口进来，M2（本地）在端口1
        self.sw.send_frame(4, M1, M2)
        self.sw.send_frame(1, M2, M1)                      # 学习 M2 -> 端口1

    def test_backlog_merged_by_seq_when_member_goes_down(self):
        sw = self.sw
        # 让两条线都积压：暂停 4、5，再发若干已知单播到聚合口
        sw.pause(4)
        sw.pause(5)
        seqs = []
        for i in range(6):
            r = sw.send_frame(1, M2, M1, payload=f"f{i}")
            seqs.append((r["frame"]["seq"], r["lag_egress"]))
        # 同流 (M2,M1) 恒走同一条成员线
        self.assertEqual({m for _, m in seqs}, {hash_member(M2, M1)})
        # 不同目的的另一流压到另一条成员线
        sw.send_frame(4, M4, M2)                           # M4 -> lag1
        for i in range(4):
            sw.send_frame(1, M2, M4, payload=f"g{i}")      # M2->M4 恒定走端口4
        self.assertTrue(queued_seqs(sw, 4))                # 两条线现在都有积压
        self.assertTrue(queued_seqs(sw, 5))
        q4, q5 = queued_seqs(sw, 4), queued_seqs(sw, 5)

        # 断开端口4：其积压按帧号并入存活成员 5
        r = sw.set_link(4, True)
        self.assertEqual(r["merged"], len(q4))
        self.assertEqual(r["flushed"], 0)                  # 存活成员仍暂停，继续等
        self.assertEqual(queued_seqs(sw, 4), [])
        self.assertEqual(queued_seqs(sw, 5), sorted(q4 + q5))  # 按原帧号统一排序
        # 学习项仍指向聚合口
        self.assertEqual(sw.mac_tables[1][M1], LAG_ID)
        self.assertTrue(sw.check_invariants())

        # 放行存活成员：按帧号 FIFO 交付，已交付过的帧不重发
        rel = sw.release(5)
        self.assertEqual(rel["delivered"], len(q4) + len(q5))
        # 放行新增的实收帧即合并队列，按帧号升序；setUp 预置帧不参与、不重发
        newly = [f.seq for f in sw.ports[5].received][-rel["delivered"]:]
        self.assertEqual(newly, sorted(q4 + q5))
        self.assertEqual(len(sw.ports[4].received), 0)

    def test_new_frames_use_only_survivor_after_down(self):
        sw = self.sw
        before5 = sw.ports[5].received_total
        sw.set_link(4, True)
        for _ in range(4):
            r = sw.send_frame(1, M2, M1)
            self.assertEqual(r["lag_egress"], 5)           # 只用存活成员
        self.assertEqual(sw.ports[5].received_total, before5 + 4)
        self.assertEqual(sw.ports[4].received_total, 0)
        # 从存活成员入帧，学习项仍指向 lag1
        sw.send_frame(5, M3, M2)
        self.assertEqual(sw.mac_tables[1][M3], LAG_ID)

    def test_delivered_frames_not_resent_after_down(self):
        sw = self.sw
        sw.send_frame(1, M2, M1)                           # 正常交付到某成员
        delivered_before = {p: sw.ports[p].received_total for p in (4, 5)}
        sw.set_link(4, True)
        sw.set_link(4, False)
        for p in (4, 5):
            self.assertEqual(sw.ports[p].received_total, delivered_before[p])

    def test_paused_survivor_keeps_waiting(self):
        # 用全新整机隔离：setUp 的预置帧不计入本用例
        sw = Switch()
        sw.start_drill()
        sw.send_frame(4, M1, M2)                           # M1 -> lag1
        sw.send_frame(4, M4, M2)                           # M4 -> lag1
        sw.pause(5)                                        # 存活成员先暂停
        sw.pause(4)
        sw.send_frame(1, M2, M1)                           # 积压（M2->M1 走端口5）
        sw.send_frame(1, M2, M4)                           # 压到端口4
        self.assertTrue(queued_seqs(sw, 4))
        backlog5 = len(sw.ports[5].queue)
        sw.set_link(4, True)
        # 存活成员 5 处于暂停：并入后整体继续积压，不自动交付
        self.assertTrue(sw.ports[5].paused)
        self.assertGreater(len(sw.ports[5].queue), backlog5)
        self.assertEqual(sw.ports[5].received_total, 0)

    def test_unpaused_survivor_flushes_merged_frames(self):
        # 用全新整机隔离：只让端口4积压1帧后断链
        sw = Switch()
        sw.start_drill()
        sw.send_frame(4, M4, M2)                           # M4 -> lag1
        sw.pause(4)
        sw.send_frame(1, M2, M4)                           # M2->M4 走端口4，积压1帧
        self.assertEqual(queued_seqs(sw, 4), [sw._seq])
        # 端口5未暂停：断 4 后并入的帧立即按帧号交付
        r = sw.set_link(4, True)
        self.assertEqual(r["merged"], 1)
        self.assertEqual(r["flushed"], 1)
        self.assertEqual(sw.ports[5].received_total, 1)

    def test_recovery_restores_two_line_split(self):
        sw = self.sw
        sw.set_link(4, True)
        self.assertEqual(sw.send_frame(1, M2, M1)["lag_egress"], 5)
        sw.set_link(4, False)
        used = set()
        for i in (1, 3, 4, 5, 6, 7, 8, 9, 10, 11):
            dst = f"02:00:00:00:00:{i:02x}"
            sw.send_frame(4, dst, M2)
            r = sw.send_frame(1, M2, dst)
            self.assertEqual(r["action"], "forward")
            used.add(r["lag_egress"])
        self.assertEqual(used, {4, 5})                     # 恢复双线分流
        # 学习项全程不动
        self.assertEqual(sw.mac_tables[1][M1], LAG_ID)

    def test_both_links_down_drops_new_frames(self):
        sw = self.sw
        sw.set_link(4, True)
        r2 = sw.set_link(5, True)                          # 防御场景：双断
        self.assertEqual(r2["agg"]["active"], [])
        r = sw.send_frame(1, M2, M1)
        self.assertEqual(r["lag_dropped"], True)
        self.assertIsNone(r["lag_egress"])
        self.assertEqual(r["delivered"], [])
        self.assertEqual(sw.agg.dropped_total, 1)
        self.assertTrue(sw.check_invariants())
        # 恢复一条线：另一条断链口无积压时正常恢复
        sw.set_link(4, False)
        self.assertEqual(sw.send_frame(1, M2, M1)["lag_egress"], 4)


class TestStopDrill(unittest.TestCase):
    def test_stop_purges_agg_entries_and_drops_member_backlog(self):
        sw = Switch()
        sw.start_drill()
        sw.send_frame(4, M1, M2)
        sw.pause(5)
        sw.send_frame(1, M2, M1)                          # 积压在成员口
        queued = sum(len(sw.ports[p].queue) for p in (4, 5))
        r = sw.stop_drill()
        self.assertTrue(r["ok"])
        self.assertEqual(r["dropped_queue"], queued)
        self.assertIn({"vlan": 1, "mac": M1}, r["purged_entries"])
        self.assertNotIn(LAG_ID, sw.mac_tables.get(1, {}).values())
        self.assertIsNone(sw.agg)
        for pid in (4, 5):
            self.assertIsNone(sw.ports[pid].agg_member)
            self.assertEqual(len(sw.ports[pid].queue), 0)
        self.assertTrue(sw.check_invariants())

    def test_stop_without_start_is_noop(self):
        sw = Switch()
        self.assertFalse(sw.stop_drill()["ok"])


if __name__ == "__main__":
    unittest.main()
