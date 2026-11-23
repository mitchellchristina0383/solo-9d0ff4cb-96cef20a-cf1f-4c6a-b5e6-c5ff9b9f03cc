"""HTTP API 层测试（Flask test client）。"""

import unittest

from app import create_app
from switch import Switch

M1 = "02:00:00:00:00:01"
M2 = "02:00:00:00:00:02"


class TestApi(unittest.TestCase):
    def setUp(self):
        self.sw = Switch()
        self.client = create_app(self.sw).test_client()

    def post(self, path, json=None):
        return self.client.post(path, json=json)

    def test_index_page_served(self):
        r = self.client.get("/")
        self.assertEqual(r.status_code, 200)
        self.assertIn("虚拟交换机".encode(), r.data)

    def test_frame_flow_over_http(self):
        r = self.post("/api/frame", {"port": 1, "src": M1, "dst": M2, "payload": "hi"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["action"], "flood")
        st = self.client.get("/api/state").get_json()
        self.assertEqual(st["mac_tables"]["1"][M1], 1)
        self.assertEqual(st["ports"][1]["received_count"], 1)   # 端口2收到泛洪帧

    def test_pause_release_over_http(self):
        self.post("/api/port/2/pause")
        self.post("/api/frame", {"port": 1, "src": M1, "dst": M2})
        st = self.client.get("/api/state").get_json()
        self.assertEqual(st["ports"][1]["queued"], 1)
        r = self.post("/api/port/2/release")
        self.assertEqual(r.get_json()["delivered"], 1)

    def test_vlan_reconfig_over_http(self):
        self.post("/api/port/2/pause")
        self.post("/api/frame", {"port": 2, "src": M2, "dst": M1})
        self.post("/api/frame", {"port": 1, "src": M1, "dst": M2})  # 积压于端口2
        r = self.post("/api/port/2/vlan", {"vlan": 20})
        j = r.get_json()
        self.assertTrue(j["changed"])
        self.assertEqual(j["dropped_queue"], 1)
        self.assertEqual(j["purged_entries"], [{"vlan": 1, "mac": M2}])

    def test_bad_requests_return_400(self):
        self.assertEqual(self.post("/api/frame", {"port": 1, "src": "xx", "dst": M2}).status_code, 400)
        self.assertEqual(self.post("/api/frame", {"port": 1, "src": M1, "dst": "ff:ff:ff:ff:ff:ff"}).status_code, 400)
        self.assertEqual(self.post("/api/port/6/pause").status_code, 400)
        self.assertEqual(self.post("/api/port/1/vlan", {"vlan": 0}).status_code, 400)
        self.assertEqual(self.post("/api/frame", {}).status_code, 400)

    def test_reset(self):
        self.post("/api/frame", {"port": 1, "src": M1, "dst": M2})
        self.post("/api/reset")
        st = self.client.get("/api/state").get_json()
        self.assertEqual(st["mac_tables"], {})
        self.assertTrue(all(p["received_count"] == 0 for p in st["ports"]))

    def test_drill_lifecycle_over_http(self):
        r = self.post("/api/drill/start")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["agg"]["members"], [4, 5])
        st = self.client.get("/api/state").get_json()
        self.assertIsNotNone(st["agg"])
        for pid in (4, 5):
            p = next(x for x in st["ports"] if x["id"] == pid)
            self.assertEqual(p["agg_member"], "lag1")
            self.assertFalse(p["link_down"])

        # 成员口改配 VLAN 被拒（固定 VLAN 1）
        self.assertEqual(self.post("/api/port/4/vlan", {"vlan": 20}).status_code, 400)
        # 非成员口仍可改配
        self.assertEqual(self.post("/api/port/3/vlan", {"vlan": 20}).status_code, 200)

    def test_drill_learning_split_and_link_down_over_http(self):
        M3, M4 = "02:00:00:00:00:03", "02:00:00:00:00:04"
        self.post("/api/drill/start")
        # 从成员口 4 注入，源 MAC 学到聚合口 lag1
        self.post("/api/frame", {"port": 4, "src": M3, "dst": M1})
        st = self.client.get("/api/state").get_json()
        self.assertEqual(st["mac_tables"]["1"][M3], "lag1")

        # 已知目的发往聚合口：只在一个成员口实收（聚合口一份）
        r = self.post("/api/frame", {"port": 1, "src": M1, "dst": M3}).get_json()
        self.assertEqual(r["action"], "forward")
        self.assertIn(r["lag_egress"], (4, 5))
        self.assertEqual(len(r["delivered"]), 1)

        # 暂停两个成员，压出双线积压，再断 4：积压并入存活成员 5
        self.post("/api/port/4/pause")
        self.post("/api/port/5/pause")
        self.post("/api/frame", {"port": 4, "src": M4, "dst": M1})
        for _ in range(3):
            self.post("/api/frame", {"port": 1, "src": M1, "dst": M3})
            self.post("/api/frame", {"port": 1, "src": M1, "dst": M4})
        st = self.client.get("/api/state").get_json()
        q4 = next(p for p in st["ports"] if p["id"] == 4)["queued"]
        q5 = next(p for p in st["ports"] if p["id"] == 5)["queued"]
        self.assertGreater(q4, 0)
        self.assertGreater(q5, 0)
        r = self.post("/api/port/4/link", {"down": True}).get_json()
        self.assertEqual(r["merged"], q4)
        self.assertEqual(r["flushed"], 0)               # 存活成员暂停中，继续等
        st = self.client.get("/api/state").get_json()
        p4 = next(p for p in st["ports"] if p["id"] == 4)
        p5 = next(p for p in st["ports"] if p["id"] == 5)
        self.assertTrue(p4["link_down"])
        self.assertEqual(p4["queued"], 0)
        self.assertEqual(p5["queued"], q4 + q5)
        # 学习项仍指向聚合口
        self.assertEqual(st["mac_tables"]["1"][M3], "lag1")
        # 新帧只用存活成员
        r = self.post("/api/frame", {"port": 1, "src": M1, "dst": M3}).get_json()
        self.assertEqual(r["lag_egress"], 5)
        self.assertEqual(r["queued"], [5])
        # 放行后按帧号 FIFO 全部交付
        rel = self.post("/api/port/5/release").get_json()
        self.assertEqual(rel["delivered"], q4 + q5 + 1)

    def test_link_endpoint_rejects_non_member(self):
        # 未开始演练时任何口都不能断链
        self.assertEqual(self.post("/api/port/1/link", {"down": True}).status_code, 400)
        self.post("/api/drill/start")
        self.assertEqual(self.post("/api/port/3/link", {"down": True}).status_code, 400)
        # 断链口不能再暂停/放行/注入
        self.post("/api/port/4/link", {"down": True})
        self.assertEqual(self.post("/api/port/4/pause").status_code, 400)
        self.assertEqual(
            self.post("/api/frame", {"port": 4, "src": M1, "dst": M2}).status_code, 400)

    def test_drill_stop_over_http(self):
        self.post("/api/drill/start")
        self.post("/api/frame", {"port": 4, "src": M1, "dst": M2})
        r = self.post("/api/drill/stop").get_json()
        self.assertTrue(r["ok"])
        self.assertIn({"vlan": 1, "mac": M1}, r["purged_entries"])
        st = self.client.get("/api/state").get_json()
        self.assertIsNone(st["agg"])
        self.assertIsNone(next(p for p in st["ports"] if p["id"] == 4)["agg_member"])


if __name__ == "__main__":
    unittest.main()
