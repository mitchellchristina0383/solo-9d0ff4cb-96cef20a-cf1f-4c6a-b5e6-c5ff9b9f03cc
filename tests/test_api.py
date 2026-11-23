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


if __name__ == "__main__":
    unittest.main()
