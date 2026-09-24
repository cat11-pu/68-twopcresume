import json
import threading
import unittest
import urllib.error
import urllib.request

from twopcresume import Coordinator
from server import serve

class TestCoordinator(unittest.TestCase):
    def test_enlist_counts(self):
        coordinator = Coordinator()
        self.assertEqual(coordinator.enlist("t1", ["n1", "n2"])["participants"], 2)

    def test_vote_counts(self):
        coordinator = Coordinator()
        coordinator.enlist("t1", ["n1", "n2"])
        self.assertEqual(coordinator.vote("t1", "n1", True)["votes"], 1)

    def test_commit_on_quorum(self):
        coordinator = Coordinator()
        coordinator.enlist("t1", ["n1", "n2"])
        coordinator.vote("t1", "n1", True)
        coordinator.vote("t1", "n2", True)
        self.assertEqual(coordinator.decide("t1")["decision"], "commit")

    def test_stats_shape(self):
        self.assertIn("quorum", Coordinator().stats())

    def test_http_vote_decide(self):
        server = serve(0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = "http://127.0.0.1:%d" % server.server_port
        urllib.request.urlopen(base + "/enlist", data=b'{"txn": "t1", "nodes": ["n1", "n2"]}', timeout=5).read()
        urllib.request.urlopen(base + "/vote", data=b'{"txn": "t1", "node": "n1", "yes": true}', timeout=5).read()
        with urllib.request.urlopen(base + "/decide", data=b'{"txn": "t1"}', timeout=5) as response:
            self.assertEqual(json.loads(response.read())["decision"], "abort")
        server.shutdown()
