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

class TestResumeRecovery(unittest.TestCase):
    def _committed(self, coordinator, txn="t1", nodes=("n1", "n2")):
        coordinator.enlist(txn, list(nodes))
        for node in nodes:
            coordinator.vote(txn, node, True)
        coordinator.decide(txn)
        return txn

    def test_resume_delivers_commit_once(self):
        coordinator = Coordinator()
        txn = self._committed(coordinator, nodes=("n1", "n2", "n3"))
        first = coordinator.resume(txn)
        self.assertEqual(first["delivered"], ["n1", "n2", "n3"])
        coordinator.resume(txn)
        self.assertEqual(coordinator.stats()["duplicates"], 1)

    def test_resume_abort_delivers_nothing(self):
        coordinator = Coordinator()
        coordinator.enlist("t1", ["n1", "n2"])
        coordinator.vote("t1", "n1", True)
        coordinator.decide("t1")
        result = coordinator.resume("t1")
        self.assertEqual(result["decision"], "abort")
        self.assertEqual(result["delivered"], [])

    def test_failed_delivery_goes_to_retry(self):
        coordinator = Coordinator()
        txn = self._committed(coordinator)
        coordinator._send = lambda t, node, decision: node != "n2"
        coordinator.resume(txn)
        self.assertEqual(coordinator.stats()["retry_nodes"], ["n2"])
        coordinator._send = lambda t, node, decision: True
        coordinator.resume(txn)
        self.assertEqual(coordinator.stats()["retry_nodes"], [])

    def test_decide_is_idempotent(self):
        coordinator = Coordinator()
        txn = self._committed(coordinator)
        coordinator.vote(txn, "n1", False)
        self.assertEqual(coordinator.decide(txn)["decision"], "commit")
        self.assertEqual(len(coordinator.wal), 1)

    def test_persist_restore_roundtrip(self):
        coordinator = Coordinator(snapshot_path=None)
        txn = self._committed(coordinator, nodes=("n1", "n2", "n3"))
        coordinator._send = lambda t, node, decision: node != "n3"
        coordinator.resume(txn)
        blob = coordinator.persist()
        restored = Coordinator()
        restored.restore(blob)
        self.assertEqual(restored.decisions, coordinator.decisions)
        self.assertEqual(restored.delivered, coordinator.delivered)
        self.assertEqual(restored.retry, coordinator.retry)

    def test_recover_reports_waiting(self):
        coordinator = Coordinator()
        self._committed(coordinator, txn="done")
        coordinator.resume("done")
        coordinator.enlist("pending", ["n1"])
        recovered = coordinator.recover()
        self.assertEqual(recovered["resolved"], 1)
        self.assertEqual(recovered["waiting"], ["pending"])
