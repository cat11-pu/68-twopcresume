"""twopcresume.py：两阶段提交（决策先落 WAL，恢复期按 WAL 重发，不重新投票）。"""
from __future__ import annotations

import json


class Coordinator:
    def __init__(self, quorum: int = 2, snapshot_path: str = "twopcresume.snapshot.json"):
        self.quorum = quorum
        self.snapshot_path = snapshot_path
        self.participants = {}
        self.votes = {}
        self.decisions = {}
        self.wal = []
        self.delivered = {}
        self.retry = {}
        self.resumed = set()
        self.duplicates = 0
        self.resolved = 0

    def enlist(self, txn: str, nodes) -> dict:
        self.participants[txn] = list(nodes)
        self.votes.setdefault(txn, {})
        return {"participants": len(self.participants[txn])}

    def vote(self, txn: str, node: str, yes: bool) -> dict:
        self.votes.setdefault(txn, {})[node] = bool(yes)
        return {"votes": len(self.votes[txn])}

    def decide(self, txn: str) -> dict:
        """按 quorum 判定；先写 WAL 再生效，同一事务只会有一种决策。"""
        if txn in self.decisions:
            return {"decision": self.decisions[txn]}
        yes = sum(1 for value in self.votes.get(txn, {}).values() if value)
        decision = "commit" if yes >= self.quorum else "abort"
        self.wal.append({"txn": txn, "decision": decision})
        self.decisions[txn] = decision
        if decision == "abort":
            self.resolved += 1
        return {"decision": decision}

    def _send(self, txn: str, node: str, decision: str) -> bool:
        """下发钩子：默认成功，子类可覆盖以注入失败。"""
        return True

    def resume(self, txn: str) -> dict:
        """恢复期处置：只依据 WAL 中的决策重发，绝不重新投票。"""
        decision = self.decisions.get(txn)
        delivered = self.delivered.setdefault(txn, set())
        retry = self.retry.setdefault(txn, set())
        if decision is None:
            return {"txn": txn, "decision": None, "delivered": sorted(delivered),
                    "retry": sorted(retry)}
        if txn in self.resumed and not retry:
            self.duplicates += 1
            return {"txn": txn, "decision": decision, "delivered": sorted(delivered),
                    "retry": sorted(retry)}
        self.resumed.add(txn)
        if decision == "commit":
            for node in self.participants.get(txn, []):
                if node in delivered:
                    continue
                if self._send(txn, node, decision):
                    delivered.add(node)
                    retry.discard(node)
                else:
                    retry.add(node)
            if not retry and all(node in delivered
                                 for node in self.participants.get(txn, [])):
                self.resolved += 1
        return {"txn": txn, "decision": decision, "delivered": sorted(delivered),
                "retry": sorted(retry)}

    def recover(self) -> dict:
        """重启恢复：单趟回放 WAL 重建决策，统计已处置与仍在等待的事务。"""
        decisions = {}
        for entry in self.wal:
            decisions.setdefault(entry["txn"], entry["decision"])
        self.decisions = decisions
        waiting = [txn for txn in self.participants if txn not in decisions]
        resolved = 0
        for txn, decision in decisions.items():
            if decision == "abort":
                resolved += 1
            elif all(node in self.delivered.get(txn, ())
                     for node in self.participants.get(txn, [])):
                resolved += 1
        self.resolved = resolved
        return {"resolved": resolved, "waiting": waiting}

    def persist(self, path: str = None) -> str:
        """快照落盘，同时返回 blob 供 restore 使用。"""
        blob = json.dumps({
            "quorum": self.quorum,
            "participants": {txn: list(nodes) for txn, nodes in self.participants.items()},
            "votes": {txn: dict(v) for txn, v in self.votes.items()},
            "wal": [dict(entry) for entry in self.wal],
            "delivered": {txn: sorted(nodes) for txn, nodes in self.delivered.items()},
            "retry": {txn: sorted(nodes) for txn, nodes in self.retry.items()},
            "resumed": sorted(self.resumed),
            "duplicates": self.duplicates,
            "resolved": self.resolved,
        }, sort_keys=True)
        target = path if path is not None else self.snapshot_path
        if target:
            with open(target, "w", encoding="utf-8") as handle:
                handle.write(blob)
        return blob

    def restore(self, blob) -> dict:
        """从 blob 恢复：决策以 WAL 为准，已下发集合与待重试集合一并还原。"""
        if isinstance(blob, (bytes, bytearray)):
            blob = blob.decode("utf-8")
        data = json.loads(blob) if isinstance(blob, str) else dict(blob)
        self.quorum = data.get("quorum", self.quorum)
        self.participants = {txn: list(nodes)
                             for txn, nodes in data.get("participants", {}).items()}
        self.votes = {txn: dict(v) for txn, v in data.get("votes", {}).items()}
        self.wal = [dict(entry) for entry in data.get("wal", [])]
        self.decisions = {}
        for entry in self.wal:
            self.decisions.setdefault(entry["txn"], entry["decision"])
        self.delivered = {txn: set(nodes)
                          for txn, nodes in data.get("delivered", {}).items()}
        self.retry = {txn: set(nodes)
                      for txn, nodes in data.get("retry", {}).items()}
        self.resumed = set(data.get("resumed", []))
        self.duplicates = data.get("duplicates", 0)
        self.resolved = data.get("resolved", 0)
        return {"restored": len(self.decisions)}

    def stats(self) -> dict:
        return {"txns": len(self.participants), "decisions": dict(self.decisions),
                "resolved": self.resolved, "quorum": self.quorum,
                "duplicates": self.duplicates,
                "retry_nodes": sorted({node for nodes in self.retry.values()
                                       for node in nodes}),
                "delivered": {txn: sorted(nodes)
                              for txn, nodes in self.delivered.items()}}
