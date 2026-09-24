"""twopcresume.py：两阶段提交（基线：决定不落盘，恢复期无处置）。"""
from __future__ import annotations


class Coordinator:
    def __init__(self, quorum: int = 2):
        self.quorum = quorum
        self.participants = {}
        self.votes = {}
        self.decisions = {}
        self.wal = []
        self.resolved = 0

    def enlist(self, txn: str, nodes) -> dict:
        self.participants[txn] = list(nodes)
        self.votes[txn] = {}
        return {"participants": len(self.participants[txn])}

    def vote(self, txn: str, node: str, yes: bool) -> dict:
        self.votes[txn][node] = yes
        return {"votes": len(self.votes[txn])}

    def decide(self, txn: str) -> dict:
        """基线：只做本地决定，不写日志、不下发。"""
        yes = len([1 for value in self.votes.get(txn, {}).values() if value])
        decision = "commit" if yes >= self.quorum else "abort"
        self.decisions[txn] = decision
        return {"decision": decision}

    def resume(self, txn: str) -> dict:
        raise NotImplementedError("恢复期处置还没实现")

    def recover(self) -> dict:
        raise NotImplementedError("重启恢复还没实现")

    def stats(self) -> dict:
        return {"txns": len(self.participants), "decisions": dict(self.decisions),
                "resolved": self.resolved, "quorum": self.quorum}
