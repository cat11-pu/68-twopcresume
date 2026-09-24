"""check_http.py：起服务、按脚本走一圈，打印验收面。"""
import json
import sys
import threading
import urllib.error
import urllib.request

from server import serve


def call(method, url, body=None):
    request = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def parse(text):
    try:
        return json.loads(text)
    except Exception:
        return {"_raw": (text or "")[:60]}


def main() -> int:
    spec = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "sample/txns.json", encoding="utf-8"))
    server = serve(0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = "http://127.0.0.1:%d" % server.server_port
    for step in spec["ops"]:
        call("POST", base + "/" + step["op"], json.dumps(step).encode())
    resumed = [parse(call("POST", base + "/resume", json.dumps({"txn": txn}).encode())[1])
               for txn in spec["resume_txns"]]
    stats = parse(call("GET", base + "/")[1])
    recovered = parse(call("POST", base + "/recover", b"{}")[1])
    print("各事务的处置 =", [(item.get("txn"), item.get("decision"), item.get("delivered")) for item in resumed])
    print("已决定的决策 =", stats.get("decisions"))
    print("重复下发被拦下的次数 =", stats.get("duplicates"))
    print("下发失败待重试的节点 =", stats.get("retry_nodes"))
    print("恢复后的已处置事务数 =", recovered.get("resolved"))
    print("恢复后仍在等待的事务 =", recovered.get("waiting"))
    print("多数派 =", stats.get("quorum"))
    print("不变量（同一事务不得下发两种决策） =", spec["single_decision_invariant"])
    print("事务数 =", spec["txn_count"])
    server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
