# twopcresume

纯 Python 标准库的本机两阶段提交协调服务：决策先落 WAL 再下发，崩溃重启后
`recover` 单趟回放 WAL，`resume` 只按 WAL 中的决策向参与者重发（不重新投票），
重复下发会被拦下并计入 `duplicates`，下发失败的节点进入 `retry_nodes` 待重试。

## 起服务

    python3 server.py 8000

浏览器打开 http://127.0.0.1:8000/ 看结果。

## 接口

- `POST /enlist {"txn", "nodes"}` 登记参与者
- `POST /vote {"txn", "node", "yes"}` 收集投票
- `POST /decide {"txn"}` 按 quorum 判定并写 WAL（幂等，不会翻案）
- `POST /resume {"txn"}` 按 WAL 决策重发，返回已下发参与者列表
- `POST /recover` 重启恢复，返回已处置数与仍在等待的事务
- `GET /` 状态（决策、duplicates、retry_nodes、已下发集合）

内核侧另有 `persist()`（快照落盘并返回 blob）与 `restore(blob)`（还原决策、
已下发集合与待重试集合）。

## 测试

    python3 -m unittest discover -s tests -v

## 验收自检

    python3 check_http.py
