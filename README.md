# Local Data Pipeline

本地 JSON 消息归档的增量处理工具。提供 SQLite 落库、重复导入去重、内容变更记录、来源哈希、事务回滚、增量摘录及只读分页。

## 快速开始

Python 3.9+，macOS / Linux，标准库即可。Windows 原生暂未支持（使用 fcntl 文件锁）。

```sh
python3 run_once.py --source examples/input --root runtime/demo
python3 core/pipeline.py read --root runtime/demo --metadata-only
python3 -m unittest discover -s tests -v
```

示例完全为虚构数据。输入为 32 位十六进制会话 ID 命名的 JSON，字段见 examples/input。每个文件最多 16 MiB、20000 条消息。createTime 为 Unix 秒数，serverId 或 id 用于标识消息。不同来源使用不同 LOCAL_DATA_NAMESPACE，不能在同一个工作目录内改变来源命名空间。

## 行为与边界

源文件只读；输出集中在 --root 指定目录。摘要为逐条截取，未使用模型推理。更改内容会产生新事件，不抹去既有变更。缺失源文件不代表删除消息。恢复发布可运行 core/pipeline.py publish --root runtime/demo。

此版抽出可复用的数据处理核心，不包含任何聊天记录、账号凭据、原有采集器或针对个人设备的状态适配器。输入来源的登录状态、历史完整性和下游真实读取均不由本工具证明。运行结果可能含输入正文，请保存在本机。

## 开发状态

0.1.0 预发布；已提供事务、重放、晚到消息及崩溃恢复测试。长期运行和多平台体验仍需真实用户反馈。
