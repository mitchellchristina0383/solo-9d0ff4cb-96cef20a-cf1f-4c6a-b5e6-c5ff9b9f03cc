# 五端口虚拟交换机实验台

Flask 实现的 5 端口 VLAN 感知学习型交换机，供值班员在实验台切换接入网段时
核对积压帧去向。

## 功能

- 网页改配任意端口 VLAN（1–4094）
- 发送带源/目的 MAC 的单播帧（非法 MAC、非单播目的会被拒绝）
- 每端口独立「暂停出帧 / 放行出帧」：暂停期间发往该口的帧进入积压队列，
  放行时按 FIFO 全部交付
- 查看各端口已收到的帧、当前积压队列、事件日志
- 各 VLAN 独立学习源 MAC；已知目的按表转发，未知目的只泛洪到本 VLAN 其他端口
- 带积压改配：改配生效瞬间，指向该口的学习项（跨所有 VLAN）与该口待发队列
  同时失效；切回原 VLAN 也按空队列处理；其他端口积压照常交付
- 同一端口的改配与放行可并发发起：核心模型用一把可重入锁把所有操作串行化，
  每个操作原子生效

## 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

### 本地直接运行

```bash
pip install -r requirements.txt
python app.py          # http://localhost:8000
```

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 网页控制台 |
| GET | `/api/state` | 全部状态（端口、MAC 表、日志） |
| POST | `/api/frame` | 发帧 `{port, src, dst, payload}` |
| POST | `/api/port/<id>/vlan` | 改配 VLAN `{vlan}` |
| POST | `/api/port/<id>/pause` | 暂停出帧 |
| POST | `/api/port/<id>/release` | 放行出帧（交付积压） |
| POST | `/api/reset` | 复位整机 |

错误输入返回 `400 {"error": ...}`。

## 设计要点

- `switch.py` 为纯模型：`Switch` 持有一把 `threading.RLock`，
  `send_frame / set_vlan / pause / release / state` 全部在锁内完成，
  因此改配与放行并发时只是被串行化，不会交错破坏状态。
- `set_vlan` 在同一临界区内依次：改端口 VLAN → 删除所有 VLAN 表中指向
  本口的学习项 → 清空本口积压队列。三者对观察者原子。
- 学习表按 VLAN 分表（`{vlan: {mac: port}}`），同一 MAC 在不同 VLAN 独立学习；
  同一 MAC 换端口出现则迁移表项。
- 不变式（`Switch.check_invariants`，测试与冒烟均断言）：
  学习项必指向当前处于该 VLAN 的端口；有积压的端口必处于暂停态；
  积压帧的 VLAN 必等于端口当前 VLAN。

## 测试

```bash
python -m unittest discover -s tests -v   # 单元测试（模型 + API）
python scripts/smoke_http.py              # 起真实服务跑端到端场景
```

## 已知限制

- 使用 Flask 内置开发服务器（threaded），未接 gunicorn 等生产级 WSGI
- 状态不持久化，进程重启即丢；无认证，仅限实验室使用
- 只支持单播帧（广播/组播目的被拒绝）；无 STP、无端口镜像等
- 积压队列无长度上限；每端口已收帧历史保留最近 500 条
