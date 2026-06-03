# 简易消息中间件系统（多进程版）

## 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        多进程架构                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   进程1: Broker Server (broker_server.py:9000)                  │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │  Socket Server - 消息路由中心                            │  │
│   │  ├── 接收 Producer 的消息                                │  │
│   │  ├── 管理 Topic 和订阅关系                               │  │
│   │  └── 推送消息给 Consumer                                 │  │
│   └─────────────────────────────────────────────────────────┘  │
│                          ▲                    │                  │
│                          │ Socket             │ Socket           │
│                          │                    ▼                  │
│   进程2: 超市系统         │      进程3: 消费者进程               │
│   ┌─────────────────────────────────┐   ┌─────────────────────┐│
│   │  app_web.py (:8000)             │   │  consumer_process.py││
│   │  └── 创建订单 ──────────────────┼──▶│  ├── LogConsumer    ││
│   │      发送消息到 Broker           │   │  ├── StockConsumer  ││
│   └─────────────────────────────────┘   │  └── NotifyConsumer ││
│                                          └─────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## 通信方式

- **协议**: TCP Socket + JSON
- **消息格式**: 长度前缀(4字节) + JSON 消息体
- **进程间通信**: 真正的网络通信，非进程内调用

## 快速开始

### 方式一：多进程分别启动（推荐）

```bash
# 终端1: 启动 Broker Server (消息路由中心)
python broker_server.py

# 终端2: 启动消费者进程
python consumer_process.py

# 终端3: 启动超市系统
python app_web.py
```

### 方式二：使用默认配置

- Broker Server: `127.0.0.1:9000`
- 超市系统: `127.0.0.1:8000`
- 消息中间件 Web 界面: `127.0.0.1:8001`

## 文件说明

| 文件 | 说明 |
|------|------|
| `broker_server.py` | Broker Server - 独立进程，Socket 服务端 |
| `broker_client.py` | Broker Client - Producer/Consumer 的 Socket 客户端封装 |
| `consumer_process.py` | 消费者独立进程 - 运行三个业务消费者 |
| `app_web.py` | 超市系统 - Flask Web 应用 |
| `broker/` | 消息中间件核心库（供参考） |
| `main.py` | 消息中间件独立 Web 演示界面（可选） |

## 核心设计

### 1. 观察者模式（发布/订阅）

```python
# Producer 发布消息
producer.publish("order.created", order_data)

# Consumer 订阅主题
consumer.subscribe("order.created", callback_function)
```

### 2. 多进程通信

```python
# Broker Server (broker_server.py)
# - 监听端口 9000
# - 接收 Producer 的消息
# - 推送给订阅的 Consumer

# Producer (broker_client.py)
# - Socket 连接到 Broker
# - 发送消息

# Consumer (broker_client.py)
# - Socket 连接到 Broker
# - 订阅主题，接收推送
```

### 3. 消息协议

```json
{
  "action": "publish",
  "topic": "order.created",
  "payload": {
    "order_id": 1001,
    "customer_id": 1,
    "total_amount": 150.00
  }
}
```

## 测试

```bash
# 运行单元测试
python -m pytest tests/test_broker.py -v

# 运行性能测试
python tests/test_performance.py

# 运行集成演示（单进程版）
python tests/test_supermarket.py
```

## 性能数据

| 指标 | 数值 |
|------|------|
| 队列模式发送吞吐率 | 170,441 条/秒 |
| 主题模式发送吞吐率 | 13,855 条/秒 |
| 消息平均延迟 | 0.017 ms |
| 并发吞吐率 (3P→3C) | 690,458 条/秒 |
