# 简易消息中间件系统

## 项目简介

本项目是一个简易的消息中间件系统，基于观察者模式实现发布/订阅功能，支持异步通信和系统解耦。

## 功能特性

- **点对点模式** - 消息通过队列传递，保证单消费者处理
- **发布/订阅模式** - 消息通过主题广播，支持多订阅者
- **观察者模式** - 实现松耦合的事件驱动架构
- **线程安全** - 支持多线程并发访问
- **统计信息** - 实时监控系统运行状态
- **Web界面** - 可视化操作和监控

## 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      简易消息中间件架构                           │
├─────────────────────────────────────────────────────────────────┤
│   ┌──────────┐      ┌──────────┐      ┌──────────┐            │
│   │ 生产者1  │──┐   │ 生产者2  │──┐   │ 生产者N  │──┐         │
│   └──────────┘  │   └──────────┘  │   └──────────┘  │         │
│                 ▼                 ▼                 ▼         │
│   ┌─────────────────────────────────────────────────────┐     │
│   │                    消息代理 (Broker)                  │     │
│   │  ┌─────────────────────────────────────────────┐   │     │
│   │  │              主题管理器 (TopicManager)        │   │     │
│   │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐   │   │     │
│   │  │  │ 主题 A  │  │ 主题 B  │  │ 主题 C  │   │   │     │
│   │  │  └────┬────┘  └────┬────┘  └────┬────┘   │   │     │
│   │  │       ▼            ▼            ▼         │   │     │
│   │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐   │   │     │
│   │  │  │ 队列 A  │  │ 队列 B  │  │ 队列 C  │   │   │     │
│   │  │  └─────────┘  └─────────┘  └─────────┘   │   │     │
│   │  └───────────────────────────────────────────┘   │     │
│   └─────────────────────────────────────────────────┘     │
│                 ▼                 ▼                 ▼         │
│   ┌──────────┐      ┌──────────┐      ┌──────────┐            │
│   │ 消费者1  │      │ 消费者2  │      │ 消费者N  │            │
│   └──────────┘      └──────────┘      └──────────┘            │
└─────────────────────────────────────────────────────────────────┘
```

## 目录结构

```
ArchMidLab/lab1/
├── broker/                    # 消息代理模块
│   ├── __init__.py
│   ├── broker.py              # Broker核心类（单例模式）
│   ├── queue.py               # 消息队列实现（FIFO）
│   └── topic.py               # 主题管理（观察者模式）
├── producer/                  # 消息生产者模块
│   ├── __init__.py
│   └── producer.py            # Producer类
├── consumer/                  # 消息消费者模块
│   ├── __init__.py
│   └── consumer.py            # Consumer类
├── models/                    # 数据模型
│   ├── __init__.py
│   └── message.py             # 消息模型
├── webapp/                    # Web演示界面
│   ├── __init__.py
│   ├── app.py                 # Flask应用
│   ├── static/                # 静态资源
│   └── templates/             # HTML模板
│       ├── base.html          # 基础模板
│       ├── index.html         # 首页
│       ├── producer.html      # 生产者页面
│       ├── consumer.html      # 消费者页面
│       └── queue.html         # 队列监控页面
├── tests/                     # 测试代码
│   ├── test_broker.py         # 单元测试
│   ├── test_performance.py    # 性能测试
│   └── test_supermarket.py    # 超市系统集成演示
├── config.py                  # 配置文件
├── main.py                    # 主程序入口
└── requirements.txt           # 依赖
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动Web服务

```bash
python main.py
```

浏览器访问：`http://127.0.0.1:8000/`

### 3. 运行测试

```bash
# 运行单元测试
python -m pytest tests/test_broker.py -v

# 运行性能测试
python tests/test_performance.py

# 运行超市系统集成演示
python tests/test_supermarket.py
```

## Web界面说明

- **首页** (`/`) - 系统概览和架构说明
- **生产者** (`/producer`) - 创建生产者并发送消息
- **消费者** (`/consumer`) - 创建消费者并接收消息
- **队列监控** (`/queue`) - 查看队列状态和消息内容

## 设计模式

### 1. 观察者模式（发布/订阅）

```python
# 主题（Subject）
class Topic:
    def subscribe(self, subscriber_id, callback): ...
    def unsubscribe(self, subscriber_id): ...
    def publish(self, message): ...  # 通知所有订阅者

# 订阅者（Observer）
class Subscriber:
    def on_message(self, message): ...
```

### 2. 单例模式（Broker）

```python
class Broker:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
```

## API接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/producer/create` | POST | 创建生产者 |
| `/api/producer/send` | POST | 发送消息 |
| `/api/consumer/create` | POST | 创建消费者 |
| `/api/consumer/subscribe` | POST | 订阅主题 |
| `/api/consumer/poll` | POST | 从队列拉取消息 |
| `/api/consumer/messages/<id>` | GET | 获取消费者消息 |
| `/api/queue/<name>/messages` | GET | 获取队列消息 |
| `/api/stats` | GET | 获取系统统计 |

## 实际应用场景

### 超市商品管理系统

1. **库存预警** - 商品库存低于阈值时自动通知
2. **订单处理** - 新订单创建后异步处理
3. **销售统计** - 每笔交易完成后异步更新统计
4. **日志记录** - 系统操作日志异步写入

## 技术栈

- Python 3
- Flask（Web框架）
- HTML/CSS/JavaScript（前端）
