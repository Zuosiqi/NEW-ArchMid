"""消费者独立进程 - 运行在单独进程中的消费者"""

import sys
import time
from datetime import datetime

# 设置stdout编码为UTF-8（Windows兼容）
if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]

from broker_client import Consumer


class LogConsumer:
    """日志消费者"""

    def __init__(self):
        self.name = "log-consumer"
        self.logs = []

    def handle_message(self, message: dict):
        data = message.get("payload", {})
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        order_id = data.get("order_id")
        customer_id = data.get("customer_id")
        total_amount = data.get("total_amount", 0)
        items_count = len(data.get("items", []))

        log_entry = f"[LOG] {timestamp} | 订单创建成功 | 订单号: {order_id} | 客户: {customer_id} | 金额: ¥{total_amount:.2f} | 商品数: {items_count}"
        print(log_entry)
        self.logs.append(log_entry)


class StockConsumer:
    """库存预警消费者"""

    def __init__(self, threshold=10):
        self.name = "stock-consumer"
        self.threshold = threshold
        self.warnings = []

    def handle_message(self, message: dict):
        data = message.get("payload", {})
        order_id = data.get("order_id")
        items = data.get("items", [])

        for item in items:
            product_id = item.get("product_id")
            quantity = item.get("quantity", 0)

            if quantity > self.threshold * 0.5:
                warning = f"[STOCK WARNING] 订单 {order_id}: 商品 {product_id} 订购数量 {quantity} 较大，请关注库存水平"
                print(warning)
                self.warnings.append(warning)


class NotifyConsumer:
    """通知消费者"""

    def __init__(self):
        self.name = "notify-consumer"
        self.notifications = []

    def handle_message(self, message: dict):
        data = message.get("payload", {})
        order_id = data.get("order_id")
        customer_id = data.get("customer_id")
        total_amount = data.get("total_amount", 0)

        notification = f"[NOTIFY] 新订单通知 | 订单号: {order_id} | 客户: {customer_id} | 金额: ¥{total_amount:.2f}"
        print(notification)
        self.notifications.append(notification)

        if total_amount > 1000:
            high_value = f"[NOTIFY] 高价值订单提醒 | 订单号: {order_id} | 金额: ¥{total_amount:.2f}，请重点关注"
            print(high_value)


def main():
    """启动消费者进程"""
    import argparse
    parser = argparse.ArgumentParser(description="消费者进程")
    parser.add_argument("--host", default="127.0.0.1", help="Broker 地址")
    parser.add_argument("--port", type=int, default=9000, help="Broker 端口")
    parser.add_argument("--topic", default="order.created", help="订阅的主题")
    args = parser.parse_args()

    print("=" * 60)
    print("消费者进程启动")
    print(f"连接 Broker: {args.host}:{args.port}")
    print(f"订阅主题: {args.topic}")
    print("=" * 60)

    # 创建三个消费者
    log_consumer = LogConsumer()
    stock_consumer = StockConsumer(threshold=10)
    notify_consumer = NotifyConsumer()

    # 创建三个客户端连接
    log_client = Consumer("log-consumer", broker_host=args.host, broker_port=args.port)
    stock_client = Consumer("stock-consumer", broker_host=args.host, broker_port=args.port)
    notify_client = Consumer("notify-consumer", broker_host=args.host, broker_port=args.port)

    # 连接到 Broker
    if not log_client.connect():
        print("日志消费者连接失败")
        return
    if not stock_client.connect():
        print("库存消费者连接失败")
        return
    if not notify_client.connect():
        print("通知消费者连接失败")
        return

    # 订阅主题
    log_client.subscribe(args.topic, log_consumer.handle_message)
    stock_client.subscribe(args.topic, stock_consumer.handle_message)
    notify_client.subscribe(args.topic, notify_consumer.handle_message)

    print("\n消费者已就绪，等待消息...\n")

    # 保持进程运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n正在关闭消费者...")
        log_client.disconnect()
        stock_client.disconnect()
        notify_client.disconnect()
        print("消费者已关闭")


if __name__ == "__main__":
    main()
