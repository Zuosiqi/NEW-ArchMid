"""日志消费者 - 记录订单操作日志"""

import time
from datetime import datetime


class LogConsumer:
    """日志消费者：订阅 order.created 事件，记录操作日志"""

    def __init__(self):
        self.name = "log-consumer"
        self.logs = []  # 存储日志记录

    def handle_order_created(self, message):
        """
        处理订单创建事件

        Args:
            message: 消息对象，payload 包含订单信息
        """
        data = message.payload
        order_id = data.get("order_id")
        customer_id = data.get("customer_id")
        total_amount = data.get("total_amount", 0)
        items_count = len(data.get("items", []))

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": timestamp,
            "event": "order.created",
            "order_id": order_id,
            "customer_id": customer_id,
            "total_amount": total_amount,
            "items_count": items_count,
        }

        self.logs.append(log_entry)

        # 输出到控制台
        print(f"[LOG] {timestamp} | 订单创建成功 | 订单号: {order_id} | "
              f"客户: {customer_id} | 金额: ¥{total_amount:.2f} | 商品数: {items_count}")

    def get_logs(self, limit=100):
        """获取最近的日志记录"""
        return self.logs[-limit:]
