"""通知消费者 - 模拟通知管理员"""


class NotifyConsumer:
    """通知消费者：订阅 order.created 事件，模拟发送通知"""

    def __init__(self):
        self.name = "notify-consumer"
        self.notifications = []  # 存储通知记录

    def handle_order_created(self, message):
        """
        处理订单创建事件，模拟发送通知

        Args:
            message: 消息对象，payload 包含订单信息
        """
        data = message.payload
        order_id = data.get("order_id")
        customer_id = data.get("customer_id")
        total_amount = data.get("total_amount", 0)

        notification = {
            "type": "order_created",
            "order_id": order_id,
            "customer_id": customer_id,
            "total_amount": total_amount,
            "message": f"新订单通知：订单号 {order_id}，客户 {customer_id}，金额 ¥{total_amount:.2f}",
        }

        self.notifications.append(notification)

        # 模拟发送通知（实际应用中可以发送邮件、短信、推送等）
        print(f"[NOTIFY] 新订单通知 | 订单号: {order_id} | "
              f"客户: {customer_id} | 金额: ¥{total_amount:.2f}")

        # 如果金额超过1000元，发送高价值订单提醒
        if total_amount > 1000:
            print(f"[NOTIFY] 高价值订单提醒 | 订单号: {order_id} | "
                  f"金额: ¥{total_amount:.2f}，请重点关注")

    def get_notifications(self, limit=100):
        """获取最近的通知记录"""
        return self.notifications[-limit:]
