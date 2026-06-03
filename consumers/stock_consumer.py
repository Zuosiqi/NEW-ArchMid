"""库存消费者 - 检查低库存预警"""


class StockConsumer:
    """库存消费者：订阅 order.created 事件，检查库存预警"""

    def __init__(self, threshold=10):
        """
        初始化库存消费者

        Args:
            threshold: 库存预警阈值，低于此值触发预警
        """
        self.name = "stock-consumer"
        self.threshold = threshold
        self.warnings = []  # 存储预警记录

    def handle_order_created(self, message):
        """
        处理订单创建事件，检查库存预警

        Args:
            message: 消息对象，payload 包含订单信息
        """
        data = message.payload
        order_id = data.get("order_id")
        items = data.get("items", [])

        for item in items:
            product_id = item.get("product_id")
            quantity = item.get("quantity", 0)

            # 模拟检查：如果单次购买数量超过阈值的50%，发出预警
            # 实际应用中应该查询数据库获取当前库存
            if quantity > self.threshold * 0.5:
                warning = {
                    "order_id": order_id,
                    "product_id": product_id,
                    "ordered_quantity": quantity,
                    "threshold": self.threshold,
                    "message": f"商品 {product_id} 本次订购 {quantity} 件，库存可能偏低",
                }
                self.warnings.append(warning)
                print(f"[STOCK WARNING] 订单 {order_id}: 商品 {product_id} "
                      f"订购数量 {quantity} 较大，请关注库存水平")

    def get_warnings(self, limit=100):
        """获取最近的预警记录"""
        return self.warnings[-limit:]
