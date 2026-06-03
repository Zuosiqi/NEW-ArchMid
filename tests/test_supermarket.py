"""超市商品管理系统集成演示 - 展示消息中间件在实际系统中的应用"""

import sys
import os
import time

# 设置stdout编码为UTF-8（Windows兼容）
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.broker import Broker
from producer.producer import Producer
from consumer.consumer import Consumer
from models.message import Message


class SupermarketDemo:
    """超市系统消息中间件集成演示

    应用场景：
    1. 库存预警 - 商品库存低于阈值时自动通知
    2. 订单处理 - 新订单创建后异步处理
    3. 销售统计 - 每笔交易完成后异步更新统计
    4. 日志记录 - 系统操作日志异步写入
    """

    def __init__(self):
        """初始化演示"""
        Broker.reset_instance()
        self.broker = Broker()
        self.setup_topics()
        self.setup_services()

    def setup_topics(self):
        """创建主题"""
        # 库存预警主题
        self.broker.create_topic("inventory-alert")
        # 订单事件主题
        self.broker.create_topic("order-events")
        # 销售统计主题
        self.broker.create_topic("sales-events")
        # 系统日志主题
        self.broker.create_topic("system-logs")

        print("✓ 主题创建完成")
        print("  - inventory-alert: 库存预警")
        print("  - order-events: 订单事件")
        print("  - sales-events: 销售事件")
        print("  - system-logs: 系统日志")

    def setup_services(self):
        """创建服务（生产者和消费者）"""

        # ==================== 生产者 ====================

        # 库存服务 - 监控库存变化
        self.inventory_service = Producer("inventory-service")
        self.inventory_service.connect(self.broker)

        # 订单服务 - 处理订单
        self.order_service = Producer("order-service")
        self.order_service.connect(self.broker)

        # 支付服务 - 处理支付
        self.payment_service = Producer("payment-service")
        self.payment_service.connect(self.broker)

        # ==================== 消费者 ====================

        # 预警服务 - 处理库存预警
        self.alert_service = Consumer("alert-service")
        self.alert_service.connect(self.broker)
        self.alert_service.subscribe("inventory-alert", self.handle_inventory_alert)

        # 库存扣减服务 - 处理订单库存扣减
        self.stock_deduction = Consumer("stock-deduction")
        self.stock_deduction.connect(self.broker)
        self.stock_deduction.subscribe("order-events", self.handle_order_stock)

        # 统计服务 - 更新销售统计
        self.stats_service = Consumer("stats-service")
        self.stats_service.connect(self.broker)
        self.stats_service.subscribe("sales-events", self.handle_sales_stats)
        self.stats_service.subscribe("order-events", self.handle_order_stats)

        # 日志服务 - 记录系统日志
        self.log_service = Consumer("log-service")
        self.log_service.connect(self.broker)
        self.log_service.subscribe("system-logs", self.handle_system_log)

        print("\n✓ 服务创建完成")
        print("  生产者: inventory-service, order-service, payment-service")
        print("  消费者: alert-service, stock-deduction, stats-service, log-service")

    # ==================== 消息处理函数 ====================

    def handle_inventory_alert(self, message):
        """处理库存预警"""
        data = message.payload
        print(f"  ⚠️  [预警服务] 商品 {data['product_name']} 库存不足!")
        print(f"      当前库存: {data['current_stock']}, 预警阈值: {data['threshold']}")

    def handle_order_stock(self, message):
        """处理订单库存扣减"""
        data = message.payload
        print(f"  📦 [库存扣减] 订单 {data['order_id']} - 扣减商品库存")
        for item in data.get('items', []):
            print(f"      - {item['product_name']}: {item['quantity']} 件")

    def handle_sales_stats(self, message):
        """处理销售统计"""
        data = message.payload
        print(f"  📊 [统计服务] 更新销售统计 - 订单 {data['order_id']}")
        print(f"      金额: ¥{data['amount']:.2f}")

    def handle_order_stats(self, message):
        """处理订单统计"""
        data = message.payload
        if data.get('event') == 'created':
            print(f"  📈 [统计服务] 新订单创建 - {data['order_id']}")

    def handle_system_log(self, message):
        """处理系统日志"""
        data = message.payload
        print(f"  📝 [日志服务] {data['level']} - {data['message']}")

    # ==================== 演示场景 ====================

    def demo_inventory_alert(self):
        """演示场景1: 库存预警"""
        print("\n" + "=" * 60)
        print("场景1: 库存预警")
        print("=" * 60)
        print("当商品库存低于阈值时，自动触发预警通知\n")

        # 模拟库存检查
        products = [
            {"product_id": "P001", "product_name": "可口可乐", "current_stock": 5, "threshold": 10},
            {"product_id": "P002", "product_name": "薯片", "current_stock": 3, "threshold": 15},
            {"product_id": "P003", "product_name": "矿泉水", "current_stock": 20, "threshold": 10},
        ]

        for product in products:
            if product["current_stock"] < product["threshold"]:
                # 发布库存预警消息
                self.inventory_service.publish_to_topic("inventory-alert", product)
                # 记录日志
                self.inventory_service.publish_to_topic("system-logs", {
                    "level": "WARNING",
                    "message": f"库存预警: {product['product_name']} 库存 {product['current_stock']}",
                    "timestamp": time.time(),
                })

    def demo_order_processing(self):
        """演示场景2: 订单处理"""
        print("\n" + "=" * 60)
        print("场景2: 订单异步处理")
        print("=" * 60)
        print("新订单创建后，异步触发库存扣减和统计更新\n")

        # 模拟新订单
        order = {
            "order_id": "ORD-20260603-001",
            "event": "created",
            "customer": "张三",
            "items": [
                {"product_id": "P001", "product_name": "可口可乐", "quantity": 2, "price": 3.5},
                {"product_id": "P002", "product_name": "薯片", "quantity": 1, "price": 8.0},
            ],
            "amount": 15.0,
            "timestamp": time.time(),
        }

        # 发布订单事件
        self.order_service.publish_to_topic("order-events", order)
        # 发布销售事件
        self.order_service.publish_to_topic("sales-events", order)
        # 记录日志
        self.order_service.publish_to_topic("system-logs", {
            "level": "INFO",
            "message": f"新订单创建: {order['order_id']}",
            "timestamp": time.time(),
        })

    def demo_payment_processing(self):
        """演示场景3: 支付处理"""
        print("\n" + "=" * 60)
        print("场景3: 支付处理")
        print("=" * 60)
        print("支付完成后，触发订单状态更新和统计\n")

        payment = {
            "order_id": "ORD-20260603-001",
            "event": "paid",
            "payment_method": "微信支付",
            "amount": 15.0,
            "timestamp": time.time(),
        }

        # 发布支付事件
        self.payment_service.publish_to_topic("order-events", payment)
        # 记录日志
        self.payment_service.publish_to_topic("system-logs", {
            "level": "INFO",
            "message": f"订单支付成功: {payment['order_id']}",
            "timestamp": time.time(),
        })

    def demo_decoupling_analysis(self):
        """演示场景4: 解耦分析"""
        print("\n" + "=" * 60)
        print("场景4: 解耦效果分析")
        print("=" * 60)

        print("""
消息中间件带来的解耦效果:

1. 服务间解耦
   - 订单服务不需要知道哪些服务需要处理订单
   - 只需发布消息到主题，由订阅者自行处理
   - 新增服务只需订阅主题，无需修改订单服务

2. 异步处理
   - 库存扣减、统计更新等操作异步执行
   - 订单服务可以快速响应用户
   - 提高系统整体响应速度

3. 可扩展性
   - 可以轻松添加新的消费者服务
   - 每个服务可以独立扩展
   - 支持多种处理策略（如重试、死信队列）

4. 故障隔离
   - 单个服务故障不影响其他服务
   - 消息可以持久化，服务恢复后继续处理
        """)

    def run_demo(self):
        """运行完整演示"""
        print("\n" + "=" * 60)
        print("超市商品管理系统 - 消息中间件集成演示")
        print("=" * 60)

        self.demo_inventory_alert()
        time.sleep(0.5)

        self.demo_order_processing()
        time.sleep(0.5)

        self.demo_payment_processing()
        time.sleep(0.5)

        self.demo_decoupling_analysis()

        # 打印统计信息
        print("\n" + "=" * 60)
        print("系统统计信息")
        print("=" * 60)
        stats = self.broker.get_stats()
        print(f"总消息发送: {stats['total_messages_sent']}")
        print(f"总消息接收: {stats['total_messages_received']}")
        print(f"主题数量: {stats['total_topics']}")


def main():
    """主函数"""
    demo = SupermarketDemo()
    demo.run_demo()


if __name__ == "__main__":
    main()
