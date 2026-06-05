import os
import sys
import time
import socket
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker_server import BrokerServer
from broker_client import Producer, Consumer


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_test_broker():
    port = get_free_port()
    server = BrokerServer(host="127.0.0.1", port=port)
    thread = threading.Thread(target=server.start, daemon=True)
    thread.start()
    time.sleep(0.5)
    return server, port


def stop_test_broker(server):
    server.stop()
    time.sleep(0.2)


def wait_until(condition, timeout=3.0, interval=0.05):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False


def test_supermarket_order_created_event():
    """
    集成场景测试：
    超市系统创建订单后，发布 order.created 事件。
    日志消费者、库存消费者、通知消费者分别订阅并处理该事件。
    """
    server, port = start_test_broker()

    order_producer = None
    log_consumer = None
    stock_consumer = None
    notify_consumer = None

    try:
        log_records = []
        stock_records = []
        notify_records = []

        def handle_log(message):
            payload = message["payload"]
            log_records.append(
                f"订单日志：订单{payload['order_id']}创建成功，金额{payload['total_amount']}"
            )

        def handle_stock(message):
            payload = message["payload"]
            for item in payload.get("items", []):
                if item.get("remaining_stock", 9999) <= 10:
                    stock_records.append(
                        f"库存预警：商品{item['product_name']}剩余{item['remaining_stock']}"
                    )

        def handle_notify(message):
            payload = message["payload"]
            notify_records.append(
                f"通知：顾客{payload['customer_id']}的订单{payload['order_id']}已创建"
            )

        log_consumer = Consumer("log-consumer-test", broker_host="127.0.0.1", broker_port=port)
        stock_consumer = Consumer("stock-consumer-test", broker_host="127.0.0.1", broker_port=port)
        notify_consumer = Consumer("notify-consumer-test", broker_host="127.0.0.1", broker_port=port)

        assert log_consumer.connect() is True
        assert stock_consumer.connect() is True
        assert notify_consumer.connect() is True

        log_consumer.subscribe("order.created", handle_log)
        stock_consumer.subscribe("order.created", handle_stock)
        notify_consumer.subscribe("order.created", handle_notify)

        time.sleep(0.3)

        order_producer = Producer("order-service-test", broker_host="127.0.0.1", broker_port=port)
        assert order_producer.connect() is True

        order_payload = {
            "order_id": 2026001,
            "customer_id": 1,
            "emp_id": 1,
            "total_amount": 168.50,
            "items": [
                {
                    "product_id": 1,
                    "product_name": "可乐",
                    "quantity": 2,
                    "price": 3.50,
                    "remaining_stock": 5
                },
                {
                    "product_id": 2,
                    "product_name": "面包",
                    "quantity": 1,
                    "price": 8.00,
                    "remaining_stock": 30
                }
            ],
            "timestamp": time.time()
        }

        assert order_producer.publish("order.created", order_payload) is True

        assert wait_until(
            lambda: len(log_records) == 1
            and len(stock_records) == 1
            and len(notify_records) == 1
        )

        assert "订单2026001创建成功" in log_records[0]
        assert "库存预警" in stock_records[0]
        assert "可乐" in stock_records[0]
        assert "顾客1" in notify_records[0]

    finally:
        if order_producer:
            order_producer.disconnect()
        if log_consumer:
            log_consumer.disconnect()
        if stock_consumer:
            stock_consumer.disconnect()
        if notify_consumer:
            notify_consumer.disconnect()
        stop_test_broker(server)