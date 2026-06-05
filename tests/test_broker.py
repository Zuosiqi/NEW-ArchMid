import os
import sys
import time
import socket
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker_server import BrokerServer
from broker_client import Producer, Consumer


def get_free_port():
    """获取一个当前可用端口，避免测试端口冲突"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_test_broker():
    """启动测试用 BrokerServer"""
    port = get_free_port()
    server = BrokerServer(host="127.0.0.1", port=port)
    thread = threading.Thread(target=server.start, daemon=True)
    thread.start()
    time.sleep(0.5)
    return server, port


def stop_test_broker(server):
    """停止测试用 BrokerServer"""
    server.stop()
    time.sleep(0.2)


def wait_until(condition, timeout=3.0, interval=0.05):
    """等待条件满足"""
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False


def test_single_producer_single_consumer():
    """测试：单生产者、单消费者、同一主题"""
    server, port = start_test_broker()

    producer = None
    consumer = None

    try:
        received = []

        consumer = Consumer("test-consumer-1", broker_host="127.0.0.1", broker_port=port)
        assert consumer.connect() is True
        consumer.subscribe("test.topic", lambda msg: received.append(msg))

        time.sleep(0.2)

        producer = Producer("test-producer-1", broker_host="127.0.0.1", broker_port=port)
        assert producer.connect() is True
        assert producer.publish("test.topic", {"value": 123}) is True

        assert wait_until(lambda: len(received) == 1)

        assert received[0]["topic"] == "test.topic"
        assert received[0]["payload"]["value"] == 123

    finally:
        if producer:
            producer.disconnect()
        if consumer:
            consumer.disconnect()
        stop_test_broker(server)


def test_one_topic_multiple_consumers():
    """测试：一个 topic 同时投递给多个消费者"""
    server, port = start_test_broker()

    producer = None
    consumer_a = None
    consumer_b = None

    try:
        received_a = []
        received_b = []

        consumer_a = Consumer("test-consumer-a", broker_host="127.0.0.1", broker_port=port)
        consumer_b = Consumer("test-consumer-b", broker_host="127.0.0.1", broker_port=port)

        assert consumer_a.connect() is True
        assert consumer_b.connect() is True

        consumer_a.subscribe("order.created", lambda msg: received_a.append(msg))
        consumer_b.subscribe("order.created", lambda msg: received_b.append(msg))

        time.sleep(0.2)

        producer = Producer("order-service-test", broker_host="127.0.0.1", broker_port=port)
        assert producer.connect() is True
        assert producer.publish("order.created", {"order_id": 1001}) is True

        assert wait_until(lambda: len(received_a) == 1 and len(received_b) == 1)

        assert received_a[0]["payload"]["order_id"] == 1001
        assert received_b[0]["payload"]["order_id"] == 1001

    finally:
        if producer:
            producer.disconnect()
        if consumer_a:
            consumer_a.disconnect()
        if consumer_b:
            consumer_b.disconnect()
        stop_test_broker(server)


def test_topic_filtering():
    """测试：消费者只收到自己订阅的主题"""
    server, port = start_test_broker()

    producer = None
    consumer = None

    try:
        received = []

        consumer = Consumer("filter-consumer", broker_host="127.0.0.1", broker_port=port)
        assert consumer.connect() is True
        consumer.subscribe("topic.a", lambda msg: received.append(msg))

        time.sleep(0.2)

        producer = Producer("filter-producer", broker_host="127.0.0.1", broker_port=port)
        assert producer.connect() is True

        assert producer.publish("topic.b", {"value": "should_not_receive"}) is True
        time.sleep(0.5)
        assert len(received) == 0

        assert producer.publish("topic.a", {"value": "should_receive"}) is True
        assert wait_until(lambda: len(received) == 1)

        assert received[0]["topic"] == "topic.a"
        assert received[0]["payload"]["value"] == "should_receive"

    finally:
        if producer:
            producer.disconnect()
        if consumer:
            consumer.disconnect()
        stop_test_broker(server)


def test_broker_stats_after_publish():
    """测试：Broker 统计信息"""
    server, port = start_test_broker()

    producer = None
    consumer = None

    try:
        received = []

        consumer = Consumer("stats-consumer", broker_host="127.0.0.1", broker_port=port)
        assert consumer.connect() is True
        consumer.subscribe("stats.topic", lambda msg: received.append(msg))

        time.sleep(0.2)

        producer = Producer("stats-producer", broker_host="127.0.0.1", broker_port=port)
        assert producer.connect() is True

        for i in range(5):
            assert producer.publish("stats.topic", {"index": i}) is True

        assert wait_until(lambda: len(received) == 5)

        stats = server.get_stats()
        assert stats["total_messages"] >= 5
        assert stats["total_enqueued"] >= 5
        assert stats["total_delivered"] >= 5
        assert stats["topics"].get("stats.topic") == 1

    finally:
        if producer:
            producer.disconnect()
        if consumer:
            consumer.disconnect()
        stop_test_broker(server)