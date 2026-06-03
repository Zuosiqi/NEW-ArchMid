"""单元测试 - Broker核心功能"""

import sys
import os
import unittest

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.broker import Broker
from broker.queue import MessageQueue
from broker.topic import Topic, Subscriber
from models.message import Message
from producer.producer import Producer
from consumer.consumer import Consumer


class TestMessage(unittest.TestCase):
    """测试消息类"""

    def test_create_message(self):
        """测试创建消息"""
        msg = Message(topic="test", payload={"data": "hello"})
        self.assertIsNotNone(msg.id)
        self.assertEqual(msg.topic, "test")
        self.assertEqual(msg.payload, {"data": "hello"})
        self.assertFalse(msg.delivered)

    def test_message_to_dict(self):
        """测试消息转字典"""
        msg = Message(topic="test", payload="hello", producer_id="p1")
        data = msg.to_dict()
        self.assertEqual(data["topic"], "test")
        self.assertEqual(data["payload"], "hello")
        self.assertEqual(data["producer_id"], "p1")

    def test_message_from_dict(self):
        """测试从字典创建消息"""
        data = {
            "id": "test-id",
            "topic": "test",
            "payload": "hello",
            "producer_id": "p1",
            "timestamp": 1234567890.0,
        }
        msg = Message.from_dict(data)
        self.assertEqual(msg.id, "test-id")
        self.assertEqual(msg.topic, "test")

    def test_mark_delivered(self):
        """测试标记消息为已投递"""
        msg = Message(topic="test", payload="hello")
        msg.mark_delivered()
        self.assertTrue(msg.delivered)
        self.assertEqual(msg.delivery_count, 1)


class TestMessageQueue(unittest.TestCase):
    """测试消息队列"""

    def setUp(self):
        """测试前准备"""
        self.queue = MessageQueue("test-queue", max_size=100)

    def test_enqueue_dequeue(self):
        """测试入队和出队"""
        msg = Message(topic="test", payload="hello")
        self.queue.enqueue(msg)

        result = self.queue.dequeue()
        self.assertEqual(result.id, msg.id)

    def test_fifo_order(self):
        """测试FIFO顺序"""
        msg1 = Message(topic="test", payload="first")
        msg2 = Message(topic="test", payload="second")

        self.queue.enqueue(msg1)
        self.queue.enqueue(msg2)

        result1 = self.queue.dequeue()
        result2 = self.queue.dequeue()

        self.assertEqual(result1.payload, "first")
        self.assertEqual(result2.payload, "second")

    def test_empty_queue(self):
        """测试空队列"""
        self.assertTrue(self.queue.is_empty())
        self.assertIsNone(self.queue.dequeue())

    def test_queue_size(self):
        """测试队列大小"""
        self.assertEqual(self.queue.size(), 0)

        for i in range(5):
            self.queue.enqueue(Message(topic="test", payload=i))

        self.assertEqual(self.queue.size(), 5)

    def test_queue_full(self):
        """测试队列满"""
        queue = MessageQueue("small-queue", max_size=2)
        queue.enqueue(Message(topic="test", payload=1))
        queue.enqueue(Message(topic="test", payload=2))

        self.assertTrue(queue.is_full())
        with self.assertRaises(ValueError):
            queue.enqueue(Message(topic="test", payload=3))

    def test_peek(self):
        """测试peek操作"""
        msg = Message(topic="test", payload="hello")
        self.queue.enqueue(msg)

        result = self.queue.peek()
        self.assertEqual(result.id, msg.id)
        self.assertEqual(self.queue.size(), 1)  # 不应该移除消息

    def test_clear(self):
        """测试清空队列"""
        for i in range(5):
            self.queue.enqueue(Message(topic="test", payload=i))

        self.queue.clear()
        self.assertTrue(self.queue.is_empty())


class TestTopic(unittest.TestCase):
    """测试主题（观察者模式）"""

    def setUp(self):
        """测试前准备"""
        self.topic = Topic("test-topic")
        self.received_messages = []

    def callback(self, message):
        """测试回调函数"""
        self.received_messages.append(message)

    def test_subscribe(self):
        """测试订阅"""
        subscriber = self.topic.subscribe("consumer-1", self.callback)
        self.assertEqual(self.topic.get_subscriber_count(), 1)
        self.assertEqual(subscriber.id, "consumer-1")

    def test_unsubscribe(self):
        """测试取消订阅"""
        self.topic.subscribe("consumer-1", self.callback)
        result = self.topic.unsubscribe("consumer-1")
        self.assertTrue(result)
        self.assertEqual(self.topic.get_subscriber_count(), 0)

    def test_publish(self):
        """测试发布消息"""
        self.topic.subscribe("consumer-1", self.callback)
        msg = Message(topic="test-topic", payload="hello")

        delivered = self.topic.publish(msg)
        self.assertEqual(delivered, 1)
        self.assertEqual(len(self.received_messages), 1)
        self.assertEqual(self.received_messages[0].payload, "hello")

    def test_multiple_subscribers(self):
        """测试多个订阅者"""
        received2 = []

        def callback2(message):
            received2.append(message)

        self.topic.subscribe("consumer-1", self.callback)
        self.topic.subscribe("consumer-2", callback2)

        msg = Message(topic="test-topic", payload="hello")
        delivered = self.topic.publish(msg)

        self.assertEqual(delivered, 2)
        self.assertEqual(len(self.received_messages), 1)
        self.assertEqual(len(received2), 1)

    def test_publish_no_subscribers(self):
        """测试无订阅者时发布"""
        msg = Message(topic="test-topic", payload="hello")
        delivered = self.topic.publish(msg)
        self.assertEqual(delivered, 0)


class TestBroker(unittest.TestCase):
    """测试Broker"""

    def setUp(self):
        """测试前准备"""
        Broker.reset_instance()
        self.broker = Broker()

    def test_singleton(self):
        """测试单例模式"""
        broker2 = Broker()
        self.assertIs(self.broker, broker2)

    def test_create_queue(self):
        """测试创建队列"""
        queue = self.broker.create_queue("test-queue")
        self.assertIsNotNone(queue)
        self.assertEqual(queue.name, "test-queue")

    def test_send_to_queue(self):
        """测试发送消息到队列"""
        self.broker.create_queue("test-queue")
        msg = Message(topic="test-queue", payload="hello")

        result = self.broker.send_to_queue("test-queue", msg)
        self.assertTrue(result)

    def test_receive_from_queue(self):
        """测试从队列接收消息"""
        self.broker.create_queue("test-queue")
        msg = Message(topic="test-queue", payload="hello")

        self.broker.send_to_queue("test-queue", msg)
        result = self.broker.receive_from_queue("test-queue")

        self.assertIsNotNone(result)
        self.assertEqual(result.payload, "hello")

    def test_topic_pub_sub(self):
        """测试主题发布/订阅"""
        received = []

        def callback(message):
            received.append(message)

        self.broker.subscribe_topic("test-topic", "consumer-1", callback)
        msg = Message(topic="test-topic", payload="hello")

        delivered = self.broker.send_to_topic("test-topic", msg)
        self.assertEqual(delivered, 1)
        self.assertEqual(len(received), 1)


class TestProducerConsumer(unittest.TestCase):
    """测试生产者和消费者"""

    def setUp(self):
        """测试前准备"""
        Broker.reset_instance()
        self.broker = Broker()

    def test_producer_send_to_queue(self):
        """测试生产者发送到队列"""
        producer = Producer("test-producer")
        producer.connect(self.broker)

        self.broker.create_queue("test-queue")
        result = producer.send_to_queue("test-queue", {"data": "hello"})
        self.assertTrue(result)
        self.assertEqual(producer.messages_sent, 1)

    def test_producer_publish_to_topic(self):
        """测试生产者发布到主题"""
        producer = Producer("test-producer")
        producer.connect(self.broker)

        received = []

        def callback(message):
            received.append(message)

        self.broker.subscribe_topic("test-topic", "consumer-1", callback)

        delivered = producer.publish_to_topic("test-topic", {"data": "hello"})
        self.assertEqual(delivered, 1)
        self.assertEqual(len(received), 1)

    def test_consumer_poll(self):
        """测试消费者轮询"""
        producer = Producer("test-producer")
        producer.connect(self.broker)

        consumer = Consumer("test-consumer")
        consumer.connect(self.broker)

        self.broker.create_queue("test-queue")
        producer.send_to_queue("test-queue", {"data": "hello"})

        msg = consumer.poll("test-queue")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.payload, {"data": "hello"})
        self.assertEqual(consumer.messages_received, 1)

    def test_consumer_subscribe(self):
        """测试消费者订阅"""
        producer = Producer("test-producer")
        producer.connect(self.broker)

        consumer = Consumer("test-consumer")
        consumer.connect(self.broker)

        received = []

        def callback(message):
            received.append(message)

        consumer.subscribe("test-topic", callback)
        producer.publish_to_topic("test-topic", {"data": "hello"})

        self.assertEqual(len(received), 1)
        self.assertEqual(consumer.messages_received, 1)


if __name__ == "__main__":
    unittest.main()
