"""消息代理（Broker）- 消息中间件的核心调度器"""

from typing import Dict, List, Optional, Callable, Any
import threading
import time

from models.message import Message
from broker.queue import MessageQueue
from broker.topic import Topic, TopicManager


class Broker:
    """消息代理类 - 中央调度器，管理所有队列和主题

    职责：
    - 管理消息队列的创建、删除
    - 管理主题的创建、删除
    - 消息路由：将消息分发到正确的队列/主题
    - 提供生产者和消费者的连接接口

    设计模式：
    - 单例模式：全局唯一Broker实例
    - 观察者模式：通过Topic实现发布/订阅
    """

    _instance: Optional["Broker"] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """单例模式实现"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """初始化Broker"""
        if hasattr(self, "_initialized"):
            return
        self._initialized = True

        self._queues: Dict[str, MessageQueue] = {}  # 队列字典
        self._topic_manager: TopicManager = TopicManager()  # 主题管理器
        self._producers: Dict[str, Any] = {}  # 已注册的生产者
        self._consumers: Dict[str, Any] = {}  # 已注册的消费者
        self._lock = threading.Lock()
        self._start_time: float = time.time()

        # 统计信息
        self._total_messages_sent: int = 0
        self._total_messages_received: int = 0

    # ==================== 队列管理 ====================

    def create_queue(self, name: str, max_size: int = 10000) -> MessageQueue:
        """
        创建消息队列

        Args:
            name: 队列名称
            max_size: 队列最大容量

        Returns:
            MessageQueue: 创建的队列对象
        """
        with self._lock:
            if name not in self._queues:
                self._queues[name] = MessageQueue(name, max_size)
            return self._queues[name]

    def delete_queue(self, name: str) -> bool:
        """
        删除消息队列

        Args:
            name: 队列名称

        Returns:
            bool: 是否成功删除
        """
        with self._lock:
            if name in self._queues:
                del self._queues[name]
                return True
            return False

    def get_queue(self, name: str) -> Optional[MessageQueue]:
        """
        获取消息队列

        Args:
            name: 队列名称

        Returns:
            MessageQueue: 队列对象，不存在时返回None
        """
        return self._queues.get(name)

    def list_queues(self) -> List[str]:
        """列出所有队列名称"""
        return list(self._queues.keys())

    # ==================== 主题管理 ====================

    def create_topic(self, name: str) -> Topic:
        """
        创建主题

        Args:
            name: 主题名称

        Returns:
            Topic: 创建的主题对象
        """
        return self._topic_manager.create_topic(name)

    def delete_topic(self, name: str) -> bool:
        """
        删除主题

        Args:
            name: 主题名称

        Returns:
            bool: 是否成功删除
        """
        return self._topic_manager.delete_topic(name)

    def get_topic(self, name: str) -> Topic:
        """
        获取主题（不存在时自动创建）

        Args:
            name: 主题名称

        Returns:
            Topic: 主题对象
        """
        return self._topic_manager.get_topic(name)

    def list_topics(self) -> List[str]:
        """列出所有主题名称"""
        return self._topic_manager.list_topics()

    # ==================== 消息发送 ====================

    def send_to_queue(self, queue_name: str, message: Message) -> bool:
        """
        发送消息到指定队列（点对点模式）

        Args:
            queue_name: 目标队列名称
            message: 要发送的消息

        Returns:
            bool: 是否发送成功
        """
        queue = self.get_queue(queue_name)
        if queue is None:
            # 自动创建队列
            queue = self.create_queue(queue_name)

        queue.enqueue(message)
        self._total_messages_sent += 1
        return True

    def send_to_topic(self, topic_name: str, message: Message) -> int:
        """
        发送消息到主题（发布/订阅模式）

        Args:
            topic_name: 目标主题名称
            message: 要发送的消息

        Returns:
            int: 成功接收消息的订阅者数量
        """
        topic = self.get_topic(topic_name)
        message.topic = topic_name
        delivered = topic.publish(message)
        self._total_messages_sent += 1
        self._total_messages_received += delivered
        return delivered

    # ==================== 消息接收 ====================

    def receive_from_queue(self, queue_name: str) -> Optional[Message]:
        """
        从队列接收消息（点对点模式）

        Args:
            queue_name: 队列名称

        Returns:
            Message: 接收到的消息，队列为空时返回None
        """
        queue = self.get_queue(queue_name)
        if queue is None:
            return None

        message = queue.dequeue()
        if message:
            message.mark_delivered()
            self._total_messages_received += 1
        return message

    def subscribe_topic(self, topic_name: str, subscriber_id: str,
                        callback: Callable[[Message], None]):
        """
        订阅主题

        Args:
            topic_name: 主题名称
            subscriber_id: 订阅者ID
            callback: 消息回调函数
        """
        topic = self.get_topic(topic_name)
        topic.subscribe(subscriber_id, callback)

    def unsubscribe_topic(self, topic_name: str, subscriber_id: str) -> bool:
        """
        取消订阅主题

        Args:
            topic_name: 主题名称
            subscriber_id: 订阅者ID

        Returns:
            bool: 是否成功取消
        """
        topic = self.get_topic(topic_name)
        return topic.unsubscribe(subscriber_id)

    # ==================== 生产者/消费者注册 ====================

    def register_producer(self, producer_id: str, producer: Any):
        """注册生产者"""
        self._producers[producer_id] = producer

    def unregister_producer(self, producer_id: str):
        """注销生产者"""
        if producer_id in self._producers:
            del self._producers[producer_id]

    def register_consumer(self, consumer_id: str, consumer: Any):
        """注册消费者"""
        self._consumers[consumer_id] = consumer

    def unregister_consumer(self, consumer_id: str):
        """注销消费者"""
        if consumer_id in self._consumers:
            del self._consumers[consumer_id]

    # ==================== 统计信息 ====================

    def get_stats(self) -> dict:
        """获取Broker统计信息"""
        queue_stats = {name: q.get_stats() for name, q in self._queues.items()}
        topic_stats = self._topic_manager.get_all_stats()

        return {
            "uptime": time.time() - self._start_time,
            "total_queues": len(self._queues),
            "total_topics": len(self.list_topics()),
            "total_producers": len(self._producers),
            "total_consumers": len(self._consumers),
            "total_messages_sent": self._total_messages_sent,
            "total_messages_received": self._total_messages_received,
            "queues": queue_stats,
            "topics": topic_stats,
        }

    def reset_stats(self):
        """重置统计信息"""
        self._total_messages_sent = 0
        self._total_messages_received = 0
        self._start_time = time.time()

    @classmethod
    def reset_instance(cls):
        """重置单例（用于测试）"""
        cls._instance = None

    def __repr__(self) -> str:
        return (f"Broker(queues={len(self._queues)}, topics={len(self.list_topics())}, "
                f"sent={self._total_messages_sent}, received={self._total_messages_received})")
