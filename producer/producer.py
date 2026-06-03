"""消息生产者 - 发送消息到队列或主题"""

import uuid
from typing import Any, Optional

from models.message import Message
from broker.broker import Broker


class Producer:
    """消息生产者类

    职责：
    - 连接到Broker
    - 发送消息到指定队列（点对点模式）
    - 发布消息到指定主题（发布/订阅模式）

    使用示例：
        producer = Producer("my-producer")
        producer.connect(broker)
        producer.send_to_queue("order-queue", {"order_id": 123})
        producer.publish_to_topic("order-events", {"event": "created", "order_id": 123})
    """

    def __init__(self, producer_id: Optional[str] = None):
        """
        初始化生产者

        Args:
            producer_id: 生产者ID，不提供时自动生成
        """
        self.id: str = producer_id or f"producer-{uuid.uuid4().hex[:8]}"
        self._broker: Optional[Broker] = None
        self._connected: bool = False
        self._messages_sent: int = 0

    def connect(self, broker: Broker) -> bool:
        """
        连接到Broker

        Args:
            broker: Broker实例

        Returns:
            bool: 是否连接成功
        """
        self._broker = broker
        self._broker.register_producer(self.id, self)
        self._connected = True
        return True

    def disconnect(self):
        """断开与Broker的连接"""
        if self._broker and self._connected:
            self._broker.unregister_producer(self.id)
        self._connected = False
        self._broker = None

    def send_to_queue(self, queue_name: str, payload: Any) -> bool:
        """
        发送消息到指定队列（点对点模式）

        Args:
            queue_name: 目标队列名称
            payload: 消息负载

        Returns:
            bool: 是否发送成功

        Raises:
            RuntimeError: 未连接到Broker时抛出
        """
        if not self._connected or not self._broker:
            raise RuntimeError(f"生产者 {self.id} 未连接到Broker")

        message = Message(topic=queue_name, payload=payload, producer_id=self.id)
        result = self._broker.send_to_queue(queue_name, message)
        if result:
            self._messages_sent += 1
        return result

    def publish_to_topic(self, topic_name: str, payload: Any) -> int:
        """
        发布消息到主题（发布/订阅模式）

        Args:
            topic_name: 目标主题名称
            payload: 消息负载

        Returns:
            int: 成功接收消息的订阅者数量

        Raises:
            RuntimeError: 未连接到Broker时抛出
        """
        if not self._connected or not self._broker:
            raise RuntimeError(f"生产者 {self.id} 未连接到Broker")

        message = Message(topic=topic_name, payload=payload, producer_id=self.id)
        delivered = self._broker.send_to_topic(topic_name, message)
        self._messages_sent += 1
        return delivered

    def send(self, destination: str, payload: Any, mode: str = "queue") -> Any:
        """
        统一发送接口

        Args:
            destination: 目标队列/主题名称
            payload: 消息负载
            mode: 发送模式，"queue"或"topic"

        Returns:
            bool或int: 队列模式返回bool，主题模式返回int
        """
        if mode == "queue":
            return self.send_to_queue(destination, payload)
        elif mode == "topic":
            return self.publish_to_topic(destination, payload)
        else:
            raise ValueError(f"不支持的发送模式: {mode}")

    @property
    def is_connected(self) -> bool:
        """是否已连接到Broker"""
        return self._connected

    @property
    def messages_sent(self) -> int:
        """已发送消息数量"""
        return self._messages_sent

    def get_stats(self) -> dict:
        """获取生产者统计信息"""
        return {
            "id": self.id,
            "connected": self._connected,
            "messages_sent": self._messages_sent,
        }

    def __repr__(self) -> str:
        return f"Producer(id={self.id}, connected={self._connected}, sent={self._messages_sent})"
