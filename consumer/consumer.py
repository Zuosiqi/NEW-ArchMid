"""消息消费者 - 从队列或主题接收消息"""

import uuid
from typing import Any, Callable, Dict, List, Optional
from collections import defaultdict

from models.message import Message
from broker.broker import Broker


class Consumer:
    """消息消费者类

    职责：
    - 连接到Broker
    - 从队列接收消息（点对点模式）
    - 订阅主题接收消息（发布/订阅模式）
    - 处理接收到的消息

    使用示例：
        def handle_message(msg):
            print(f"收到消息: {msg}")

        consumer = Consumer("my-consumer")
        consumer.connect(broker)
        consumer.subscribe("order-events", handle_message)
        # 或者从队列轮询
        msg = consumer.poll("order-queue")
    """

    def __init__(self, consumer_id: Optional[str] = None):
        """
        初始化消费者

        Args:
            consumer_id: 消费者ID，不提供时自动生成
        """
        self.id: str = consumer_id or f"consumer-{uuid.uuid4().hex[:8]}"
        self._broker: Optional[Broker] = None
        self._connected: bool = False
        self._messages_received: int = 0
        self._subscriptions: Dict[str, Callable[[Message], None]] = {}  # 订阅的主题和回调
        self._message_history: List[Message] = []  # 接收的消息历史

    def connect(self, broker: Broker) -> bool:
        """
        连接到Broker

        Args:
            broker: Broker实例

        Returns:
            bool: 是否连接成功
        """
        self._broker = broker
        self._broker.register_consumer(self.id, self)
        self._connected = True
        return True

    def disconnect(self):
        """断开与Broker的连接，取消所有订阅"""
        if self._broker and self._connected:
            # 取消所有订阅
            for topic_name in list(self._subscriptions.keys()):
                self.unsubscribe(topic_name)
            self._broker.unregister_consumer(self.id)
        self._connected = False
        self._broker = None

    def subscribe(self, topic_name: str, callback: Optional[Callable[[Message], None]] = None) -> bool:
        """
        订阅主题（发布/订阅模式）

        Args:
            topic_name: 主题名称
            callback: 消息回调函数，不提供时使用默认处理

        Returns:
            bool: 是否订阅成功

        Raises:
            RuntimeError: 未连接到Broker时抛出
        """
        if not self._connected or not self._broker:
            raise RuntimeError(f"消费者 {self.id} 未连接到Broker")

        # 包装回调函数，确保统计信息更新
        def wrapped_callback(message: Message):
            self._messages_received += 1
            self._message_history.append(message)
            if callback:
                callback(message)

        self._broker.subscribe_topic(topic_name, self.id, wrapped_callback)
        self._subscriptions[topic_name] = wrapped_callback
        return True

    def unsubscribe(self, topic_name: str) -> bool:
        """
        取消订阅主题

        Args:
            topic_name: 主题名称

        Returns:
            bool: 是否取消成功
        """
        if not self._connected or not self._broker:
            return False

        result = self._broker.unsubscribe_topic(topic_name, self.id)
        if topic_name in self._subscriptions:
            del self._subscriptions[topic_name]
        return result

    def poll(self, queue_name: str) -> Optional[Message]:
        """
        从队列轮询消息（点对点模式）

        Args:
            queue_name: 队列名称

        Returns:
            Message: 接收到的消息，队列为空时返回None

        Raises:
            RuntimeError: 未连接到Broker时抛出
        """
        if not self._connected or not self._broker:
            raise RuntimeError(f"消费者 {self.id} 未连接到Broker")

        message = self._broker.receive_from_queue(queue_name)
        if message:
            self._messages_received += 1
            self._message_history.append(message)
        return message

    def _default_handler(self, message: Message):
        """
        默认消息处理函数

        Args:
            message: 接收到的消息
        """
        self._messages_received += 1
        self._message_history.append(message)

    def get_message_history(self, limit: int = 100) -> List[Message]:
        """
        获取消息接收历史

        Args:
            limit: 返回的最大消息数

        Returns:
            List[Message]: 消息列表
        """
        return self._message_history[-limit:]

    def clear_history(self):
        """清空消息历史"""
        self._message_history.clear()

    @property
    def is_connected(self) -> bool:
        """是否已连接到Broker"""
        return self._connected

    @property
    def messages_received(self) -> int:
        """已接收消息数量"""
        return self._messages_received

    @property
    def subscriptions(self) -> List[str]:
        """当前订阅的主题列表"""
        return list(self._subscriptions.keys())

    def get_stats(self) -> dict:
        """获取消费者统计信息"""
        return {
            "id": self.id,
            "connected": self._connected,
            "messages_received": self._messages_received,
            "subscriptions": self.subscriptions,
            "history_size": len(self._message_history),
        }

    def __repr__(self) -> str:
        return (f"Consumer(id={self.id}, connected={self._connected}, "
                f"received={self._messages_received}, subscriptions={len(self._subscriptions)})")
