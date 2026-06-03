"""主题管理 - 实现观察者模式（发布/订阅）"""

from typing import Callable, Dict, List, Set, Any
import threading

from models.message import Message


class Subscriber:
    """订阅者类 - 观察者模式中的观察者"""

    def __init__(self, subscriber_id: str, callback: Callable[[Message], None]):
        """
        初始化订阅者

        Args:
            subscriber_id: 订阅者唯一ID
            callback: 收到消息时的回调函数
        """
        self.id: str = subscriber_id
        self.callback: Callable[[Message], None] = callback
        self.messages_received: int = 0

    def on_message(self, message: Message):
        """接收消息并调用回调函数"""
        self.messages_received += 1
        self.callback(message)

    def __repr__(self) -> str:
        return f"Subscriber(id={self.id}, received={self.messages_received})"


class Topic:
    """主题类 - 观察者模式中的主题（Subject）

    实现发布/订阅模式：
    - 生产者可以向主题发布消息
    - 消费者可以订阅主题接收消息
    - 一个主题可以有多个订阅者（一对多）
    """

    def __init__(self, name: str):
        """
        初始化主题

        Args:
            name: 主题名称
        """
        self.name: str = name
        self._subscribers: Dict[str, Subscriber] = {}  # 订阅者字典
        self._lock = threading.Lock()  # 线程安全锁
        self._total_published: int = 0  # 总发布消息数
        self._total_delivered: int = 0  # 总投递消息数

    def subscribe(self, subscriber_id: str, callback: Callable[[Message], None]) -> Subscriber:
        """
        订阅主题（注册观察者）

        Args:
            subscriber_id: 订阅者ID
            callback: 消息回调函数

        Returns:
            Subscriber: 创建的订阅者对象
        """
        with self._lock:
            if subscriber_id in self._subscribers:
                # 更新回调函数
                self._subscribers[subscriber_id].callback = callback
            else:
                # 创建新订阅者
                subscriber = Subscriber(subscriber_id, callback)
                self._subscribers[subscriber_id] = subscriber
            return self._subscribers[subscriber_id]

    def unsubscribe(self, subscriber_id: str) -> bool:
        """
        取消订阅（移除观察者）

        Args:
            subscriber_id: 订阅者ID

        Returns:
            bool: 是否成功取消
        """
        with self._lock:
            if subscriber_id in self._subscribers:
                del self._subscribers[subscriber_id]
                return True
            return False

    def publish(self, message: Message) -> int:
        """
        发布消息到主题（通知所有观察者）

        Args:
            message: 要发布的消息

        Returns:
            int: 成功接收消息的订阅者数量
        """
        with self._lock:
            self._total_published += 1
            delivered_count = 0

            # 通知所有订阅者
            for subscriber in self._subscribers.values():
                try:
                    subscriber.on_message(message)
                    delivered_count += 1
                except Exception as e:
                    # 订阅者处理失败不影响其他订阅者
                    print(f"订阅者 {subscriber.id} 处理消息失败: {e}")

            self._total_delivered += delivered_count
            return delivered_count

    def get_subscribers(self) -> List[Subscriber]:
        """获取所有订阅者列表"""
        return list(self._subscribers.values())

    def get_subscriber_count(self) -> int:
        """获取订阅者数量"""
        return len(self._subscribers)

    def has_subscriber(self, subscriber_id: str) -> bool:
        """检查是否有指定订阅者"""
        return subscriber_id in self._subscribers

    def get_stats(self) -> dict:
        """获取主题统计信息"""
        return {
            "name": self.name,
            "subscriber_count": self.get_subscriber_count(),
            "total_published": self._total_published,
            "total_delivered": self._total_delivered,
            "subscribers": [s.id for s in self._subscribers.values()],
        }

    def __repr__(self) -> str:
        return f"Topic(name={self.name}, subscribers={self.get_subscriber_count()})"


class TopicManager:
    """主题管理器 - 管理所有主题"""

    def __init__(self):
        """初始化主题管理器"""
        self._topics: Dict[str, Topic] = {}
        self._lock = threading.Lock()

    def create_topic(self, name: str) -> Topic:
        """
        创建主题

        Args:
            name: 主题名称

        Returns:
            Topic: 创建的主题对象
        """
        with self._lock:
            if name not in self._topics:
                self._topics[name] = Topic(name)
            return self._topics[name]

    def delete_topic(self, name: str) -> bool:
        """
        删除主题

        Args:
            name: 主题名称

        Returns:
            bool: 是否成功删除
        """
        with self._lock:
            if name in self._topics:
                del self._topics[name]
                return True
            return False

    def get_topic(self, name: str) -> Topic:
        """
        获取主题

        Args:
            name: 主题名称

        Returns:
            Topic: 主题对象，不存在时自动创建
        """
        with self._lock:
            if name not in self._topics:
                self._topics[name] = Topic(name)
            return self._topics[name]

    def list_topics(self) -> List[str]:
        """列出所有主题名称"""
        return list(self._topics.keys())

    def get_all_stats(self) -> Dict[str, dict]:
        """获取所有主题的统计信息"""
        return {name: topic.get_stats() for name, topic in self._topics.items()}
