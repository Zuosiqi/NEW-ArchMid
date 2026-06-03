"""消息队列实现 - 基于内存的FIFO队列"""

from collections import deque
from typing import Optional, List
import threading

from models.message import Message


class MessageQueue:
    """消息队列类，实现FIFO（先进先出）消息存储"""

    def __init__(self, name: str, max_size: int = 10000):
        """
        初始化消息队列

        Args:
            name: 队列名称
            max_size: 队列最大容量，默认10000条消息
        """
        self.name: str = name
        self.max_size: int = max_size
        self._queue: deque = deque()  # 使用deque实现高效FIFO
        self._lock = threading.Lock()  # 线程安全锁
        self._total_enqueued: int = 0  # 总入队数
        self._total_dequeued: int = 0  # 总出队数

    def enqueue(self, message: Message) -> bool:
        """
        将消息加入队列

        Args:
            message: 要入队的消息

        Returns:
            bool: 是否成功入队

        Raises:
            ValueError: 队列已满时抛出异常
        """
        with self._lock:
            if len(self._queue) >= self.max_size:
                raise ValueError(f"队列 '{self.name}' 已满，容量: {self.max_size}")
            self._queue.append(message)
            self._total_enqueued += 1
            return True

    def dequeue(self) -> Optional[Message]:
        """
        从队列取出消息（FIFO）

        Returns:
            Message: 队列头部的消息，队列为空时返回None
        """
        with self._lock:
            if not self._queue:
                return None
            message = self._queue.popleft()
            self._total_dequeued += 1
            return message

    def peek(self) -> Optional[Message]:
        """
        查看队列头部消息但不取出

        Returns:
            Message: 队列头部的消息，队列为空时返回None
        """
        with self._lock:
            if not self._queue:
                return None
            return self._queue[0]

    def size(self) -> int:
        """获取队列当前大小"""
        return len(self._queue)

    def is_empty(self) -> bool:
        """判断队列是否为空"""
        return len(self._queue) == 0

    def is_full(self) -> bool:
        """判断队列是否已满"""
        return len(self._queue) >= self.max_size

    def clear(self):
        """清空队列"""
        with self._lock:
            self._queue.clear()

    def get_stats(self) -> dict:
        """获取队列统计信息"""
        return {
            "name": self.name,
            "current_size": self.size(),
            "max_size": self.max_size,
            "total_enqueued": self._total_enqueued,
            "total_dequeued": self._total_dequeued,
            "is_empty": self.is_empty(),
            "is_full": self.is_full(),
        }

    def get_messages(self, limit: int = 100) -> List[Message]:
        """
        获取队列中的消息列表（不取出）

        Args:
            limit: 返回的最大消息数

        Returns:
            List[Message]: 消息列表
        """
        with self._lock:
            return list(self._queue)[:limit]

    def __len__(self) -> int:
        return self.size()

    def __repr__(self) -> str:
        return f"MessageQueue(name={self.name}, size={self.size()}, max={self.max_size})"
