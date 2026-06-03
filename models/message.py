"""消息模型 - 定义消息的数据结构"""

import uuid
import time
from typing import Any, Dict, Optional


class Message:
    """消息类，表示在消息中间件中传递的数据单元"""

    def __init__(self, topic: str, payload: Any, producer_id: str = ""):
        """
        初始化消息

        Args:
            topic: 消息主题
            payload: 消息负载（可以是任意数据）
            producer_id: 生产者ID
        """
        self.id: str = str(uuid.uuid4())  # 消息唯一ID
        self.topic: str = topic            # 消息主题
        self.payload: Any = payload        # 消息内容
        self.producer_id: str = producer_id  # 生产者ID
        self.timestamp: float = time.time()  # 创建时间戳
        self.delivered: bool = False       # 是否已投递
        self.delivery_count: int = 0       # 投递次数

    def to_dict(self) -> Dict[str, Any]:
        """将消息转换为字典格式"""
        return {
            "id": self.id,
            "topic": self.topic,
            "payload": self.payload,
            "producer_id": self.producer_id,
            "timestamp": self.timestamp,
            "delivered": self.delivered,
            "delivery_count": self.delivery_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """从字典创建消息对象"""
        msg = cls(
            topic=data["topic"],
            payload=data["payload"],
            producer_id=data.get("producer_id", ""),
        )
        msg.id = data.get("id", msg.id)
        msg.timestamp = data.get("timestamp", msg.timestamp)
        msg.delivered = data.get("delivered", False)
        msg.delivery_count = data.get("delivery_count", 0)
        return msg

    def mark_delivered(self):
        """标记消息为已投递"""
        self.delivered = True
        self.delivery_count += 1

    def __repr__(self) -> str:
        return f"Message(id={self.id[:8]}..., topic={self.topic}, payload={self.payload!r})"

    def __str__(self) -> str:
        return f"[{self.topic}] {self.payload}"
