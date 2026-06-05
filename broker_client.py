"""Broker Client - Producer/Consumer 的 Socket 客户端封装"""

import sys
import json
import socket
import threading
import time
import uuid
from typing import Callable, Optional, Dict, Any

# 设置stdout编码为UTF-8（Windows兼容）
if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]


class BrokerClient:
    """Broker 客户端基类

    通过 Socket 连接到 Broker Server，支持：
    - 注册为 Producer 或 Consumer
    - 发布消息到主题
    - 订阅主题接收消息
    """

    def __init__(self, client_id: str, client_type: str,
                 broker_host: str = "127.0.0.1", broker_port: int = 9000):
        """
        初始化客户端

        Args:
            client_id: 客户端唯一ID
            client_type: 客户端类型（producer/consumer）
            broker_host: Broker 地址
            broker_port: Broker 端口
        """
        self.client_id = client_id
        self.client_type = client_type
        self.broker_host = broker_host
        self.broker_port = broker_port

        self.socket: Optional[socket.socket] = None
        self.connected = False
        self.running = False

        # 消息回调 {topic: callback}
        self.callbacks: Dict[str, Callable] = {}

        # 接收线程
        self._recv_thread: Optional[threading.Thread] = None

        # 统计
        self.messages_sent = 0
        self.messages_received = 0

    def connect(self) -> bool:
        """连接到 Broker Server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.broker_host, self.broker_port))
            self.connected = True
            self.running = True

            # 发送注册消息
            self._send_message({
                "action": "register",
                "client_id": self.client_id,
                "client_type": self.client_type
            })

            # 启动接收线程
            self._recv_thread = threading.Thread(target=self._receive_loop, daemon=True)
            self._recv_thread.start()

            print(f"[{self.client_id}] 已连接到 Broker {self.broker_host}:{self.broker_port}")
            return True

        except Exception as e:
            print(f"[{self.client_id}] 连接失败: {e}")
            return False

    def disconnect(self):
        """断开连接"""
        self.running = False
        self.connected = False
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
        print(f"[{self.client_id}] 已断开连接")

    def publish(self, topic: str, payload: Any) -> bool:
        """
        发布消息到主题

        Args:
            topic: 主题名称
            payload: 消息内容

        Returns:
            bool: 是否发送成功
        """
        if not self.connected:
            print(f"[{self.client_id}] 未连接到 Broker")
            return False

        message = {
            "action": "publish",
            "topic": topic,
            "payload": payload,
            "sender_id": self.client_id,
            "timestamp": time.time()
        }

        try:
            self._send_message(message)
            self.messages_sent += 1
            return True
        except Exception as e:
            print(f"[{self.client_id}] 发布失败: {e}")
            return False

    def subscribe(self, topic: str, callback: Callable[[dict], None]) -> bool:
        """
        订阅主题

        Args:
            topic: 主题名称
            callback: 收到消息时的回调函数

        Returns:
            bool: 是否订阅成功
        """
        if not self.connected:
            print(f"[{self.client_id}] 未连接到 Broker")
            return False

        # 保存回调
        self.callbacks[topic] = callback

        # 发送订阅请求
        message = {
            "action": "subscribe",
            "topic": topic,
            "subscriber_id": self.client_id
        }

        try:
            self._send_message(message)
            print(f"[{self.client_id}] 订阅主题: {topic}")
            return True
        except Exception as e:
            print(f"[{self.client_id}] 订阅失败: {e}")
            return False

    def unsubscribe(self, topic: str) -> bool:
        """取消订阅"""
        if topic in self.callbacks:
            del self.callbacks[topic]

        message = {
            "action": "unsubscribe",
            "topic": topic,
            "subscriber_id": self.client_id
        }

        try:
            self._send_message(message)
            return True
        except Exception:
            return False

    def _receive_loop(self):
        """接收消息循环"""
        while self.running and self.connected:
            try:
                message = self._recv_message()
                if message:
                    self._handle_message(message)
            except ConnectionResetError:
                self.connected = False
                break
            except Exception as e:
                if self.running:
                    print(f"[{self.client_id}] 接收错误: {e}")
                break

    def _handle_message(self, message: dict):
        """处理接收到的消息"""
        action = message.get("action")

        if action == "message":
            # 收到推送的消息
            topic = message.get("topic")
            payload = message.get("payload")
            self.messages_received += 1

            # 调用回调
            if topic in self.callbacks:
                try:
                    self.callbacks[topic]({
                        "topic": topic,
                        "payload": payload,
                        "timestamp": time.time()
                    })
                except Exception as e:
                    print(f"[{self.client_id}] 回调执行失败: {e}")

        elif action == "subscribed":
            print(f"[{self.client_id}] 订阅成功: {message.get('topic')}")

        elif action == "published":
            pass  # 发布确认，忽略

        elif action == "pong":
            pass  # 心跳响应，忽略

    def _send_message(self, message: dict):
        """发送消息（带长度前缀）"""
        if not self.socket:
            raise ConnectionError("未建立连接")
        data = json.dumps(message, ensure_ascii=False).encode('utf-8')
        length = len(data).to_bytes(4, byteorder='big')
        self.socket.sendall(length + data)

    def _recv_message(self) -> Optional[dict]:
        """接收消息（带长度前缀）"""
        # 接收长度
        length_bytes = self._recv_exact(4)
        if not length_bytes:
            return None

        length = int.from_bytes(length_bytes, byteorder='big')
        # 接收内容
        data = self._recv_exact(length)
        if not data:
            return None

        return json.loads(data.decode('utf-8'))

    def _recv_exact(self, size: int) -> bytes:
        """精确接收指定字节数"""
        if not self.socket:
            return b''
        data = b''
        while len(data) < size:
            chunk = self.socket.recv(size - len(data))
            if not chunk:
                return b''
            data += chunk
        return data


class Producer(BrokerClient):
    """消息生产者"""

    def __init__(self, producer_id: Optional[str] = None, **kwargs):
        producer_id = producer_id or f"producer-{uuid.uuid4().hex[:8]}"
        super().__init__(client_id=producer_id, client_type="producer", **kwargs)

    def send_to_topic(self, topic: str, payload: Any) -> bool:
        """发送消息到主题"""
        return self.publish(topic, payload)


class Consumer(BrokerClient):
    """消息消费者"""

    def __init__(self, consumer_id: Optional[str] = None, **kwargs):
        consumer_id = consumer_id or f"consumer-{uuid.uuid4().hex[:8]}"
        super().__init__(client_id=consumer_id, client_type="consumer", **kwargs)

    def on_message(self, topic: str, callback: Callable[[dict], None]):
        """注册消息处理回调并订阅"""
        self.subscribe(topic, callback)


def main():
    """测试用"""
    import argparse
    parser = argparse.ArgumentParser(description="Broker Client Test")
    parser.add_argument("--type", choices=["producer", "consumer"], required=True)
    parser.add_argument("--id", help="客户端ID")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    if args.type == "producer":
        client = Producer(args.id, broker_host=args.host, broker_port=args.port)
        client.connect()

        # 测试发送
        for i in range(5):
            client.publish("test.topic", {"index": i, "message": f"消息 {i}"})
            time.sleep(1)

        client.disconnect()
    else:
        client = Consumer(args.id, broker_host=args.host, broker_port=args.port)
        client.connect()
        client.subscribe("test.topic", lambda msg: print(f"收到: {msg}"))

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            client.disconnect()


if __name__ == "__main__":
    main()
