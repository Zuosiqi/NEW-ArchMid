"""Broker Server - 独立进程，基于Socket的消息中间件服务端"""

import sys
import json
import socket
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Callable, Any

# 设置stdout编码为UTF-8（Windows兼容）
if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]


class BrokerServer:
    """消息代理服务端 - Socket Server

    职责：
    - 监听端口，接收 Producer 和 Consumer 的连接
    - 管理 Topic 和订阅关系
    - 路由消息：从 Producer 接收，推送给订阅的 Consumer
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 9000):
        """
        初始化 Broker Server

        Args:
            host: 监听地址
            port: 监听端口
        """
        self.host = host
        self.port = port
        self.server_socket = None

        # 主题 -> 订阅者列表 {topic_name: [{id, socket}]}
        self.subscriptions: Dict[str, List[Dict[str, Any]]] = {}

        # 客户端连接 {client_id: socket}
        self.clients: Dict[str, socket.socket] = {}

        self.total_messages = 0
        self.total_delivered = 0
        self.total_enqueued = 0
        self.total_dropped = 0

        self.running = False

        self.lock = threading.Lock()

        # Broker 内部消息队列：Producer 发布消息后先入队，由分发线程异步投递
        self.message_queue = queue.Queue(maxsize=10000)

        # 独立消息分发线程
        self.dispatcher_thread = None

    def start(self):
        """启动 Broker Server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(100)
        self.running = True

        # 启动独立消息分发线程
        self.dispatcher_thread = threading.Thread(
            target=self._dispatch_loop,
            daemon=True
        )
        self.dispatcher_thread.start()

        print("=" * 60)
        print("Broker Server 启动")
        print("消息分发线程已启动")

        try:
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    # 为每个连接创建线程
                    thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )
                    thread.start()
                except OSError:
                    break
        except KeyboardInterrupt:
            print("\nBroker Server 正在关闭...")
        finally:
            self.stop()

    def stop(self):
        """停止 Broker Server"""
        self.running = False

        # 尝试唤醒分发线程
        try:
            self.message_queue.put_nowait(None)
        except Exception:
            pass

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

        print("Broker Server 已关闭")

    def _handle_client(self, client_socket: socket.socket, address: tuple):
        """处理客户端连接"""
        client_id = None
        try:
            while self.running:
                # 接收消息长度（4字节）
                length_bytes = self._recv_exact(client_socket, 4)
                if not length_bytes:
                    break

                length = int.from_bytes(length_bytes, byteorder='big')
                # 接收消息内容
                data = self._recv_exact(client_socket, length)
                if not data:
                    break

                # 解析 JSON 消息
                message = json.loads(data.decode('utf-8'))
                response = self._process_message(message, client_socket)

                # 如果是注册消息，记录 client_id
                if message.get("action") == "register":
                    client_id = message.get("client_id")

                # 发送响应
                if response:
                    self._send_message(client_socket, response)

        except ConnectionResetError:
            pass
        except Exception as e:
            print(f"处理客户端 {address} 出错: {e}")
        finally:
            # 清理连接
            if client_id:
                self._remove_client(client_id)
            try:
                client_socket.close()
            except Exception:
                pass
            print(f"客户端断开: {address} (id={client_id})")

    def _process_message(self, message: dict, client_socket: socket.socket) -> dict:
        """处理接收到的消息"""
        action = message.get("action")

        if action == "register":
            return self._handle_register(message, client_socket)
        elif action == "subscribe":
            return self._handle_subscribe(message)
        elif action == "unsubscribe":
            return self._handle_unsubscribe(message)
        elif action == "publish":
            return self._handle_publish(message)
        elif action == "ping":
            return {"action": "pong"}
        else:
            return {"action": "error", "message": f"未知操作: {action}"}

    def _handle_register(self, message: dict, client_socket: socket.socket) -> dict:
        """处理客户端注册"""
        client_id = message.get("client_id")
        client_type = message.get("client_type", "unknown")

        with self.lock:
            self.clients[client_id] = client_socket

        print(f"[注册] {client_type}: {client_id}")
        return {"action": "registered", "client_id": client_id}

    def _handle_subscribe(self, message: dict) -> dict:
        """处理订阅请求"""
        topic = message.get("topic")
        subscriber_id = message.get("subscriber_id")

        with self.lock:
            if topic not in self.subscriptions:
                self.subscriptions[topic] = []

            # 检查是否已订阅
            for sub in self.subscriptions[topic]:
                if sub["id"] == subscriber_id:
                    return {"action": "subscribed", "topic": topic, "message": "已订阅"}

            # 添加订阅
            self.subscriptions[topic].append({
                "id": subscriber_id,
                "socket": self.clients.get(subscriber_id)
            })

        print(f"[订阅] {subscriber_id} -> {topic}")
        return {"action": "subscribed", "topic": topic}

    def _handle_unsubscribe(self, message: dict) -> dict:
        """处理取消订阅"""
        topic = message.get("topic")
        subscriber_id = message.get("subscriber_id")

        with self.lock:
            if topic in self.subscriptions:
                self.subscriptions[topic] = [
                    sub for sub in self.subscriptions[topic]
                    if sub["id"] != subscriber_id
                ]

        print(f"[取消订阅] {subscriber_id} -> {topic}")
        return {"action": "unsubscribed", "topic": topic}

    def _handle_publish(self, message: dict) -> dict:
        """处理消息发布：只负责入队，不直接分发"""
        topic = message.get("topic")
        payload = message.get("payload")

        if not topic:
            return {
                "action": "error",
                "message": "topic不能为空"
            }

        event = {
            "topic": topic,
            "payload": payload,
            "sender_id": message.get("sender_id"),
            "timestamp": message.get("timestamp"),
            "enqueue_time": time.time()
        }

        try:
            self.message_queue.put(event, block=False)

            with self.lock:
                self.total_messages += 1
                self.total_enqueued += 1

            queue_size = self.message_queue.qsize()
            print(f"[入队] {topic} | 当前队列长度: {queue_size}")

            return {
                "action": "published",
                "topic": topic,
                "status": "queued",
                "queue_size": queue_size
            }

        except queue.Full:
            with self.lock:
                self.total_dropped += 1

            print(f"[丢弃] {topic} | Broker消息队列已满")

            return {
                "action": "error",
                "topic": topic,
                "message": "Broker消息队列已满"
            }

    def _dispatch_loop(self):
        """消息分发事件循环：不断从队列中取消息并投递给订阅者"""
        while self.running:
            try:
                event = self.message_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if event is None:
                break

            try:
                delivered = self._deliver_event(event)

                with self.lock:
                    self.total_delivered += delivered

                print(f"[分发] {event['topic']} -> 投递 {delivered} 个订阅者")

            except Exception as e:
                print(f"[分发异常] {e}")

            finally:
                try:
                    self.message_queue.task_done()
                except Exception:
                    pass

    def _deliver_event(self, event: dict) -> int:
        """将单条事件投递给订阅该 topic 的所有消费者"""
        topic = event.get("topic")
        payload = event.get("payload")

        # 复制订阅者列表，避免发送网络消息时长时间占用锁
        with self.lock:
            subscribers = list(self.subscriptions.get(topic, []))

        delivered = 0
        failed_subscribers = []

        for sub in subscribers:
            subscriber_id = sub.get("id")
            sub_socket = sub.get("socket") or self.clients.get(subscriber_id)

            if not sub_socket:
                failed_subscribers.append(subscriber_id)
                continue

            push_msg = {
                "action": "message",
                "topic": topic,
                "payload": payload,
                "subscriber_id": subscriber_id,
                "sender_id": event.get("sender_id"),
                "enqueue_time": event.get("enqueue_time"),
                "deliver_time": time.time()
            }

            try:
                self._send_message(sub_socket, push_msg)
                delivered += 1
            except Exception as e:
                print(f"推送给 {subscriber_id} 失败: {e}")
                failed_subscribers.append(subscriber_id)

        # 清理失效订阅者
        if failed_subscribers:
            with self.lock:
                for topic_name in list(self.subscriptions.keys()):
                    self.subscriptions[topic_name] = [
                        sub for sub in self.subscriptions[topic_name]
                        if sub.get("id") not in failed_subscribers
                    ]

        return delivered

    def _remove_client(self, client_id: str):
        """移除客户端连接"""
        with self.lock:
            if client_id in self.clients:
                del self.clients[client_id]

            # 移除相关订阅
            for topic in list(self.subscriptions.keys()):
                self.subscriptions[topic] = [
                    sub for sub in self.subscriptions[topic]
                    if sub["id"] != client_id
                ]

    def _send_message(self, sock: socket.socket, message: dict):
        """发送消息（带长度前缀）"""
        data = json.dumps(message, ensure_ascii=False).encode('utf-8')
        length = len(data).to_bytes(4, byteorder='big')
        sock.sendall(length + data)

    def _recv_exact(self, sock: socket.socket, size: int) -> bytes:
        """精确接收指定字节数"""
        data = b''
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                return b''
            data += chunk
        return data

    def get_stats(self) -> dict:
        """获取统计信息"""
        with self.lock:
            return {
                "total_messages": self.total_messages,
                "total_enqueued": self.total_enqueued,
                "total_delivered": self.total_delivered,
                "total_dropped": self.total_dropped,
                "queue_size": self.message_queue.qsize(),
                "topics": {t: len(s) for t, s in self.subscriptions.items()},
                "clients": list(self.clients.keys()),
            }

def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser(description="Broker Server")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--port", type=int, default=9000, help="监听端口")
    args = parser.parse_args()

    server = BrokerServer(host=args.host, port=args.port)
    server.start()


if __name__ == "__main__":
    main()
