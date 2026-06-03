"""性能测试脚本 - 测试消息中间件的吞吐率和延迟（多进程版）

使用方法：
1. 先启动 broker_server.py
2. 运行本脚本
"""

import sys
import os
import time
import threading

# 设置stdout编码为UTF-8（Windows兼容）
if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker_client import Producer, Consumer


class PerformanceTester:
    """性能测试类"""

    def __init__(self, broker_host="127.0.0.1", broker_port=9000):
        """初始化测试器"""
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.results = {}

    def test_publish_throughput(self, message_count: int = 1000, rounds: int = 3):
        """
        测试发布吞吐率

        Args:
            message_count: 每轮测试的消息数量
            rounds: 测试轮数
        """
        print("\n" + "=" * 60)
        print("发布吞吐率测试")
        print("=" * 60)

        throughput_results = []

        for round_num in range(1, rounds + 1):
            # 创建生产者
            producer = Producer(f"perf-producer-{round_num}",
                              broker_host=self.broker_host,
                              broker_port=self.broker_port)
            if not producer.connect():
                print(f"第 {round_num} 轮: 连接 Broker 失败")
                continue

            # 预热
            for i in range(100):
                producer.publish("perf-test", {"warmup": i})
            time.sleep(0.1)

            # 测试发送性能
            start_time = time.time()
            for i in range(message_count):
                producer.publish("perf-test", {"index": i, "data": f"message-{i}"})
            send_time = time.time() - start_time

            # 计算吞吐率
            send_throughput = message_count / send_time if send_time > 0 else 0

            throughput_results.append({
                "round": round_num,
                "message_count": message_count,
                "send_time": send_time,
                "send_throughput": send_throughput,
            })

            print(f"\n第 {round_num} 轮:")
            print(f"  消息数量: {message_count}")
            print(f"  发送耗时: {send_time:.4f} 秒")
            print(f"  发送吞吐率: {send_throughput:.2f} 条/秒")

            producer.disconnect()

        # 计算平均值
        if throughput_results:
            avg_send_throughput = sum(r["send_throughput"] for r in throughput_results) / len(throughput_results)
            print("\n" + "-" * 40)
            print(f"平均发送吞吐率: {avg_send_throughput:.2f} 条/秒")

            self.results["publish_throughput"] = {
                "rounds": throughput_results,
                "avg_send_throughput": avg_send_throughput,
            }

    def test_subscribe_latency(self, message_count: int = 100):
        """
        测试订阅接收延迟

        Args:
            message_count: 测试消息数量
        """
        print("\n" + "=" * 60)
        print("订阅接收延迟测试")
        print("=" * 60)

        # 创建生产者和消费者
        producer = Producer("latency-producer",
                          broker_host=self.broker_host,
                          broker_port=self.broker_port)
        consumer = Consumer("latency-consumer",
                          broker_host=self.broker_host,
                          broker_port=self.broker_port)

        if not producer.connect() or not consumer.connect():
            print("连接 Broker 失败")
            return

        latencies = []
        received_count = 0
        lock = threading.Lock()

        def on_message(msg):
            nonlocal received_count
            receive_time = time.time()
            send_time = msg.get("payload", {}).get("send_time", 0)
            if send_time:
                latency = (receive_time - send_time) * 1000  # 毫秒
                with lock:
                    latencies.append(latency)
                    received_count += 1

        consumer.subscribe("latency-test", on_message)
        time.sleep(0.1)  # 等待订阅生效

        # 发送消息
        for i in range(message_count):
            producer.publish("latency-test", {
                "index": i,
                "send_time": time.time(),
            })

        # 等待接收
        timeout = time.time() + 5
        while time.time() < timeout and received_count < message_count:
            time.sleep(0.01)

        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            sorted_latencies = sorted(latencies)
            p95_latency = sorted_latencies[int(len(sorted_latencies) * 0.95)]
            p99_latency = sorted_latencies[int(len(sorted_latencies) * 0.99)]

            print(f"\n消息数量: {len(latencies)}/{message_count}")
            print(f"平均延迟: {avg_latency:.4f} ms")
            print(f"最小延迟: {min_latency:.4f} ms")
            print(f"最大延迟: {max_latency:.4f} ms")
            print(f"P95 延迟: {p95_latency:.4f} ms")
            print(f"P99 延迟: {p99_latency:.4f} ms")

            self.results["latency"] = {
                "message_count": len(latencies),
                "avg_latency": avg_latency,
                "min_latency": min_latency,
                "max_latency": max_latency,
                "p95_latency": p95_latency,
                "p99_latency": p99_latency,
            }
        else:
            print("没有收到消息，延迟测试失败")

        producer.disconnect()
        consumer.disconnect()

    def test_concurrent_publishers(self, message_count: int = 500, publisher_count: int = 3):
        """
        测试并发发布吞吐率

        Args:
            message_count: 每个生产者发送的消息数量
            publisher_count: 生产者数量
        """
        print("\n" + "=" * 60)
        print(f"并发发布吞吐率测试 ({publisher_count} 个生产者)")
        print("=" * 60)

        total_sent = 0
        sent_counts = [0] * publisher_count
        threads = []

        def publisher_task(idx):
            producer = Producer(f"concurrent-producer-{idx}",
                              broker_host=self.broker_host,
                              broker_port=self.broker_port)
            if not producer.connect():
                return

            count = 0
            for i in range(message_count):
                producer.publish("concurrent-test", {
                    "producer": idx,
                    "index": i,
                })
                count += 1

            sent_counts[idx] = count
            producer.disconnect()

        # 启动并发生产者
        start_time = time.time()
        for i in range(publisher_count):
            t = threading.Thread(target=publisher_task, args=(i,))
            t.start()
            threads.append(t)

        # 等待完成
        for t in threads:
            t.join()

        end_time = time.time()
        elapsed = end_time - start_time

        total_sent = sum(sent_counts)

        print(f"\n总耗时: {elapsed:.4f} 秒")
        print(f"总发送: {total_sent} 条")
        print(f"发送吞吐率: {total_sent / elapsed:.2f} 条/秒")
        print(f"每个生产者发送: {sent_counts}")

        self.results["concurrent_publish"] = {
            "publisher_count": publisher_count,
            "total_sent": total_sent,
            "elapsed": elapsed,
            "send_throughput": total_sent / elapsed,
        }

    def run_all_tests(self):
        """运行所有性能测试"""
        print("\n" + "=" * 60)
        print("开始消息中间件性能测试")
        print(f"Broker: {self.broker_host}:{self.broker_port}")
        print("=" * 60)

        self.test_publish_throughput(message_count=1000, rounds=3)
        self.test_subscribe_latency(message_count=100)
        self.test_concurrent_publishers(message_count=500, publisher_count=3)

        print("\n" + "=" * 60)
        print("性能测试完成")
        print("=" * 60)

        # 打印总结
        print("\n测试结果总结:")
        print("-" * 40)

        if "publish_throughput" in self.results:
            r = self.results["publish_throughput"]
            print(f"发布吞吐率: {r['avg_send_throughput']:.2f} 条/秒")

        if "latency" in self.results:
            r = self.results["latency"]
            print(f"消息延迟:")
            print(f"  平均: {r['avg_latency']:.4f} ms")
            print(f"  P95: {r['p95_latency']:.4f} ms")
            print(f"  P99: {r['p99_latency']:.4f} ms")

        if "concurrent_publish" in self.results:
            r = self.results["concurrent_publish"]
            print(f"并发发布 ({r['publisher_count']} 个生产者):")
            print(f"  吞吐率: {r['send_throughput']:.2f} 条/秒")

        return self.results


def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser(description="性能测试")
    parser.add_argument("--host", default="127.0.0.1", help="Broker 地址")
    parser.add_argument("--port", type=int, default=9000, help="Broker 端口")
    args = parser.parse_args()

    tester = PerformanceTester(broker_host=args.host, broker_port=args.port)
    tester.run_all_tests()


if __name__ == "__main__":
    main()
