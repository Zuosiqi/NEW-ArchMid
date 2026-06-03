"""性能测试脚本 - 测试消息中间件的吞吐率和延迟"""

import sys
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 设置stdout编码为UTF-8（Windows兼容）
if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.broker import Broker
from producer.producer import Producer
from consumer.consumer import Consumer
from models.message import Message


class PerformanceTester:
    """性能测试类"""

    def __init__(self):
        """初始化测试器"""
        self.results = {}

    def test_queue_throughput(self, message_count: int = 1000, rounds: int = 3):
        """
        测试队列模式吞吐率

        Args:
            message_count: 每轮测试的消息数量
            rounds: 测试轮数
        """
        print("\n" + "=" * 60)
        print("队列模式吞吐率测试")
        print("=" * 60)

        throughput_results = []

        for round_num in range(1, rounds + 1):
            # 重置Broker
            Broker.reset_instance()
            broker = Broker()

            # 创建生产者和消费者
            producer = Producer("perf-producer")
            producer.connect(broker)

            consumer = Consumer("perf-consumer")
            consumer.connect(broker)

            # 创建队列
            broker.create_queue("perf-queue")

            # 预热
            for i in range(100):
                producer.send_to_queue("perf-queue", {"warmup": i})
            for i in range(100):
                consumer.poll("perf-queue")

            # 测试发送性能
            start_time = time.time()
            for i in range(message_count):
                producer.send_to_queue("perf-queue", {"index": i, "data": f"message-{i}"})
            send_time = time.time() - start_time

            # 测试接收性能
            start_time = time.time()
            received_count = 0
            for i in range(message_count):
                msg = consumer.poll("perf-queue")
                if msg:
                    received_count += 1
            receive_time = time.time() - start_time

            # 计算吞吐率
            send_throughput = message_count / send_time if send_time > 0 else 0
            receive_throughput = message_count / receive_time if receive_time > 0 else 0

            throughput_results.append({
                "round": round_num,
                "message_count": message_count,
                "send_time": send_time,
                "receive_time": receive_time,
                "send_throughput": send_throughput,
                "receive_throughput": receive_throughput,
            })

            print(f"\n第 {round_num} 轮:")
            print(f"  消息数量: {message_count}")
            print(f"  发送耗时: {send_time:.4f} 秒")
            print(f"  接收耗时: {receive_time:.4f} 秒")
            print(f"  发送吞吐率: {send_throughput:.2f} 条/秒")
            print(f"  接收吞吐率: {receive_throughput:.2f} 条/秒")

        # 计算平均值
        avg_send_throughput = sum(r["send_throughput"] for r in throughput_results) / rounds
        avg_receive_throughput = sum(r["receive_throughput"] for r in throughput_results) / rounds

        print("\n" + "-" * 40)
        print(f"平均发送吞吐率: {avg_send_throughput:.2f} 条/秒")
        print(f"平均接收吞吐率: {avg_receive_throughput:.2f} 条/秒")

        self.results["queue_throughput"] = {
            "rounds": throughput_results,
            "avg_send_throughput": avg_send_throughput,
            "avg_receive_throughput": avg_receive_throughput,
        }

    def test_topic_throughput(self, message_count: int = 1000, subscriber_count: int = 3, rounds: int = 3):
        """
        测试主题模式吞吐率（一对多）

        Args:
            message_count: 每轮测试的消息数量
            subscriber_count: 订阅者数量
            rounds: 测试轮数
        """
        print("\n" + "=" * 60)
        print(f"主题模式吞吐率测试 (1 个生产者 -> {subscriber_count} 个订阅者)")
        print("=" * 60)

        throughput_results = []

        for round_num in range(1, rounds + 1):
            # 重置Broker
            Broker.reset_instance()
            broker = Broker()

            # 创建生产者
            producer = Producer("perf-producer")
            producer.connect(broker)

            # 创建多个消费者
            consumers = []
            received_counts = [0] * subscriber_count

            for i in range(subscriber_count):
                consumer = Consumer(f"consumer-{i}")
                consumer.connect(broker)

                # 使用闭包捕获正确的索引
                def make_callback(idx):
                    def callback(message):
                        received_counts[idx] += 1
                    return callback

                consumer.subscribe("perf-topic", make_callback(i))
                consumers.append(consumer)

            # 预热
            for i in range(100):
                producer.publish_to_topic("perf-topic", {"warmup": i})
            received_counts = [0] * subscriber_count

            # 测试发送性能
            start_time = time.time()
            for i in range(message_count):
                producer.publish_to_topic("perf-topic", {"index": i, "data": f"message-{i}"})
            send_time = time.time() - start_time

            # 等待所有消息被处理
            time.sleep(0.1)

            # 计算总接收数
            total_received = sum(received_counts)

            # 计算吞吐率
            send_throughput = message_count / send_time if send_time > 0 else 0
            total_throughput = total_received / send_time if send_time > 0 else 0

            throughput_results.append({
                "round": round_num,
                "message_count": message_count,
                "subscriber_count": subscriber_count,
                "total_received": total_received,
                "send_time": send_time,
                "send_throughput": send_throughput,
                "total_throughput": total_throughput,
                "received_per_subscriber": received_counts,
            })

            print(f"\n第 {round_num} 轮:")
            print(f"  消息数量: {message_count}")
            print(f"  订阅者数量: {subscriber_count}")
            print(f"  总接收数: {total_received}")
            print(f"  发送耗时: {send_time:.4f} 秒")
            print(f"  发送吞吐率: {send_throughput:.2f} 条/秒")
            print(f"  总投递吞吐率: {total_throughput:.2f} 条/秒")
            print(f"  每个订阅者接收: {received_counts}")

        # 计算平均值
        avg_send_throughput = sum(r["send_throughput"] for r in throughput_results) / rounds
        avg_total_throughput = sum(r["total_throughput"] for r in throughput_results) / rounds

        print("\n" + "-" * 40)
        print(f"平均发送吞吐率: {avg_send_throughput:.2f} 条/秒")
        print(f"平均总投递吞吐率: {avg_total_throughput:.2f} 条/秒")

        self.results["topic_throughput"] = {
            "rounds": throughput_results,
            "avg_send_throughput": avg_send_throughput,
            "avg_total_throughput": avg_total_throughput,
        }

    def test_latency(self, message_count: int = 100):
        """
        测试消息端到端延迟

        Args:
            message_count: 测试消息数量
        """
        print("\n" + "=" * 60)
        print("消息延迟测试")
        print("=" * 60)

        # 重置Broker
        Broker.reset_instance()
        broker = Broker()

        # 创建生产者和消费者
        producer = Producer("latency-producer")
        producer.connect(broker)

        latencies = []

        def latency_callback(message):
            """计算延迟的回调函数"""
            receive_time = time.time()
            send_time = message.payload.get("send_time", 0)
            if send_time:
                latency = (receive_time - send_time) * 1000  # 转换为毫秒
                latencies.append(latency)

        consumer = Consumer("latency-consumer")
        consumer.connect(broker)
        consumer.subscribe("latency-topic", latency_callback)

        # 发送消息并测量延迟
        for i in range(message_count):
            producer.publish_to_topic("latency-topic", {
                "index": i,
                "send_time": time.time(),
            })

        # 等待所有消息被处理
        time.sleep(0.5)

        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
            p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]

            print(f"\n消息数量: {len(latencies)}")
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
            print("没有收集到延迟数据")

    def test_concurrent_throughput(self, message_count: int = 1000, producer_count: int = 3, consumer_count: int = 3):
        """
        测试并发吞吐率（多生产者多消费者）

        Args:
            message_count: 每个生产者发送的消息数量
            producer_count: 生产者数量
            consumer_count: 消费者数量
        """
        print("\n" + "=" * 60)
        print(f"并发吞吐率测试 ({producer_count} 个生产者 -> {consumer_count} 个消费者)")
        print("=" * 60)

        # 重置Broker
        Broker.reset_instance()
        broker = Broker()

        # 创建队列
        broker.create_queue("concurrent-queue")

        # 统计
        total_sent = 0
        total_received = 0
        sent_counts = [0] * producer_count
        received_counts = [0] * consumer_count

        def producer_task(idx):
            """生产者任务"""
            producer = Producer(f"producer-{idx}")
            producer.connect(broker)
            count = 0
            for i in range(message_count):
                producer.send_to_queue("concurrent-queue", {
                    "producer": idx,
                    "index": i,
                })
                count += 1
            sent_counts[idx] = count
            return count

        def consumer_task(idx):
            """消费者任务"""
            consumer = Consumer(f"consumer-{idx}")
            consumer.connect(broker)
            count = 0
            while True:
                msg = consumer.poll("concurrent-queue")
                if msg:
                    count += 1
                else:
                    # 队列为空，检查是否所有生产者都完成
                    if all(s > 0 for s in sent_counts):
                        # 再次尝试
                        msg = consumer.poll("concurrent-queue")
                        if msg:
                            count += 1
                        else:
                            break
                    time.sleep(0.001)  # 短暂等待
            received_counts[idx] = count
            return count

        # 启动消费者
        consumer_threads = []
        for i in range(consumer_count):
            t = threading.Thread(target=consumer_task, args=(i,))
            t.start()
            consumer_threads.append(t)

        # 启动生产者
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=producer_count) as executor:
            producer_futures = [executor.submit(producer_task, i) for i in range(producer_count)]
            for future in as_completed(producer_futures):
                future.result()

        # 等待消费者完成
        for t in consumer_threads:
            t.join(timeout=5)

        end_time = time.time()
        elapsed = end_time - start_time

        total_sent = sum(sent_counts)
        total_received = sum(received_counts)

        print(f"\n总耗时: {elapsed:.4f} 秒")
        print(f"总发送: {total_sent} 条")
        print(f"总接收: {total_received} 条")
        print(f"发送吞吐率: {total_sent / elapsed:.2f} 条/秒")
        print(f"接收吞吐率: {total_received / elapsed:.2f} 条/秒")
        print(f"每个生产者发送: {sent_counts}")
        print(f"每个消费者接收: {received_counts}")

        self.results["concurrent_throughput"] = {
            "producer_count": producer_count,
            "consumer_count": consumer_count,
            "total_sent": total_sent,
            "total_received": total_received,
            "elapsed": elapsed,
            "send_throughput": total_sent / elapsed,
            "receive_throughput": total_received / elapsed,
        }

    def run_all_tests(self):
        """运行所有性能测试"""
        print("\n" + "=" * 60)
        print("开始消息中间件性能测试")
        print("=" * 60)

        self.test_queue_throughput(message_count=1000, rounds=3)
        self.test_topic_throughput(message_count=1000, subscriber_count=3, rounds=3)
        self.test_latency(message_count=100)
        self.test_concurrent_throughput(message_count=500, producer_count=3, consumer_count=3)

        print("\n" + "=" * 60)
        print("性能测试完成")
        print("=" * 60)

        # 打印总结
        print("\n测试结果总结:")
        print("-" * 40)

        if "queue_throughput" in self.results:
            r = self.results["queue_throughput"]
            print(f"队列模式吞吐率:")
            print(f"  发送: {r['avg_send_throughput']:.2f} 条/秒")
            print(f"  接收: {r['avg_receive_throughput']:.2f} 条/秒")

        if "topic_throughput" in self.results:
            r = self.results["topic_throughput"]
            print(f"主题模式吞吐率:")
            print(f"  发送: {r['avg_send_throughput']:.2f} 条/秒")
            print(f"  总投递: {r['avg_total_throughput']:.2f} 条/秒")

        if "latency" in self.results:
            r = self.results["latency"]
            print(f"消息延迟:")
            print(f"  平均: {r['avg_latency']:.4f} ms")
            print(f"  P95: {r['p95_latency']:.4f} ms")
            print(f"  P99: {r['p99_latency']:.4f} ms")

        if "concurrent_throughput" in self.results:
            r = self.results["concurrent_throughput"]
            print(f"并发吞吐率 ({r['producer_count']}P -> {r['consumer_count']}C):")
            print(f"  发送: {r['send_throughput']:.2f} 条/秒")
            print(f"  接收: {r['receive_throughput']:.2f} 条/秒")

        return self.results


def main():
    """主函数"""
    tester = PerformanceTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()
