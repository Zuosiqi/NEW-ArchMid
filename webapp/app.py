"""Flask Web应用 - 消息中间件演示界面"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, render_template, request, jsonify, redirect, url_for
from broker.broker import Broker
from producer.producer import Producer
from consumer.consumer import Consumer
from models.message import Message


def create_app():
    """创建Flask应用"""
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # 获取Broker单例
    broker = Broker()

    # 存储生产者和消费者实例
    producers = {}
    consumers = {}
    consumer_messages = {}  # 存储每个消费者接收到的消息

    def consumer_callback(consumer_id):
        """创建消费者回调函数"""
        def callback(message):
            if consumer_id not in consumer_messages:
                consumer_messages[consumer_id] = []
            consumer_messages[consumer_id].append(message.to_dict())
            # 只保留最近100条消息
            if len(consumer_messages[consumer_id]) > 100:
                consumer_messages[consumer_id] = consumer_messages[consumer_id][-100:]
        return callback

    # ==================== 页面路由 ====================

    @app.route("/")
    def index():
        """首页"""
        stats = broker.get_stats()
        return render_template("index.html", stats=stats)

    @app.route("/producer")
    def producer_page():
        """生产者页面"""
        stats = broker.get_stats()
        return render_template("producer.html", stats=stats, producers=producers)

    @app.route("/consumer")
    def consumer_page():
        """消费者页面"""
        stats = broker.get_stats()
        return render_template("consumer.html", stats=stats, consumers=consumers,
                               messages=consumer_messages)

    @app.route("/queue")
    def queue_page():
        """队列监控页面"""
        stats = broker.get_stats()
        queue_data = {}
        for name in broker.list_queues():
            q = broker.get_queue(name)
            if q:
                queue_data[name] = {
                    "stats": q.get_stats(),
                    "messages": [m.to_dict() for m in q.get_messages(20)],
                }
        return render_template("queue.html", stats=stats, queue_data=queue_data)

    # ==================== API路由 ====================

    @app.route("/api/producer/create", methods=["POST"])
    def create_producer():
        """创建生产者"""
        data = request.get_json() or {}
        producer_id = data.get("id")
        producer = Producer(producer_id)
        producer.connect(broker)
        producers[producer.id] = producer
        return jsonify({"success": True, "producer_id": producer.id})

    @app.route("/api/producer/send", methods=["POST"])
    def send_message():
        """发送消息"""
        data = request.get_json() or {}
        producer_id = data.get("producer_id")
        destination = data.get("destination", "default")
        payload = data.get("payload", "")
        mode = data.get("mode", "queue")

        if producer_id not in producers:
            return jsonify({"success": False, "error": "生产者不存在"}), 400

        producer = producers[producer_id]
        try:
            result = producer.send(destination, payload, mode)
            return jsonify({
                "success": True,
                "mode": mode,
                "destination": destination,
                "result": result,
            })
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/consumer/create", methods=["POST"])
    def create_consumer():
        """创建消费者"""
        data = request.get_json() or {}
        consumer_id = data.get("id")
        consumer = Consumer(consumer_id)
        consumer.connect(broker)
        consumers[consumer.id] = consumer
        consumer_messages[consumer.id] = []
        return jsonify({"success": True, "consumer_id": consumer.id})

    @app.route("/api/consumer/subscribe", methods=["POST"])
    def subscribe_topic():
        """订阅主题"""
        data = request.get_json() or {}
        consumer_id = data.get("consumer_id")
        topic_name = data.get("topic")

        if consumer_id not in consumers:
            return jsonify({"success": False, "error": "消费者不存在"}), 400

        consumer = consumers[consumer_id]
        callback = consumer_callback(consumer_id)
        consumer.subscribe(topic_name, callback)
        return jsonify({"success": True, "topic": topic_name})

    @app.route("/api/consumer/poll", methods=["POST"])
    def poll_message():
        """从队列拉取消息"""
        data = request.get_json() or {}
        consumer_id = data.get("consumer_id")
        queue_name = data.get("queue", "default")

        if consumer_id not in consumers:
            return jsonify({"success": False, "error": "消费者不存在"}), 400

        consumer = consumers[consumer_id]
        message = consumer.poll(queue_name)
        if message:
            return jsonify({"success": True, "message": message.to_dict()})
        return jsonify({"success": True, "message": None})

    @app.route("/api/consumer/messages/<consumer_id>")
    def get_consumer_messages(consumer_id):
        """获取消费者接收到的消息"""
        limit = request.args.get("limit", 50, type=int)
        messages = consumer_messages.get(consumer_id, [])
        return jsonify({"success": True, "messages": messages[-limit:]})

    @app.route("/api/queue/<queue_name>/messages")
    def get_queue_messages(queue_name):
        """获取队列中的消息"""
        limit = request.args.get("limit", 50, type=int)
        queue = broker.get_queue(queue_name)
        if queue:
            messages = [m.to_dict() for m in queue.get_messages(limit)]
            return jsonify({"success": True, "messages": messages, "stats": queue.get_stats()})
        return jsonify({"success": False, "error": "队列不存在"}), 404

    @app.route("/api/stats")
    def get_stats():
        """获取系统统计信息"""
        return jsonify(broker.get_stats())

    @app.route("/api/queue/create", methods=["POST"])
    def create_queue():
        """创建队列"""
        data = request.get_json() or {}
        name = data.get("name", "default")
        max_size = data.get("max_size", 10000)
        broker.create_queue(name, max_size)
        return jsonify({"success": True, "queue_name": name})

    @app.route("/api/topic/create", methods=["POST"])
    def create_topic():
        """创建主题"""
        data = request.get_json() or {}
        name = data.get("name", "default")
        broker.create_topic(name)
        return jsonify({"success": True, "topic_name": name})

    @app.route("/api/reset", methods=["POST"])
    def reset_system():
        """重置系统"""
        # 断开所有生产者和消费者
        for p in producers.values():
            p.disconnect()
        for c in consumers.values():
            c.disconnect()
        producers.clear()
        consumers.clear()
        consumer_messages.clear()

        # 重置Broker
        Broker.reset_instance()
        return jsonify({"success": True})

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="127.0.0.1", port=8000, debug=True)
