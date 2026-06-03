"""配置文件 - 消息中间件系统配置"""

# Broker配置
BROKER_CONFIG = {
    "default_queue_max_size": 10000,  # 默认队列最大容量
    "enable_persistence": False,      # 是否启用持久化
    "log_level": "INFO",              # 日志级别
}

# Web应用配置
WEBAPP_CONFIG = {
    "host": "127.0.0.1",
    "port": 8000,
    "debug": True,
}

# 性能测试配置
PERFORMANCE_CONFIG = {
    "test_message_count": 1000,       # 测试消息数量
    "test_rounds": 3,                 # 测试轮数
    "warmup_messages": 100,           # 预热消息数
}
