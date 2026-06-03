"""主程序入口 - 启动消息中间件Web演示界面"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from webapp.app import create_app
from config import WEBAPP_CONFIG


def main():
    """主函数"""
    app = create_app()

    print("=" * 60)
    print("简易消息中间件系统")
    print("=" * 60)
    print(f"访问地址: http://{WEBAPP_CONFIG['host']}:{WEBAPP_CONFIG['port']}/")
    print("功能说明:")
    print("  - 生产者界面: /producer")
    print("  - 消费者界面: /consumer")
    print("  - 队列监控:   /queue")
    print("  - 系统状态:   /api/stats")
    print("=" * 60)

    app.run(
        host=WEBAPP_CONFIG["host"],
        port=WEBAPP_CONFIG["port"],
        debug=WEBAPP_CONFIG["debug"],
    )


if __name__ == "__main__":
    main()
