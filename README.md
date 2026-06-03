# ArchMid_Lab1 - 超市商品管理系统

## 1. 安装依赖
```bash
pip install -r requirements.txt
```

## 2. 初始化数据库
方式 A：用脚本执行 `init_db.sql`
```bash
python init_db.py --host localhost --user root --password "你的密码"
```

方式 B：MySQL 客户端执行
```sql
SOURCE init_db.sql;
```

## 3. 生成测试数据
```bash
python generate_data.py
```

## 4. 启动 Web 服务
```bash
python app_web.py
```
浏览器访问：`http://127.0.0.1:8000/`

## 5. 连接配置（环境变量）
默认：
- DB_HOST=localhost
- DB_USER=root
- DB_PASSWORD=你的密码
- DB_NAME=product_manage_db
- PORT=8000


