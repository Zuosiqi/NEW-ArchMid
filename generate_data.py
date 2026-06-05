import os
import random
import string
from datetime import datetime, timedelta

import pymysql


def env(key: str, default: str) -> str:
    v = os.getenv(key)
    return default if v is None or v.strip() == "" else v


def get_connection():
    return pymysql.connect(
        host=env("DB_HOST", "localhost"),
        user=env("DB_USER", "root"),
        password=env("DB_PASSWORD", "Z9J7R1R1"),
        database=env("DB_NAME", "product_manage_db"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def random_phone() -> str:
    return "1" + "".join(random.choices(string.digits, k=10))


def random_date(start_date: datetime, end_date: datetime) -> str:
    days_between_dates = (end_date - start_date).days
    random_number_of_days = random.randrange(days_between_dates)
    dt = start_date + timedelta(days=random_number_of_days)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


DEFAULT_CATEGORIES = [
    (1, "食品", "零食饮料、生鲜等"),
    (2, "日用品", "纸品清洁、家居日用等"),
    (3, "数码", "手机电脑及配件"),
    (4, "服饰", "男女装、鞋包"),
    (5, "美妆", "护肤彩妆"),
    (6, "母婴", "母婴用品"),
    (7, "家电", "小家电、大家电"),
    (8, "办公", "文具耗材"),
    (9, "运动", "运动户外"),
    (10, "图书", "图书教材"),
]


"""
商品名生成策略（仅影响 name 字段）：
为彻底避免“跨类别/跨类型乱拼”，改为：每个 category_id 下维护一个“商品类型目录”，
每个商品类型绑定自己的品牌池/卖点池/规格单位池/包装池。
生成时只能从同一商品类型目录中取元素，杜绝类似：
  - 高露洁 洗发水 120抽
  - 斑马 计算器 0.5mm
  - 充电宝 静音版
"""

CATEGORY_ITEM_CATALOG = {
    # 1 食品
    1: [
        {"item": "可乐", "brands": ["可口可乐", "百事"], "tags": ["原味", "无糖"], "specs": ["330ml", "500ml", "1.25L"], "packs": ["6瓶装", "12罐装"]},
        {"item": "矿泉水", "brands": ["农夫山泉", "怡宝", "百岁山"], "tags": ["天然矿泉", "弱碱性"], "specs": ["350ml", "550ml", "1.5L"], "packs": ["6瓶装", "12瓶装"]},
        {"item": "牛奶", "brands": ["伊利", "蒙牛", "光明"], "tags": ["高钙", "纯牛奶"], "specs": ["200ml", "250ml"], "packs": ["12盒装", "24盒装"]},
        {"item": "薯片", "brands": ["乐事", "上好佳", "旺旺"], "tags": ["原味", "烧烤味", "番茄味"], "specs": ["70g", "100g"], "packs": ["分享装"]},
        {"item": "方便面", "brands": ["康师傅", "统一", "白象"], "tags": ["红烧牛肉味", "香辣味"], "specs": ["桶装", "袋装"], "packs": ["5连包"]},
    ],
    # 2 日用品（纸品/清洁/洗护/口腔等：按“商品类型”绑定参数）
    2: [
        {"item": "抽纸", "brands": ["维达", "心相印", "清风"], "tags": ["3层加厚", "柔韧不掉屑"], "specs": ["100抽", "120抽", "150抽"], "packs": ["3包", "6包", "10包"]},
        {"item": "洗衣液", "brands": ["蓝月亮", "立白", "汰渍"], "tags": ["深层去渍", "除菌抑味"], "specs": ["1kg", "2kg", "3kg"], "packs": ["补充装"]},
        {"item": "洗发水", "brands": ["海飞丝", "飘柔", "潘婷", "沙宣"], "tags": ["控油去屑", "柔顺修护"], "specs": ["400ml", "500ml", "750ml"], "packs": ["家庭装"]},
        {"item": "牙膏", "brands": ["高露洁", "佳洁士", "云南白药"], "tags": ["清新口气", "防蛀护龈"], "specs": ["120g", "180g"], "packs": ["2支装"]},
    ],
    # 3 数码（按类型绑定，充电宝不会出现“静音版”）
    3: [
        {"item": "无线鼠标", "brands": ["罗技", "小米", "华为"], "tags": ["静音按键", "人体工学"], "specs": ["2.4G无线", "蓝牙双模"], "packs": ["标准版"]},
        {"item": "充电宝", "brands": ["小米", "安克", "罗马仕"], "tags": ["快充", "大容量"], "specs": ["10000mAh", "20000mAh"], "packs": ["Type-C输入"]},
        {"item": "平板电脑", "brands": ["苹果", "华为", "小米", "联想"], "tags": ["护眼屏", "学习办公"], "specs": ["11英寸", "128GB"], "packs": ["WiFi版"]},
        {"item": "蓝牙耳机", "brands": ["苹果", "华为", "小米", "索尼"], "tags": ["主动降噪", "入耳式"], "specs": ["蓝牙5.3"], "packs": ["充电盒套装"]},
    ],
    # 4 服饰
    4: [
        {"item": "T恤", "brands": ["优衣库", "李宁", "安踏"], "tags": ["纯棉", "透气"], "specs": ["M码", "L码", "XL码"], "packs": ["黑色", "白色"]},
        {"item": "卫衣", "brands": ["耐克", "阿迪达斯", "李宁"], "tags": ["加厚保暖"], "specs": ["M码", "L码", "XL码"], "packs": ["灰色", "黑色"]},
    ],
    # 5 美妆
    5: [
        {"item": "洗面奶", "brands": ["欧莱雅", "珀莱雅", "自然堂"], "tags": ["温和清洁", "控油清爽"], "specs": ["100g", "150g"], "packs": ["标准装"]},
        {"item": "面膜", "brands": ["珀莱雅", "自然堂", "美迪惠尔"], "tags": ["补水保湿", "提亮肤色"], "specs": ["10片装", "20片装"], "packs": ["礼盒装"]},
    ],
    # 6 母婴（严格母婴语义）
    6: [
        {"item": "纸尿裤", "brands": ["帮宝适", "好奇"], "tags": ["超薄透气款", "干爽不侧漏", "柔软亲肤"], "specs": ["M码", "L码", "XL码"], "packs": ["纸尿裤装"]},
        {"item": "奶粉", "brands": ["飞鹤", "合生元", "爱他美", "伊利金领冠"], "tags": ["配方升级", "易吸收"], "specs": ["1段", "2段", "3段"], "packs": ["800g罐装"]},
        {"item": "婴儿湿巾", "brands": ["好奇", "全棉时代", "贝亲"], "tags": ["无香精配方", "柔软亲肤"], "specs": ["80抽", "100抽"], "packs": ["带盖装"]},
    ],
    # 7 家电（静音只用于适合静音的品类）
    7: [
        {"item": "空气炸锅", "brands": ["美的", "苏泊尔", "九阳", "小熊"], "tags": ["免油炸", "多功能"], "specs": ["3.5L", "4.5L", "5L"], "packs": ["易清洗"]},
        {"item": "电风扇", "brands": ["格力", "美的", "艾美特"], "tags": ["静音设计", "多档风速"], "specs": ["落地款", "台式款"], "packs": ["遥控版"]},
        {"item": "加湿器", "brands": ["小米", "飞利浦", "美的"], "tags": ["静音运行", "大雾量"], "specs": ["3L", "4L"], "packs": ["家用款"]},
    ],
    # 8 办公/文具（严格绑定参数：0.5mm 只给笔类）
    8: [
        {"item": "中性笔", "brands": ["得力", "晨光", "斑马", "百乐"], "tags": ["顺滑书写"], "specs": ["0.5mm", "0.7mm"], "packs": ["12支装", "24支装"]},
        {"item": "笔记本", "brands": ["国誉", "得力", "晨光"], "tags": ["加厚纸张"], "specs": ["A5", "B5"], "packs": ["80页", "120页"]},
        {"item": "计算器", "brands": ["卡西欧", "得力"], "tags": ["大屏显示", "太阳能"], "specs": ["12位"], "packs": ["办公款"]},
    ],
    # 9 运动
    9: [
        {"item": "瑜伽垫", "brands": ["迪卡侬", "李宁", "安踏"], "tags": ["防滑加厚"], "specs": ["6mm", "8mm", "10mm"], "packs": ["环保材质"]},
        {"item": "篮球", "brands": ["耐克", "斯伯丁", "李宁"], "tags": ["耐磨防滑"], "specs": ["7号球"], "packs": ["室内外通用"]},
    ],
    # 10 图书
    10: [
        {"item": "数据库系统概论", "brands": ["高等教育出版社", "机械工业出版社"], "tags": ["教材", "配套实验"], "specs": ["第7版", "第4版"], "packs": ["平装"]},
        {"item": "算法导论", "brands": ["机械工业出版社"], "tags": ["经典教材"], "specs": ["第3版"], "packs": ["精装"]},
    ],
}


def make_product_name(category_id: int) -> str:
    """
    生成“更像真实商品”的中文名称，并与类别匹配。
    例：'小米 无线鼠标 2.4G 静音版'、'得力 中性笔 0.5mm 黑色 12支装'
    """
    candidates = CATEGORY_ITEM_CATALOG.get(category_id)
    if not candidates:
        return "无品牌 商品 标准版"

    it = random.choice(candidates)
    brand = random.choice(it["brands"])
    item = it["item"]

    tags = it.get("tags", [])
    specs = it.get("specs", [])
    packs = it.get("packs", [])

    parts = [brand, item]
    if tags and random.random() < 0.9:
        parts.append(random.choice(tags))
    if specs and random.random() < 0.85:
        parts.append(random.choice(specs))
    if packs and random.random() < 0.6:
        parts.append(random.choice(packs))

    return " ".join(parts).strip()


def generate_data():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 检测是否存在库存流水表（新实体）
            cursor.execute("SELECT DATABASE() AS db")
            row = cursor.fetchone()
            if not row:
                raise RuntimeError("SELECT DATABASE() 返回为空，数据库连接异常")
            db = row["db"]
            cursor.execute(
                """
                SELECT COUNT(*) AS c
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME='StockLedger'
                """,
                (db,),
            )
            row = cursor.fetchone()
            has_ledger = int(row["c"]) > 0 if row else False

            # 1) 确保 Category 已存在（优先使用 init_db.sql 预置的中文类别）
            cursor.execute("SELECT COUNT(*) AS c FROM Category")
            row = cursor.fetchone()
            if int(row["c"]) == 0 if row else True:
                print("Inserting default Categories...")
                cursor.executemany(
                    "INSERT INTO Category (category_id, name, description) VALUES (%s, %s, %s)",
                    DEFAULT_CATEGORIES,
                )
                conn.commit()

            # 2. Suppliers (50)
            print("Generating Suppliers...")
            suppliers = []
            phones = set()
            while len(suppliers) < 50:
                phone = random_phone()
                if phone not in phones:
                    phones.add(phone)
                    suppliers.append(
                        (
                            f"Supplier_{random_string(5)}",
                            f"Contact_{random_string(3)}",
                            phone,
                            f"Address_{random_string(10)}",
                        )
                    )
            cursor.executemany(
                "INSERT INTO Supplier (name, contact_person, phone, address) VALUES (%s, %s, %s, %s)",
                suppliers,
            )
            conn.commit()

            # 3) Products（中文真实商品名 + 与类别匹配）
            print("Generating Products...")
            products = []
            barcodes = set()
            while len(products) < 10000:
                barcode = random_string(13)
                if barcode not in barcodes:
                    barcodes.add(barcode)
                    cat_id = random.randint(1, 10)
                    price = round(random.uniform(10.0, 500.0), 2)
                    # 成本价：用于利润分析（一般低于售价）
                    cost_price = round(price * random.uniform(0.4, 0.85), 2)
                    stock = random.randint(10, 1000)
                    status = random.choice(["在售", "停售"])
                    name = make_product_name(cat_id)
                    products.append((name, barcode, cat_id, price, cost_price, stock, status))
            cursor.executemany(
                "INSERT INTO Product (name, barcode, category_id, price, cost_price, stock_quantity, status) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                products,
            )
            conn.commit()

            # 3.1) 初始化库存流水（让“库存余额趋势”可真实计算）
            if has_ledger:
                print("Writing initial StockLedger (init)...")
                # 把“初始化入库”回填到 14 天前，避免看板把 init 当成“今天突然暴增”
                init_time = (datetime.now() - timedelta(days=13)).strftime("%Y-%m-%d 09:00:00")
                cursor.execute("SELECT product_id, stock_quantity, cost_price FROM Product")
                init_rows = []
                for row in cursor.fetchall():
                    qty = int(row["stock_quantity"] or 0)
                    if qty > 0:
                        init_rows.append((row["product_id"], qty, float(row["cost_price"] or 0), "init", None, None, None, "初始化入库", init_time))
                # 分批写入避免过大
                chunk = 5000
                for i in range(0, len(init_rows), chunk):
                    cursor.executemany(
                        """
                        INSERT INTO StockLedger
                            (product_id, change_qty, unit_cost, ref_type, ref_id, sale_order_id, purchase_id, note, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        init_rows[i : i + chunk],
                    )
                conn.commit()

            # 4. Supplies (20000)
            print("Generating Supplies...")
            supplies = []
            supply_set = set()
            while len(supplies) < 20000:
                p_id = random.randint(1, 10000)
                s_id = random.randint(1, 50)
                pair = (p_id, s_id)
                if pair not in supply_set:
                    supply_set.add(pair)
                    price = round(random.uniform(5.0, 450.0), 2)
                    supplies.append((p_id, s_id, price))
            cursor.executemany("INSERT INTO Supply (product_id, supplier_id, supply_price) VALUES (%s, %s, %s)", supplies)
            conn.commit()

            # 5. Employees (50)
            print("Generating Employees...")
            employees = []
            phones.clear()
            while len(employees) < 50:
                phone = random_phone()
                if phone not in phones:
                    phones.add(phone)
                    employees.append(
                        (
                            f"Emp_{random_string(4)}",
                            random.choice(["收银员", "库管", "经理"]),
                            phone,
                            random_date(datetime(2020, 1, 1), datetime(2026, 1, 1)).split()[0],
                        )
                    )
            cursor.executemany("INSERT INTO Employee (name, role, phone, hire_date) VALUES (%s, %s, %s, %s)", employees)
            conn.commit()

            # 6. Customers (5000)
            print("Generating Customers...")
            customers = []
            phones.clear()
            while len(customers) < 5000:
                phone = random_phone()
                if phone not in phones:
                    phones.add(phone)
                    customers.append(
                        (f"Cust_{random_string(5)}", phone, random.randint(0, 5000), random.choice(["普通会员", "白银会员", "黄金会员"]))
                    )
            cursor.executemany("INSERT INTO Customer (name, phone, points, level) VALUES (%s, %s, %s, %s)", customers)
            conn.commit()

            # 7. SalesOrders and OrderDetails (30000 orders)
            print("Generating Orders and Details...")
            cursor.execute("SELECT product_id, price, cost_price FROM Product")
            prod_map = {row["product_id"]: {"price": row["price"], "cost_price": row["cost_price"]} for row in cursor.fetchall()}

            # 为了让“近14天趋势图”必然有数据：订单日期分布向近期倾斜
            now = datetime.now()
            start_recent = now - timedelta(days=30)
            start_history = now - timedelta(days=365)
            end_date = now

            orders = []
            for _ in range(30000):
                c_id = random.randint(1, 5000)
                e_id = random.randint(1, 50)
                # 80% 订单集中在近30天，20% 分布在近1年（更贴近真实业务）
                if random.random() < 0.8:
                    o_date = random_date(start_recent, end_date)
                else:
                    o_date = random_date(start_history, start_recent)
                orders.append((c_id, e_id, o_date))

            print("Inserting Orders...")
            chunk_size = 5000
            for i in range(0, len(orders), chunk_size):
                cursor.executemany(
                    "INSERT INTO SalesOrder (customer_id, emp_id, order_date, total_amount) VALUES (%s, %s, %s, 0)",
                    orders[i : i + chunk_size],
                )
            conn.commit()

            # 取回 order_id 与 order_date（用于库存流水的 created_at 对齐到真实发生时间）
            cursor.execute("SELECT order_id, order_date FROM SalesOrder")
            order_rows = cursor.fetchall()
            order_ids = [row["order_id"] for row in order_rows]
            order_date_map = {row["order_id"]: row["order_date"].strftime("%Y-%m-%d %H:%M:%S") for row in order_rows}

            print("Inserting OrderDetails...")
            details = []
            order_updates = []
            ledger_rows = []
            for o_id in order_ids:
                num_items = random.randint(1, 5)
                total_amount = 0.0
                o_date = order_date_map[o_id]
                for _ in range(num_items):
                    p_id = random.randint(1, 10000)
                    qty = random.randint(1, 10)
                    unit_price = prod_map[p_id]["price"]
                    unit_cost = prod_map[p_id]["cost_price"] or 0
                    total_amount += float(unit_price) * qty
                    details.append((o_id, p_id, qty, unit_price, unit_cost))
                    if has_ledger:
                        # created_at 与订单时间对齐，便于按日计算库存余额趋势
                        ledger_rows.append((p_id, -qty, float(unit_cost or 0), "sale", o_id, o_id, None, "模拟销售出库", o_date))
                order_updates.append((total_amount, o_id))

            for i in range(0, len(details), chunk_size * 2):
                cursor.executemany(
                    "INSERT INTO OrderDetail (order_id, product_id, quantity, unit_price, unit_cost) VALUES (%s, %s, %s, %s, %s)",
                    details[i : i + chunk_size * 2],
                )
            conn.commit()

            if has_ledger and ledger_rows:
                print("Writing sale StockLedger (sale)...")
                chunk = 10000
                for i in range(0, len(ledger_rows), chunk):
                    cursor.executemany(
                        """
                        INSERT INTO StockLedger
                            (product_id, change_qty, unit_cost, ref_type, ref_id, sale_order_id, purchase_id, note, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        ledger_rows[i : i + chunk],
                    )
                conn.commit()

            print("Updating Order Totals...")
            for i in range(0, len(order_updates), chunk_size):
                cursor.executemany("UPDATE SalesOrder SET total_amount = %s WHERE order_id = %s", order_updates[i : i + chunk_size])
            conn.commit()

            print("✅ Data generation complete!")
    finally:
        conn.close()


if __name__ == "__main__":
    generate_data()
