import os
import time
from decimal import Decimal

from flask import Flask, jsonify, render_template, request
import pymysql


app = Flask(__name__)


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


def _to_jsonable_value(v):
    if isinstance(v, Decimal):
        return float(v)
    return v


def _to_jsonable_row(row: dict) -> dict:
    return {k: _to_jsonable_value(v) for k, v in row.items()}


def _column_exists(cursor, table_name: str, column_name: str) -> bool:
    """
    兼容性处理：用于判断数据库表是否已升级到包含某字段。
    避免因同学本机未重新执行 init_db.sql 导致 1054 Unknown column。
    """
    cursor.execute("SELECT DATABASE() AS db")
    db = cursor.fetchone()["db"]
    cursor.execute(
        """
        SELECT COUNT(*) AS c
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
        """,
        (db, table_name, column_name),
    )
    return int(cursor.fetchone()["c"]) > 0


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute("SELECT DATABASE() AS db")
    db = cursor.fetchone()["db"]
    cursor.execute(
        """
        SELECT COUNT(*) AS c
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
        """,
        (db, table_name),
    )
    return int(cursor.fetchone()["c"]) > 0


def _integrity_error_to_zh_message(e: pymysql.err.IntegrityError, *, barcode=None, category_id=None):

    error_code = e.args[0] if e.args else None

    # 1062: Duplicate entry
    if error_code == 1062:
        if barcode:
            return f"操作失败：条形码 '{barcode}' 已存在！<br>原因：违反 UNIQUE 唯一约束。"
        return "操作失败：数据已存在重复值！<br>原因：违反 UNIQUE 唯一约束。"

    # 1452: Cannot add or update a child row: a foreign key constraint fails
    if error_code == 1452:
        if category_id is not None:
            return f"操作失败：类别ID '{category_id}' 不存在！<br>原因：违反 FOREIGN KEY 外键约束。"
        return "操作失败：外键引用无效！<br>原因：违反 FOREIGN KEY 外键约束。"

    # 1451: Cannot delete or update a parent row
    if error_code == 1451:
        return "操作失败：该记录已被其它表引用，无法删除/更新。<br>原因：违反 FOREIGN KEY 外键约束（请先删除关联记录）。"

    # 1048: Column cannot be null
    if error_code == 1048:
        return "操作失败：存在必填字段为空（NULL）。<br>原因：违反 NOT NULL 非空约束。"

    # 3819: Check constraint is violated (MySQL 8.0.16+)
    if error_code == 3819:
        return "操作失败：数据不合法（例如价格/库存为负数）。<br>原因：违反 CHECK 检查约束。"

    # 1406: Data too long for column
    if error_code == 1406:
        return "操作失败：输入内容过长，超出字段长度限制。<br>原因：字段长度约束（VARCHAR 长度）。"

    return f"数据库完整性错误：{e}"


def _validate_product_input_for_create(data: dict):
    """
    应用层参数校验（创建）：在进入数据库前先把“必填/类型/范围/枚举”拦住并给中文提示。
    """
    name = (data.get("name") or "").strip()
    barcode = (data.get("barcode") or "").strip()

    if not name:
        return None, "❌ 商品名称不能为空！（应用层校验）"
    if not barcode:
        return None, "❌ 条形码不能为空！（应用层校验）"

    if len(name) > 100:
        return None, "❌ 商品名称过长（最多 100 个字符）！（应用层校验）"
    if len(barcode) > 50:
        return None, "❌ 条形码过长（最多 50 个字符）！（应用层校验）"

    try:
        cat_id = int(data.get("category_id"))
    except (ValueError, TypeError):
        return None, "❌ 类别ID必须是有效的整数！（应用层校验）"

    try:
        price = float(data.get("price"))
    except (ValueError, TypeError):
        return None, "❌ 价格必须是有效的数字！（应用层校验）"

    try:
        stock = int(data.get("stock"))
    except (ValueError, TypeError):
        return None, "❌ 库存必须是有效的整数！（应用层校验）"

    if price < 0:
        return None, "❌ 价格不能为负数！（应用层校验）"
    if stock < 0:
        return None, "❌ 库存不能为负数！（应用层校验）"

    # 可选字段：成本价（用于利润分析）。不要求前端填写，默认 0。
    cost_price_raw = data.get("cost_price", None)
    if cost_price_raw is None or str(cost_price_raw).strip() == "":
        cost_price = 0.0
    else:
        try:
            cost_price = float(cost_price_raw)
        except (ValueError, TypeError):
            return None, "❌ 成本价必须是有效的数字！（应用层校验）"
        if cost_price < 0:
            return None, "❌ 成本价不能为负数！（应用层校验）"

    return {
        "name": name,
        "barcode": barcode,
        "category_id": cat_id,
        "price": price,
        "cost_price": cost_price,
        "stock": stock,
    }, ""


def _validate_product_input_for_update(data: dict):
    """
    应用层参数校验（更新）：比创建多一个 status 枚举校验。
    """
    payload, msg = _validate_product_input_for_create(
        {
            "name": data.get("name"),
            "barcode": data.get("barcode"),
            "category_id": data.get("category_id"),
            "price": data.get("price"),
            "cost_price": data.get("cost_price"),
            # 更新用 stock_quantity（前端编辑弹窗发这个字段）
            "stock": data.get("stock_quantity"),
        }
    )
    if payload is None:
        return None, msg

    status = (data.get("status") or "在售").strip() or "在售"
    if status not in ("在售", "停售"):
        return None, "❌ 状态只能是“在售/停售”！（应用层校验）"

    payload["status"] = status
    return payload, ""


@app.route("/")
def index():
    return render_template("index.html")


# =========================
# 基础：健康检查
# =========================
@app.route("/api/health", methods=["GET"])
def health():
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 AS ok")
            data = cursor.fetchone()
        conn.close()
        return jsonify({"success": True, "data": data, "message": "✅ 数据库连接正常"})
    except Exception as e:
        return jsonify({"success": False, "message": f"❌ 数据库连接失败：{e}"})


# =========================
# 业务下拉选项
# =========================
@app.route("/api/options/customers", methods=["GET"])
def options_customers():
    """
    下拉选项：顾客（会员）
    返回：[{id, name, level, display_name}]
    """
    q = (request.args.get("q") or "").strip()
    limit = request.args.get("limit", default=200, type=int)
    limit = min(max(limit, 10), 500)
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            if q:
                cursor.execute(
                    """
                    SELECT customer_id, name, level
                    FROM Customer
                    WHERE name LIKE %s
                    ORDER BY customer_id ASC
                    LIMIT %s
                    """,
                    (f"%{q}%", limit),
                )
            else:
                cursor.execute(
                    """
                    SELECT customer_id, name, level
                    FROM Customer
                    ORDER BY customer_id ASC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cursor.fetchall()
        conn.close()
        data = [{"id": r["customer_id"], "name": r["name"], "level": r.get("level") or "", "display_name": f"{r['name']}（{r.get('level') or '会员'}）"} for r in rows]
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "message": f"获取顾客列表失败：{type(e).__name__}"})


@app.route("/api/options/employees", methods=["GET"])
def options_employees():
    """
    下拉选项：员工
    返回：[{id, name, role, display_name}]
    """
    q = (request.args.get("q") or "").strip()
    limit = request.args.get("limit", default=200, type=int)
    limit = min(max(limit, 10), 500)
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            if q:
                cursor.execute(
                    """
                    SELECT emp_id, name, role
                    FROM Employee
                    WHERE name LIKE %s
                    ORDER BY emp_id ASC
                    LIMIT %s
                    """,
                    (f"%{q}%", limit),
                )
            else:
                cursor.execute(
                    """
                    SELECT emp_id, name, role
                    FROM Employee
                    ORDER BY emp_id ASC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cursor.fetchall()
        conn.close()
        data = [{"id": r["emp_id"], "name": r["name"], "role": r.get("role") or "", "display_name": f"{r['name']}（{r.get('role') or '员工'}）"} for r in rows]
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "message": f"获取员工列表失败：{type(e).__name__}"})


@app.route("/api/options/suppliers", methods=["GET"])
def options_suppliers():
    """
    下拉选项：供应商
    返回：[{id, name, display_name}]
    """
    q = (request.args.get("q") or "").strip()
    limit = request.args.get("limit", default=200, type=int)
    limit = min(max(limit, 10), 500)
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            if q:
                cursor.execute(
                    """
                    SELECT supplier_id, name
                    FROM Supplier
                    WHERE name LIKE %s
                    ORDER BY supplier_id ASC
                    LIMIT %s
                    """,
                    (f"%{q}%", limit),
                )
            else:
                cursor.execute(
                    """
                    SELECT supplier_id, name
                    FROM Supplier
                    ORDER BY supplier_id ASC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cursor.fetchall()
        conn.close()
        data = [{"id": r["supplier_id"], "name": r["name"], "display_name": r["name"]} for r in rows]
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "message": f"获取供应商列表失败：{type(e).__name__}"})


@app.route("/api/options/products", methods=["GET"])
def options_products():
    """
    下拉选项：商品
    可选参数：
      - active_only=1：仅返回“在售”商品（用于销售开单）
    返回：[{id, name, status, price, stock_quantity, display_name}]
    """
    active_only = request.args.get("active_only", default="0").strip() in ("1", "true", "True")
    q = (request.args.get("q") or "").strip()
    limit = request.args.get("limit", default=500, type=int)
    limit = min(max(limit, 50), 2000)

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            where = []
            params = []
            if active_only:
                where.append("p.status='在售'")
            if q:
                where.append("p.name LIKE %s")
                params.append(f"%{q}%")

            where_sql = ("WHERE " + " AND ".join(where)) if where else ""
            sql = f"""
                SELECT
                    p.product_id,
                    p.name,
                    p.status,
                    p.price,
                    p.stock_quantity
                FROM Product p
                {where_sql}
                ORDER BY p.product_id ASC
                LIMIT %s
            """
            params.append(limit)
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        conn.close()

        data = []
        for r in rows:
            pid = r["product_id"]
            name = r["name"]
            status = r.get("status") or ""
            price = float(r.get("price") or 0)
            stock = int(r.get("stock_quantity") or 0)
            # 展示名：门店用户更关心名称/价格/库存（不展示ID）
            display = f"{name}（￥{price:.2f}，库存{stock}）" if status == "在售" else f"{name}（{status}）"
            data.append(
                {
                    "id": pid,
                    "name": name,
                    "status": status,
                    "price": price,
                    "stock_quantity": stock,
                    "display_name": display,
                }
            )
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "message": f"获取商品列表失败：{type(e).__name__}"})


# =========================
# 接口：保持返回结构不变
# =========================
@app.route("/api/products", methods=["GET"])
def get_products():

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM v_product_detail ORDER BY product_id DESC LIMIT 100")
            data = [_to_jsonable_row(r) for r in cursor.fetchall()]
        conn.close()
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/products", methods=["POST"])
def add_product():

    data = request.json or {}
    payload, msg = _validate_product_input_for_create(data)
    if payload is None:
        return jsonify({"success": False, "message": msg})

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            # 应用层提前校验外键：给出更明确的中文提示（同时数据库层仍会兜底）
            cursor.execute("SELECT 1 FROM Category WHERE category_id=%s", (payload["category_id"],))
            if cursor.fetchone() is None:
                conn.close()
                return jsonify(
                    {
                        "success": False,
                        "message": f"❌ 类别ID '{payload['category_id']}' 不存在！（应用层校验）<br>请先在 Category 表中创建该类别，或选择 1~10 的有效类别ID。",
                    }
                )

            sql = """
            INSERT INTO Product (name, barcode, category_id, price, cost_price, stock_quantity)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(
                sql,
                (
                    payload["name"],
                    payload["barcode"],
                    payload["category_id"],
                    payload["price"],
                    payload["cost_price"],
                    payload["stock"],
                ),
            )
        conn.commit()
        conn.close()
        return jsonify({"success": True, "message": "✅ 商品添加成功！"})
    except pymysql.err.IntegrityError as e:
        return jsonify(
            {
                "success": False,
                "message": _integrity_error_to_zh_message(
                    e, barcode=payload.get("barcode"), category_id=payload.get("category_id")
                ),
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": f"未知错误: {e}"})


@app.route("/api/products/<int:product_id>", methods=["PUT"])
def update_product(product_id: int):
    data = request.json or {}
    payload, msg = _validate_product_input_for_update(data)
    if payload is None:
        return jsonify({"success": False, "message": msg})

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM Product WHERE product_id=%s", (product_id,))
            if cursor.fetchone() is None:
                conn.close()
                return jsonify({"success": False, "message": f"更新失败：商品不存在（ID={product_id}）"})

            cursor.execute("SELECT 1 FROM Category WHERE category_id=%s", (payload["category_id"],))
            if cursor.fetchone() is None:
                conn.close()
                return jsonify(
                    {
                        "success": False,
                        "message": f"❌ 类别ID '{payload['category_id']}' 不存在！（应用层校验）<br>请先创建该类别，或选择有效类别ID。",
                    }
                )

            cursor.execute(
                """
                UPDATE Product
                SET name=%s, barcode=%s, category_id=%s, price=%s, cost_price=%s, stock_quantity=%s, status=%s
                WHERE product_id=%s
                """,
                (
                    payload["name"],
                    payload["barcode"],
                    payload["category_id"],
                    payload["price"],
                    payload["cost_price"],
                    payload["stock"],
                    payload["status"],
                    product_id,
                ),
            )
        conn.commit()
        conn.close()
        return jsonify({"success": True, "message": "✅ 商品更新成功！"})
    except pymysql.err.IntegrityError as e:
        return jsonify(
            {
                "success": False,
                "message": _integrity_error_to_zh_message(
                    e, barcode=payload.get("barcode"), category_id=payload.get("category_id")
                ),
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": f"未知错误: {e}"})


@app.route("/api/products/<int:product_id>", methods=["GET"])
def get_product_detail(product_id: int):
    """
    获取单个商品详情（用于验收入口：下单前后对比库存等）。
    """
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM v_product_detail WHERE product_id=%s", (product_id,))
            row = cursor.fetchone()
        conn.close()
        if row is None:
            return jsonify({"success": False, "message": f"商品不存在（product_id={product_id}）"})
        return jsonify({"success": True, "data": _to_jsonable_row(row)})
    except Exception as e:
        return jsonify({"success": False, "message": f"查询失败：{type(e).__name__}"})


@app.route("/api/stock_ledger", methods=["GET"])
def get_stock_ledger():
    """
    查询某商品的库存流水（用于验收：展示“销售出库/撤销回补/进货入库”等流水证据）。
    GET /api/stock_ledger?product_id=1&limit=20
    """
    product_id = request.args.get("product_id", type=int)
    limit = request.args.get("limit", default=20, type=int)
    limit = min(max(limit, 1), 200)

    if product_id is None:
        return jsonify({"success": False, "message": "参数错误：product_id 必填"})

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            if not _table_exists(cursor, "StockLedger"):
                conn.close()
                return jsonify({"success": False, "message": "当前数据库未创建 StockLedger 表，无法查询库存流水"})

            cursor.execute(
                """
                SELECT
                    ledger_id, product_id, change_qty, unit_cost,
                    ref_type, ref_id, note, created_at
                FROM StockLedger
                WHERE product_id=%s
                ORDER BY created_at DESC, ledger_id DESC
                LIMIT %s
                """,
                (product_id, limit),
            )
            rows = [_to_jsonable_row(r) for r in cursor.fetchall()]
        conn.close()
        return jsonify({"success": True, "data": rows})
    except Exception as e:
        return jsonify({"success": False, "message": f"查询库存流水失败：{type(e).__name__}"})


@app.route("/api/products/<int:product_id>", methods=["DELETE"])
def delete_product(product_id: int):
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM Product WHERE product_id=%s", (product_id,))
            affected = cursor.rowcount
        conn.commit()
        conn.close()
        if affected == 0:
            return jsonify({"success": False, "message": f"删除失败：商品不存在（ID={product_id}）"})
        return jsonify({"success": True, "message": "✅ 商品删除成功！"})
    except pymysql.err.IntegrityError as e:
        return jsonify({"success": False, "message": _integrity_error_to_zh_message(e)})
    except Exception as e:
        return jsonify({"success": False, "message": f"未知错误: {e}"})


# =========================
# 复杂查询
# =========================
@app.route("/api/complex_query", methods=["GET"])
def complex_query():
    sql = """
    SELECT
        c.name AS category_name,
        p.name AS product_name,
        SUM(od.quantity) AS total_sold_quantity,
        SUM(od.quantity * od.unit_price) AS total_sales_amount
    FROM OrderDetail od
    JOIN Product p ON od.product_id = p.product_id
    JOIN Category c ON p.category_id = c.category_id
    GROUP BY c.category_id, p.product_id
    ORDER BY total_sales_amount DESC
    LIMIT 10;
    """
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            for row in data:
                row["total_sales_amount"] = float(row["total_sales_amount"])
        conn.close()
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


# =========================
# 事务下单：SalesOrder + OrderDetail（核心业务闭环）
# =========================
def _validate_create_order_payload(data: dict):
    """
    订单创建参数校验：
    - customer_id/emp_id 必填、整数、存在
    - items 必填、列表、至少 1 项
    - 每项 product_id/quantity 校验（整数、范围、存在、库存足够由事务内校验）
    """
    if not isinstance(data, dict):
        return None, "❌ 请求体必须是 JSON 对象！（应用层校验）"

    try:
        customer_id = int(data.get("customer_id"))
    except (ValueError, TypeError):
        return None, "❌ customer_id 必须是有效整数！（应用层校验）"

    try:
        emp_id = int(data.get("emp_id"))
    except (ValueError, TypeError):
        return None, "❌ emp_id 必须是有效整数！（应用层校验）"

    items = data.get("items")
    if not isinstance(items, list) or len(items) == 0:
        return None, "❌ items 必须是非空数组！（应用层校验）"
    if len(items) > 50:
        return None, "❌ 单笔订单明细过多（最多 50 行）！（应用层校验）"

    normalized_items = []
    for i, it in enumerate(items, start=1):
        if not isinstance(it, dict):
            return None, f"❌ 第 {i} 条明细必须是对象！（应用层校验）"
        try:
            product_id = int(it.get("product_id"))
        except (ValueError, TypeError):
            return None, f"❌ 第 {i} 条明细 product_id 必须是整数！（应用层校验）"
        try:
            qty = int(it.get("quantity"))
        except (ValueError, TypeError):
            return None, f"❌ 第 {i} 条明细 quantity 必须是整数！（应用层校验）"
        if qty <= 0:
            return None, f"❌ 第 {i} 条明细 quantity 必须大于 0！（应用层校验）"
        if qty > 999999:
            return None, f"❌ 第 {i} 条明细 quantity 过大！（应用层校验）"
        normalized_items.append({"product_id": product_id, "quantity": qty})

    return {"customer_id": customer_id, "emp_id": emp_id, "items": normalized_items}, ""


@app.route("/api/orders", methods=["POST"])
def create_order():
    """
    核心业务闭环：创建销售订单（主表+明细表）并自动扣减库存、回写订单总额。
    事务控制：任一环节失败（库存不足/外键不存在/约束失败等）整体回滚。
    """
    data = request.json or {}
    payload, msg = _validate_create_order_payload(data)
    if payload is None:
        return jsonify({"success": False, "message": msg})

    try:
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                has_ledger = _table_exists(cursor, "StockLedger")

                # 1) 校验客户/员工存在（应用层预校验 + 数据库层兜底）
                cursor.execute("SELECT 1 FROM Customer WHERE customer_id=%s", (payload["customer_id"],))
                if cursor.fetchone() is None:
                    conn.rollback()
                    return jsonify({"success": False, "message": f"❌ customer_id={payload['customer_id']} 不存在！（应用层校验）"})

                cursor.execute("SELECT 1 FROM Employee WHERE emp_id=%s", (payload["emp_id"],))
                if cursor.fetchone() is None:
                    conn.rollback()
                    return jsonify({"success": False, "message": f"❌ emp_id={payload['emp_id']} 不存在！（应用层校验）"})

                # 2) 先创建订单主表（total_amount 先写 0，后回写）
                cursor.execute(
                    "INSERT INTO SalesOrder (customer_id, emp_id, total_amount, status) VALUES (%s, %s, 0, '已完成')",
                    (payload["customer_id"], payload["emp_id"]),
                )
                order_id = cursor.lastrowid

                # 3) 锁定商品行并扣库存（SELECT ... FOR UPDATE 防并发超卖）
                total_amount = 0.0
                details = []
                ledger_rows = []

                for it in payload["items"]:
                    pid = it["product_id"]
                    qty = it["quantity"]

                    cursor.execute(
                        """
                        SELECT product_id, price, cost_price, stock_quantity, status
                        FROM Product
                        WHERE product_id=%s
                        FOR UPDATE
                        """,
                        (pid,),
                    )
                    prod = cursor.fetchone()
                    if prod is None:
                        raise ValueError(f"❌ 商品不存在：product_id={pid}（应用层校验）")

                    if prod["status"] != "在售":
                        raise ValueError(f"❌ 商品当前不可售（状态={prod['status']}）：product_id={pid}")

                    stock = int(prod["stock_quantity"])
                    if stock < qty:
                        raise ValueError(f"❌ 库存不足：product_id={pid}，当前库存={stock}，需要={qty}")

                    unit_price = float(prod["price"])
                    unit_cost = float(prod.get("cost_price") or 0)
                    line_amount = unit_price * qty
                    total_amount += line_amount

                    # 扣库存
                    cursor.execute(
                        "UPDATE Product SET stock_quantity = stock_quantity - %s WHERE product_id=%s",
                        (qty, pid),
                    )

                    details.append((order_id, pid, qty, unit_price, unit_cost))
                    if has_ledger:
                        ledger_rows.append((pid, -qty, unit_cost, "sale", order_id, "销售出库"))

                # 4) 插入明细（批量）
                cursor.executemany(
                    "INSERT INTO OrderDetail (order_id, product_id, quantity, unit_price, unit_cost) VALUES (%s, %s, %s, %s, %s)",
                    details,
                )

                # 4.1 写入库存流水（批量）
                if has_ledger and ledger_rows:
                    cursor.executemany(
                        """
                        INSERT INTO StockLedger
                            (product_id, change_qty, unit_cost, ref_type, ref_id, sale_order_id, purchase_id, note)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        # sale_order_id 绑定 order_id，purchase_id 为空
                        [(pid, chg, uc, rt, rid, rid if rt == "sale" else None, None, note) for (pid, chg, uc, rt, rid, note) in ledger_rows],
                    )

                # 5) 回写总额
                cursor.execute("UPDATE SalesOrder SET total_amount=%s WHERE order_id=%s", (total_amount, order_id))

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return jsonify(
            {
                "success": True,
                "message": "✅ 订单创建成功（事务提交）",
                "data": {"order_id": order_id, "total_amount": round(total_amount, 2)},
            }
        )
    except pymysql.err.IntegrityError as e:
        return jsonify({"success": False, "message": _integrity_error_to_zh_message(e)})
    except ValueError as e:
        return jsonify({"success": False, "message": str(e)})
    except Exception as e:
        return jsonify({"success": False, "message": f"未知错误：{e}"})


@app.route("/api/orders/<int:order_id>", methods=["GET"])
def get_order(order_id: int):
    """
    查询订单（用于演示“撤销前后对比/明细核对”）。
    返回：订单主信息 + 明细列表（含商品名称）。
    """
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    o.order_id,
                    o.order_date,
                    o.total_amount,
                    o.status,
                    o.customer_id,
                    c.name AS customer_name,
                    o.emp_id,
                    e.name AS emp_name
                FROM SalesOrder o
                JOIN Customer c ON c.customer_id = o.customer_id
                JOIN Employee e ON e.emp_id = o.emp_id
                WHERE o.order_id=%s
                """,
                (order_id,),
            )
            order = cursor.fetchone()
            if order is None:
                conn.close()
                return jsonify({"success": False, "message": f"订单不存在（order_id={order_id}）"})

            cursor.execute(
                """
                SELECT
                    od.detail_id,
                    od.product_id,
                    p.name AS product_name,
                    od.quantity,
                    od.unit_price,
                    od.unit_cost
                FROM OrderDetail od
                JOIN Product p ON p.product_id = od.product_id
                WHERE od.order_id=%s
                ORDER BY od.detail_id ASC
                """,
                (order_id,),
            )
            details = [_to_jsonable_row(r) for r in cursor.fetchall()]

        conn.close()
        return jsonify({"success": True, "data": {"order": _to_jsonable_row(order), "details": details}})
    except Exception as e:
        return jsonify({"success": False, "message": f"查询失败：{type(e).__name__}"})


@app.route("/api/orders/<int:order_id>", methods=["DELETE"])
def delete_order(order_id: int):
    """
    撤销/删除订单（事务）：回补库存 +（可选）写入库存流水，再删除订单主表。
    说明：
      - OrderDetail 因 ON DELETE CASCADE 会随 SalesOrder 一并删除
      - 若不先回补库存，删除订单会导致“库存与历史交易不一致”，验收时容易被追问
      - 本实现将 DELETE 语义定义为“撤销订单并回补”，用于课堂演示更稳妥
    """
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                has_ledger = _table_exists(cursor, "StockLedger")

                # 1) 锁定订单主记录，避免并发撤销/重复撤销
                cursor.execute("SELECT order_id, status FROM SalesOrder WHERE order_id=%s FOR UPDATE", (order_id,))
                order = cursor.fetchone()
                if order is None:
                    conn.rollback()
                    return jsonify({"success": False, "message": f"撤销失败：订单不存在（order_id={order_id}）"})

                # 2) 取出订单明细（用于回补库存）
                cursor.execute(
                    """
                    SELECT product_id, quantity, unit_cost
                    FROM OrderDetail
                    WHERE order_id=%s
                    """,
                    (order_id,),
                )
                details = cursor.fetchall() or []
                if len(details) == 0:
                    conn.rollback()
                    return jsonify({"success": False, "message": "撤销失败：订单无明细，无法回补库存（数据异常）"})

                # 3) 回补库存（锁定商品行，避免并发超卖/并发撤销导致库存错乱）
                ledger_rows = []
                for r in details:
                    pid = int(r["product_id"])
                    qty = int(r["quantity"])
                    unit_cost = float(r.get("unit_cost") or 0)

                    cursor.execute("SELECT product_id FROM Product WHERE product_id=%s FOR UPDATE", (pid,))
                    if cursor.fetchone() is None:
                        raise ValueError(f"撤销失败：商品不存在（product_id={pid}），无法回补库存")

                    cursor.execute(
                        "UPDATE Product SET stock_quantity = stock_quantity + %s WHERE product_id=%s",
                        (qty, pid),
                    )

                    if has_ledger:
                        ledger_rows.append((pid, qty, unit_cost, "cancel_sale", order_id, "撤销销售回补库存"))

                if has_ledger and ledger_rows:
                    cursor.executemany(
                        """
                        INSERT INTO StockLedger (product_id, change_qty, unit_cost, ref_type, ref_id, note)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        ledger_rows,
                    )

                # 4) 删除订单主表（明细因外键级联删除）
                cursor.execute("DELETE FROM SalesOrder WHERE order_id=%s", (order_id,))
                affected = cursor.rowcount
                if affected == 0:
                    raise ValueError("撤销失败：订单删除未生效（未知原因）")

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return jsonify({"success": True, "message": "✅ 订单已撤销：库存已回补（事务提交）"})
    except pymysql.err.IntegrityError as e:
        return jsonify({"success": False, "message": _integrity_error_to_zh_message(e)})
    except ValueError as e:
        return jsonify({"success": False, "message": str(e)})
    except Exception as e:
        # 避免把原始异常直接抛给前端（验收时要求“明确中文提示”）
        return jsonify({"success": False, "message": f"撤销失败：系统错误，请稍后重试。({type(e).__name__})"})


# =========================
# 进货入库：PurchaseOrder + PurchaseDetail（带事务 + 写入库存流水）
# =========================
def _validate_create_purchase_payload(data: dict):
    if not isinstance(data, dict):
        return None, "❌ 请求体必须是 JSON 对象！（应用层校验）"

    try:
        supplier_id = int(data.get("supplier_id"))
    except (ValueError, TypeError):
        return None, "❌ supplier_id 必须是有效整数！（应用层校验）"

    try:
        emp_id = int(data.get("emp_id"))
    except (ValueError, TypeError):
        return None, "❌ emp_id 必须是有效整数！（应用层校验）"

    items = data.get("items")
    if not isinstance(items, list) or len(items) == 0:
        return None, "❌ items 必须是非空数组！（应用层校验）"
    if len(items) > 50:
        return None, "❌ 单笔进货明细过多（最多 50 行）！（应用层校验）"

    normalized_items = []
    for i, it in enumerate(items, start=1):
        if not isinstance(it, dict):
            return None, f"❌ 第 {i} 条明细必须是对象！（应用层校验）"
        try:
            product_id = int(it.get("product_id"))
        except (ValueError, TypeError):
            return None, f"❌ 第 {i} 条明细 product_id 必须是整数！（应用层校验）"
        try:
            qty = int(it.get("quantity"))
        except (ValueError, TypeError):
            return None, f"❌ 第 {i} 条明细 quantity 必须是整数！（应用层校验）"
        if qty <= 0:
            return None, f"❌ 第 {i} 条明细 quantity 必须大于 0！（应用层校验）"

        try:
            unit_cost = float(it.get("unit_cost"))
        except (ValueError, TypeError):
            return None, f"❌ 第 {i} 条明细 unit_cost 必须是数字！（应用层校验）"
        if unit_cost < 0:
            return None, f"❌ 第 {i} 条明细 unit_cost 不能为负数！（应用层校验）"

        normalized_items.append({"product_id": product_id, "quantity": qty, "unit_cost": unit_cost})

    return {"supplier_id": supplier_id, "emp_id": emp_id, "items": normalized_items}, ""


@app.route("/api/purchases", methods=["POST"])
def create_purchase():
    """
    进货入库（事务）：
      - 创建进货单主表 + 明细表
      - 更新商品库存（+quantity）
      - 更新成本价（加权平均：按入库前库存与本次入库数量计算）
      - 写入库存流水 StockLedger（ref_type='purchase'）
    """
    data = request.json or {}
    payload, msg = _validate_create_purchase_payload(data)
    if payload is None:
        return jsonify({"success": False, "message": msg})

    try:
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                has_ledger = _table_exists(cursor, "StockLedger")

                cursor.execute("SELECT 1 FROM Supplier WHERE supplier_id=%s", (payload["supplier_id"],))
                if cursor.fetchone() is None:
                    conn.rollback()
                    return jsonify({"success": False, "message": f"❌ supplier_id={payload['supplier_id']} 不存在！（应用层校验）"})

                cursor.execute("SELECT 1 FROM Employee WHERE emp_id=%s", (payload["emp_id"],))
                if cursor.fetchone() is None:
                    conn.rollback()
                    return jsonify({"success": False, "message": f"❌ emp_id={payload['emp_id']} 不存在！（应用层校验）"})

                # 主表
                cursor.execute(
                    "INSERT INTO PurchaseOrder (supplier_id, emp_id, total_cost, status) VALUES (%s, %s, 0, '已入库')",
                    (payload["supplier_id"], payload["emp_id"]),
                )
                purchase_id = cursor.lastrowid

                details = []
                ledger_rows = []
                total_cost = 0.0

                for it in payload["items"]:
                    pid = it["product_id"]
                    qty = it["quantity"]
                    unit_cost = float(it["unit_cost"])

                    # 锁定商品行
                    cursor.execute(
                        """
                        SELECT product_id, stock_quantity, cost_price
                        FROM Product
                        WHERE product_id=%s
                        FOR UPDATE
                        """,
                        (pid,),
                    )
                    prod = cursor.fetchone()
                    if prod is None:
                        raise ValueError(f"❌ 商品不存在：product_id={pid}（应用层校验）")

                    old_stock = int(prod["stock_quantity"])
                    old_cost = float(prod.get("cost_price") or 0)

                    # 加权平均成本（更符合真实业务）
                    new_cost = old_cost
                    if old_stock + qty > 0:
                        new_cost = (old_stock * old_cost + qty * unit_cost) / (old_stock + qty)

                    # 入库：加库存 + 更新成本价
                    cursor.execute(
                        "UPDATE Product SET stock_quantity = stock_quantity + %s, cost_price=%s WHERE product_id=%s",
                        (qty, new_cost, pid),
                    )

                    details.append((purchase_id, pid, qty, unit_cost))
                    total_cost += unit_cost * qty

                    if has_ledger:
                        ledger_rows.append((pid, qty, unit_cost, "purchase", purchase_id, "进货入库"))

                cursor.executemany(
                    "INSERT INTO PurchaseDetail (purchase_id, product_id, quantity, unit_cost) VALUES (%s, %s, %s, %s)",
                    details,
                )

                if has_ledger and ledger_rows:
                    cursor.executemany(
                        """
                        INSERT INTO StockLedger
                            (product_id, change_qty, unit_cost, ref_type, ref_id, sale_order_id, purchase_id, note)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        # purchase_id 绑定进货单，sale_order_id 为空
                        [(pid, chg, uc, rt, rid, None, rid if rt == "purchase" else None, note) for (pid, chg, uc, rt, rid, note) in ledger_rows],
                    )

                cursor.execute("UPDATE PurchaseOrder SET total_cost=%s WHERE purchase_id=%s", (total_cost, purchase_id))

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return jsonify(
            {
                "success": True,
                "message": "✅ 进货入库成功（事务提交）",
                "data": {"purchase_id": purchase_id, "total_cost": round(total_cost, 2)},
            }
        )
    except pymysql.err.IntegrityError as e:
        return jsonify({"success": False, "message": _integrity_error_to_zh_message(e)})
    except ValueError as e:
        return jsonify({"success": False, "message": str(e)})
    except Exception as e:
        return jsonify({"success": False, "message": f"未知错误：{e}"})


# =========================
# 商品列表：分页 + 排序（避免一次性加载大量数据）
# =========================
@app.route("/api/products/paged", methods=["GET"])
def get_products_paged():
    """
    返回结构：
      {success: true, data: {items: [...], total: N, limit, offset, sort_by, sort_order}}

    sort_by 支持：
      - product_id / price / stock_quantity / sold_qty
    """
    limit = request.args.get("limit", default=20, type=int)
    offset = request.args.get("offset", default=0, type=int)
    sort_by = (request.args.get("sort_by") or "product_id").strip()
    sort_order = (request.args.get("sort_order") or "desc").strip().lower()

    limit = min(max(limit, 1), 200)
    offset = max(offset, 0)
    if sort_order not in ("asc", "desc"):
        sort_order = "desc"

    # 白名单，防 SQL 注入
    if sort_by not in ("product_id", "price", "stock_quantity", "sold_qty"):
        sort_by = "product_id"

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS cnt FROM Product")
            total = int(cursor.fetchone()["cnt"])

            # 统一返回 sold_qty，避免前端“销量”列一直为 0
            # 说明：sold_qty 来源于订单明细汇总（OrderDetail），没有订单则为 0
            order_expr = (
                f"sold_qty {sort_order}, v.product_id DESC"
                if sort_by == "sold_qty"
                else f"v.{sort_by} {sort_order}, v.product_id DESC"
            )

            sql = f"""
            SELECT
                v.*,
                COALESCE(s.sold_qty, 0) AS sold_qty
            FROM v_product_detail v
            LEFT JOIN (
                SELECT product_id, SUM(quantity) AS sold_qty
                FROM OrderDetail
                GROUP BY product_id
            ) s ON s.product_id = v.product_id
            ORDER BY {order_expr}
            LIMIT %s OFFSET %s
            """
            cursor.execute(sql, (limit, offset))

            items = [_to_jsonable_row(r) for r in cursor.fetchall()]

        conn.close()
        return jsonify(
            {
                "success": True,
                "data": {
                    "items": items,
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "sort_by": sort_by,
                    "sort_order": sort_order,
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


# =========================
# 统计模块（库存预警 / 在售占比 / 销售趋势）
# =========================
@app.route("/api/stats/overview", methods=["GET"])
def stats_overview():
    stock_threshold = request.args.get("stock_threshold", default=20, type=int)
    stock_threshold = max(stock_threshold, 0)

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            # 兼容：部分同学数据库未升级 unit_cost 字段，这里自动降级利润计算逻辑
            has_unit_cost = _column_exists(cursor, "OrderDetail", "unit_cost")
            has_cost_price = _column_exists(cursor, "Product", "cost_price")
            has_ledger = _table_exists(cursor, "StockLedger")

            # 1) 库存预警
            cursor.execute(
                """
                SELECT product_id, name AS product_name, stock_quantity, status
                FROM Product
                WHERE stock_quantity <= %s
                ORDER BY stock_quantity ASC, product_id DESC
                LIMIT 20
                """,
                (stock_threshold,),
            )
            stock_warn = cursor.fetchall()

            cursor.execute("SELECT COUNT(*) AS c FROM Product WHERE stock_quantity <= %s", (stock_threshold,))
            stock_warn_cnt = int(cursor.fetchone()["c"])

            # 2) 在售/停售占比
            cursor.execute(
                """
                SELECT status, COUNT(*) AS c
                FROM Product
                GROUP BY status
                """
            )
            status_rows = cursor.fetchall()
            status_map = {r["status"]: int(r["c"]) for r in status_rows}

            # 3) 销售额趋势（近 14 天，按日）
            if has_unit_cost:
                trend_sql = """
                    SELECT
                        DATE(o.order_date) AS d,
                        SUM(od.quantity * od.unit_price) AS sales_amount,
                        SUM(od.quantity * (od.unit_price - od.unit_cost)) AS profit_amount
                    FROM SalesOrder o
                    JOIN OrderDetail od ON od.order_id = o.order_id
                    WHERE o.order_date >= (NOW() - INTERVAL 14 DAY)
                    GROUP BY DATE(o.order_date)
                    ORDER BY d ASC
                """
                cursor.execute(trend_sql)
            elif has_cost_price:
                # 降级：用当前商品成本价估算利润（历史成本会受变更影响，但至少不报错）
                trend_sql = """
                    SELECT
                        DATE(o.order_date) AS d,
                        SUM(od.quantity * od.unit_price) AS sales_amount,
                        SUM(od.quantity * (od.unit_price - COALESCE(p.cost_price, 0))) AS profit_amount
                    FROM SalesOrder o
                    JOIN OrderDetail od ON od.order_id = o.order_id
                    JOIN Product p ON p.product_id = od.product_id
                    WHERE o.order_date >= (NOW() - INTERVAL 14 DAY)
                    GROUP BY DATE(o.order_date)
                    ORDER BY d ASC
                """
                cursor.execute(trend_sql)
            else:
                trend_sql = """
                    SELECT
                        DATE(o.order_date) AS d,
                        SUM(od.quantity * od.unit_price) AS sales_amount,
                        0 AS profit_amount
                    FROM SalesOrder o
                    JOIN OrderDetail od ON od.order_id = o.order_id
                    WHERE o.order_date >= (NOW() - INTERVAL 14 DAY)
                    GROUP BY DATE(o.order_date)
                    ORDER BY d ASC
                """
                cursor.execute(trend_sql)

            trend = cursor.fetchall()
            for r in trend:
                r["sales_amount"] = float(r["sales_amount"] or 0)
                r["profit_amount"] = float(r["profit_amount"] or 0)

            # 4) 库存消耗趋势（近 14 天销量，按日）——可作为“库存变化”的直观代理
            cursor.execute(
                """
                SELECT
                    DATE(o.order_date) AS d,
                    SUM(od.quantity) AS sold_qty
                FROM SalesOrder o
                JOIN OrderDetail od ON od.order_id = o.order_id
                WHERE o.order_date >= (NOW() - INTERVAL 14 DAY)
                GROUP BY DATE(o.order_date)
                ORDER BY d ASC
                """
            )
            sold_trend = cursor.fetchall()
            for r in sold_trend:
                r["sold_qty"] = int(r["sold_qty"] or 0)

            # 4.1) 库存余额趋势（基于库存流水，近 14 天按日）
            # 若有 StockLedger，则计算“总库存余额”随时间变化；否则不返回该字段
            stock_balance_trend = []
            if has_ledger:
                cursor.execute(
                    """
                    SELECT DATE(created_at) AS d, SUM(change_qty) AS net_qty
                    FROM StockLedger
                    WHERE created_at >= (CURDATE() - INTERVAL 13 DAY)
                    GROUP BY DATE(created_at)
                    ORDER BY d ASC
                    """
                )
                net_rows = cursor.fetchall()
                net_map = {str(r["d"]): int(r["net_qty"] or 0) for r in net_rows}

                cursor.execute("SELECT COALESCE(SUM(stock_quantity), 0) AS s FROM Product")
                current_total = int(cursor.fetchone()["s"] or 0)

                # 计算起始余额：当前总库存 - 近14天净变动合计
                total_net = sum(net_map.values())
                start_balance = current_total - total_net

                cursor.execute(
                    """
                    SELECT DATE(CURDATE() - INTERVAL n DAY) AS d
                    FROM (
                        SELECT 13 AS n UNION ALL SELECT 12 UNION ALL SELECT 11 UNION ALL SELECT 10 UNION ALL
                        SELECT 9 UNION ALL SELECT 8 UNION ALL SELECT 7 UNION ALL SELECT 6 UNION ALL
                        SELECT 5 UNION ALL SELECT 4 UNION ALL SELECT 3 UNION ALL SELECT 2 UNION ALL
                        SELECT 1 UNION ALL SELECT 0
                    ) t
                    ORDER BY d ASC
                    """
                )
                days = [str(r["d"]) for r in cursor.fetchall()]

                bal = start_balance
                for d in days:
                    bal += net_map.get(d, 0)
                    stock_balance_trend.append({"d": d, "stock_balance": int(bal)})

            # 5) 当前库存按类别汇总（柱状图）
            cursor.execute(
                """
                SELECT
                    c.category_id,
                    c.name AS category_name,
                    SUM(p.stock_quantity) AS stock_sum
                FROM Category c
                JOIN Product p ON p.category_id = c.category_id
                GROUP BY c.category_id
                ORDER BY stock_sum DESC;
                """
            )
            stock_by_cat = cursor.fetchall()
            for r in stock_by_cat:
                r["stock_sum"] = int(r["stock_sum"] or 0)

        conn.close()
        return jsonify(
            {
                "success": True,
                "data": {
                    "stock_threshold": stock_threshold,
                    "stock_warning_count": stock_warn_cnt,
                    "stock_warning_list": stock_warn,
                    "status_count": status_map,
                    "sales_trend_daily_14d": trend,
                    "sold_trend_daily_14d": sold_trend,
                    "stock_balance_trend_daily_14d": stock_balance_trend,
                    "stock_by_category": stock_by_cat,
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


# =========================
# 复杂查询增强
# =========================
@app.route("/api/queries/category_sales_mom", methods=["GET"])
def query_category_sales_mom():
    """
    近 30 天各类别销售额 vs 上一个 30 天（环比），并计算利润（基于 unit_cost 快照）。
    解决什么问题：
      - 识别哪些品类在增长/下滑，用于补货、营销资源倾斜
    """
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            has_unit_cost = _column_exists(cursor, "OrderDetail", "unit_cost")
            has_cost_price = _column_exists(cursor, "Product", "cost_price")

            profit_expr = "od.quantity * (od.unit_price - od.unit_cost)" if has_unit_cost else (
                "od.quantity * (od.unit_price - COALESCE(p.cost_price, 0))" if has_cost_price else "0"
            )

            sql = """
            SELECT
                c.category_id,
                c.name AS category_name,
                SUM(CASE WHEN o.order_date >= (NOW() - INTERVAL 30 DAY)
                         THEN od.quantity * od.unit_price ELSE 0 END) AS sales_curr_30d,
                SUM(CASE WHEN o.order_date < (NOW() - INTERVAL 30 DAY)
                          AND o.order_date >= (NOW() - INTERVAL 60 DAY)
                         THEN od.quantity * od.unit_price ELSE 0 END) AS sales_prev_30d,
                SUM(CASE WHEN o.order_date >= (NOW() - INTERVAL 30 DAY)
                         THEN {profit_expr} ELSE 0 END) AS profit_curr_30d
            FROM Category c
            JOIN Product p ON p.category_id = c.category_id
            JOIN OrderDetail od ON od.product_id = p.product_id
            JOIN SalesOrder o ON o.order_id = od.order_id
            GROUP BY c.category_id
            ORDER BY sales_curr_30d DESC;
            """
            sql = sql.format(profit_expr=profit_expr)
            cursor.execute(sql)
            rows = cursor.fetchall()

        conn.close()

        # 应用层补充“环比”字段，避免 MySQL 除零
        for r in rows:
            curr = float(r["sales_curr_30d"] or 0)
            prev = float(r["sales_prev_30d"] or 0)
            r["sales_curr_30d"] = curr
            r["sales_prev_30d"] = prev
            r["profit_curr_30d"] = float(r["profit_curr_30d"] or 0)
            r["mom_rate"] = None if prev <= 0 else round((curr - prev) / prev, 4)

        return jsonify(
            {
                "success": True,
                "data": rows,
                "meta": {
                    "biz_desc": "近30天各类别销售额环比分析：识别增长/下滑品类，辅助补货与营销决策。",
                    "fields": {
                        "sales_curr_30d": "近30天销售额",
                        "sales_prev_30d": "前30天销售额（30~60天前）",
                        "mom_rate": "环比增长率=(本期-上期)/上期（上期为0则为空）",
                        "profit_curr_30d": "近30天利润额（基于明细 unit_cost 快照）",
                    },
                    "sql": sql.strip(),
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/queries/member_spend_top", methods=["GET"])
def query_member_spend_top():
    """
    近 30 天会员消费排行（Customer + SalesOrder + OrderDetail 三表），附带件数与最近下单时间。
    解决什么问题：
      - 识别高价值客户，指导会员运营与精准营销
    """
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = """
            SELECT
                c.customer_id,
                c.name AS customer_name,
                c.level,
                SUM(od.quantity * od.unit_price) AS spend_30d,
                SUM(od.quantity) AS item_qty_30d,
                MAX(o.order_date) AS last_order_time
            FROM Customer c
            JOIN SalesOrder o ON o.customer_id = c.customer_id
            JOIN OrderDetail od ON od.order_id = o.order_id
            WHERE o.order_date >= (NOW() - INTERVAL 30 DAY)
            GROUP BY c.customer_id
            ORDER BY spend_30d DESC
            LIMIT 10;
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            for r in rows:
                r["spend_30d"] = float(r["spend_30d"] or 0)
                r["item_qty_30d"] = int(r["item_qty_30d"] or 0)
        conn.close()

        return jsonify(
            {
                "success": True,
                "data": rows,
                "meta": {
                    "biz_desc": "近30天会员消费TOP10：识别高价值客户，用于会员分层运营/优惠券投放/重点维护。",
                    "fields": {
                        "spend_30d": "近30天消费金额（由明细汇总）",
                        "item_qty_30d": "近30天购买件数",
                        "last_order_time": "最近一次下单时间",
                    },
                    "sql": sql.strip(),
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


# =========================
# 性能实验严谨化：索引实验（多轮统计）/ 视图实验（可维护性对比）
# =========================
def _measure_query_time(cursor, sql: str, params, loops: int) -> float:
    start = time.perf_counter()
    for _ in range(loops):
        cursor.execute(sql, params)
        cursor.fetchall()
    return time.perf_counter() - start


@app.route("/api/experiments/index", methods=["GET"])
def experiment_index():
    """
    索引实验：同一 SQL 做多轮「有索引/无索引」对照，输出 avg/min/max/提升倍数。
    """
    rounds = request.args.get("rounds", default=5, type=int)
    loops = request.args.get("loops", default=200, type=int)
    rounds = min(max(rounds, 3), 20)
    loops = min(max(loops, 50), 2000)

    ensure_index_sql = "CREATE INDEX idx_product_name ON Product(name);"
    drop_index_sql = "DROP INDEX idx_product_name ON Product;"
    query_sql = "SELECT * FROM Product WHERE name = %s"
    query_param = ("Product_NonExistent_Test",)

    def stats(arr):
        arr2 = sorted(arr)
        return {
            "avg": sum(arr2) / len(arr2),
            "min": arr2[0],
            "max": arr2[-1],
        }

    logs = []
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            # 有索引
            try:
                cursor.execute(ensure_index_sql)
            except Exception:
                pass

            indexed_times = []
            logs.append(f"[A] 有索引：rounds={rounds}, loops/round={loops}")
            for i in range(rounds):
                t = _measure_query_time(cursor, query_sql, query_param, loops)
                indexed_times.append(t)
                logs.append(f"  round {i+1}: {t:.6f}s")

            # 无索引
            logs.append("\n[B] 删除索引并测试无索引…")
            try:
                cursor.execute(drop_index_sql)
            except Exception:
                pass

            unindexed_times = []
            logs.append(f"无索引：rounds={rounds}, loops/round={loops}")
            for i in range(rounds):
                t = _measure_query_time(cursor, query_sql, query_param, loops)
                unindexed_times.append(t)
                logs.append(f"  round {i+1}: {t:.6f}s")

            # 恢复索引
            logs.append("\n[C] 恢复索引…")
            try:
                cursor.execute(ensure_index_sql)
            except Exception:
                pass

        conn.commit()
        conn.close()

        s_idx = stats(indexed_times)
        s_no = stats(unindexed_times)
        ratio = (s_no["avg"] / s_idx["avg"]) if s_idx["avg"] > 0 else None
        logs.append("\n================ 统计结论 ================")
        logs.append(f"有索引 avg/min/max: {s_idx['avg']:.6f}/{s_idx['min']:.6f}/{s_idx['max']:.6f} s")
        logs.append(f"无索引 avg/min/max: {s_no['avg']:.6f}/{s_no['min']:.6f}/{s_no['max']:.6f} s")
        logs.append(f"平均提升倍数(无索引/有索引): {ratio:.2f}x" if ratio is not None else "平均提升倍数：无法计算")

        return jsonify(
            {
                "success": True,
                "data": {
                    "rounds": rounds,
                    "loops": loops,
                    "indexed": s_idx,
                    "unindexed": s_no,
                    "ratio": ratio,
                    "sql": "SELECT * FROM Product WHERE name = 'Product_NonExistent_Test'",
                },
                "logs": logs,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/experiments/view", methods=["GET"])
def experiment_view():
    """
    视图实验：比较“直接多表SQL”和“基于视图查询”的可维护性与性能（多轮平均耗时）。
    """
    rounds = request.args.get("rounds", default=5, type=int)
    loops = request.args.get("loops", default=200, type=int)
    limit = request.args.get("limit", default=50, type=int)
    rounds = min(max(rounds, 3), 20)
    loops = min(max(loops, 50), 2000)
    limit = min(max(limit, 10), 200)

    direct_sql = """
    SELECT
        p.product_id,
        p.name AS product_name,
        p.barcode,
        p.category_id,
        c.name AS category_name,
        p.price,
        p.cost_price,
        p.stock_quantity,
        p.status
    FROM Product p
    JOIN Category c ON p.category_id = c.category_id
    ORDER BY p.product_id DESC
    LIMIT %s
    """
    view_sql = """
    SELECT *
    FROM v_product_detail
    ORDER BY product_id DESC
    LIMIT %s
    """

    def stats(arr):
        arr2 = sorted(arr)
        return {"avg": sum(arr2) / len(arr2), "min": arr2[0], "max": arr2[-1]}

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            direct_times = []
            view_times = []
            for _ in range(rounds):
                direct_times.append(_measure_query_time(cursor, direct_sql, (limit,), loops))
                view_times.append(_measure_query_time(cursor, view_sql, (limit,), loops))

            # 取一次结果给前端展示（避免传太大）
            cursor.execute(direct_sql, (limit,))
            direct_sample = [_to_jsonable_row(r) for r in cursor.fetchall()]
            cursor.execute(view_sql, (limit,))
            view_sample = [_to_jsonable_row(r) for r in cursor.fetchall()]

        conn.close()

        s_direct = stats(direct_times)
        s_view = stats(view_times)
        ratio = (s_direct["avg"] / s_view["avg"]) if s_view["avg"] > 0 else None

        return jsonify(
            {
                "success": True,
                "data": {
                    "rounds": rounds,
                    "loops": loops,
                    "limit": limit,
                    "direct_sql": direct_sql.strip(),
                    "view_sql": view_sql.strip(),
                    "direct": s_direct,
                    "view": s_view,
                    "ratio_direct_over_view": ratio,
                    "maintainability_note": "视图将多表 JOIN 封装成统一接口：调用方 SQL 更短、更一致；当底层表结构变化时，优先修改视图即可降低改动面。",
                    "sample_equal": len(direct_sample) == len(view_sample),
                    "direct_sample": direct_sample[:10],
                    "view_sample": view_sample[:10],
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


# =========================
# 动态 SQL
# =========================
@app.route("/api/dynamic_query", methods=["POST"])
def dynamic_query():
    data = request.json or {}
    conditions = []
    params = []

    base_sql = "SELECT * FROM v_product_detail"

    if data.get("check_name") and data.get("name"):
        conditions.append("product_name LIKE %s")
        params.append(f"%{data.get('name')}%")

    if data.get("check_barcode") and data.get("barcode"):
        conditions.append("barcode = %s")
        params.append(data.get("barcode"))

    if data.get("check_price"):
        min_p = data.get("min_price")
        max_p = data.get("max_price")
        if min_p is not None and str(min_p).strip() != "":
            conditions.append("price >= %s")
            params.append(float(min_p))
        if max_p is not None and str(max_p).strip() != "":
            conditions.append("price <= %s")
            params.append(float(max_p))

    if data.get("check_status") and data.get("status"):
        conditions.append("status = %s")
        params.append(data.get("status"))

    final_sql = f"{base_sql} WHERE " + " AND ".join(conditions) if conditions else base_sql

    # 防止一次拉太多数据导致页面卡顿
    final_sql = f"{final_sql} LIMIT 200"

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            raw_sql = cursor.mogrify(final_sql, params)
            if isinstance(raw_sql, (bytes, bytearray)):
                raw_sql = raw_sql.decode("utf-8", errors="ignore")

            cursor.execute(final_sql, params)
            results = [_to_jsonable_row(r) for r in cursor.fetchall()]

        conn.close()
        return jsonify({"success": True, "data": results, "sql": raw_sql})
    except Exception as e:
        return jsonify({"success": False, "message": str(e), "sql": final_sql})


# =========================
# 索引性能对比
# =========================
@app.route("/api/test_index", methods=["GET"])
def test_index():
    ensure_index_sql = "CREATE INDEX idx_product_name ON Product(name);"
    drop_index_sql = "DROP INDEX idx_product_name ON Product;"
    logs = []

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            # 1. 确保有索引
            try:
                cursor.execute(ensure_index_sql)
            except Exception:
                pass

            logs.append("[1] 正在测试【有索引】状态...")
            start_time = time.time()
            for _ in range(100):
                cursor.execute("SELECT * FROM Product WHERE name = 'Product_NonExistent_Test'")
                cursor.fetchall()
            indexed_time = time.time() - start_time
            logs.append(f"    耗时: {indexed_time:.6f} 秒")

            # 2. 删除索引
            logs.append("\n[2] 正在删除索引...")
            cursor.execute(drop_index_sql)

            # 3. 无索引测试
            logs.append("\n[3] 正在测试【无索引】全表扫描状态...")
            start_time = time.time()
            for _ in range(100):
                cursor.execute("SELECT * FROM Product WHERE name = 'Product_NonExistent_Test'")
                cursor.fetchall()
            unindexed_time = time.time() - start_time
            logs.append(f"    耗时: {unindexed_time:.6f} 秒")

            # 4. 恢复索引
            logs.append("\n[4] 正在恢复索引...")
            cursor.execute(ensure_index_sql)

            logs.append("\n================ 最终结论 ================")
            logs.append(f"有索引查询耗时: {indexed_time:.6f} 秒")
            logs.append(f"无索引查询耗时: {unindexed_time:.6f} 秒")
            if unindexed_time > indexed_time:
                ratio = unindexed_time / indexed_time if indexed_time > 0 else 0
                logs.append(f"结论：B+树索引使查询速度提升了约 {ratio:.2f} 倍！")
            else:
                logs.append("结论：数据量较小或存在缓存，差异不明显。")

        conn.commit()
        conn.close()
        return jsonify({"success": True, "logs": logs})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(env("PORT", "8000")), debug=True)
