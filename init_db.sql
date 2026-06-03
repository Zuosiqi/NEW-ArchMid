CREATE DATABASE IF NOT EXISTS product_manage_db DEFAULT CHARSET utf8mb4;
USE product_manage_db;

DROP VIEW IF EXISTS v_sales_summary;
DROP VIEW IF EXISTS v_product_detail;

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS StockLedger;
DROP TABLE IF EXISTS PurchaseDetail;
DROP TABLE IF EXISTS PurchaseOrder;
DROP TABLE IF EXISTS OrderDetail;
DROP TABLE IF EXISTS SalesOrder;
DROP TABLE IF EXISTS Customer;
DROP TABLE IF EXISTS Employee;
DROP TABLE IF EXISTS Supply;
DROP TABLE IF EXISTS Product;
DROP TABLE IF EXISTS Supplier;
DROP TABLE IF EXISTS Category;

SET FOREIGN_KEY_CHECKS = 1;

-- 1. 商品类别表 (Category)
CREATE TABLE Category (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255)
);

-- 预置一些基础类别，方便不生成大数据也能立刻演示“新增商品/外键约束”
INSERT INTO Category (category_id, name, description) VALUES
    (1, '食品', '零食饮料、生鲜等'),
    (2, '日用品', '纸品清洁、家居日用等'),
    (3, '数码', '手机电脑及配件'),
    (4, '服饰', '男女装、鞋包'),
    (5, '美妆', '护肤彩妆'),
    (6, '母婴', '母婴用品'),
    (7, '家电', '小家电、大家电'),
    (8, '办公', '文具耗材'),
    (9, '运动', '运动户外'),
    (10, '图书', '图书教材')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    description = VALUES(description);

-- 2. 供应商表 (Supplier)
CREATE TABLE Supplier (
    supplier_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    contact_person VARCHAR(50) NOT NULL,
    phone VARCHAR(20) NOT NULL UNIQUE,
    address VARCHAR(255)
);

-- 3. 商品表 (Product)
CREATE TABLE Product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    barcode VARCHAR(50) UNIQUE NOT NULL,
    category_id INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    cost_price DECIMAL(10, 2) NOT NULL DEFAULT 0 CHECK (cost_price >= 0),
    stock_quantity INT NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    status VARCHAR(20) DEFAULT '在售',
    FOREIGN KEY (category_id) REFERENCES Category(category_id) ON DELETE RESTRICT
);

-- 4. 供应关系表 (Supply) - 多对多关系
CREATE TABLE Supply (
    supply_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    supplier_id INT NOT NULL,
    supply_price DECIMAL(10, 2) NOT NULL CHECK (supply_price > 0),
    FOREIGN KEY (product_id) REFERENCES Product(product_id) ON DELETE CASCADE,
    FOREIGN KEY (supplier_id) REFERENCES Supplier(supplier_id) ON DELETE CASCADE,
    UNIQUE (product_id, supplier_id)
);

-- 5. 员工表 (Employee)
CREATE TABLE Employee (
    emp_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    role VARCHAR(30) NOT NULL,
    phone VARCHAR(20) UNIQUE NOT NULL,
    hire_date DATE
);

-- 预置员工（便于演示“下单事务”与统计查询）
INSERT INTO Employee (emp_id, name, role, phone, hire_date) VALUES
    (1, '张店员', '收银员', '13800000001', '2024-03-01'),
    (2, '李库管', '库管',   '13800000002', '2024-03-01'),
    (3, '王经理', '经理',   '13800000003', '2024-03-01')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    role = VALUES(role),
    hire_date = VALUES(hire_date);

-- 6. 客户/会员表 (Customer)
CREATE TABLE Customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    phone VARCHAR(20) UNIQUE NOT NULL,
    points INT DEFAULT 0 CHECK (points >= 0),
    level VARCHAR(20) DEFAULT '普通会员'
);

-- 预置客户（便于演示“下单事务”与会员排行）
INSERT INTO Customer (customer_id, name, phone, points, level) VALUES
    (1, '左思琪', '13900000001', 120, '普通会员'),
    (2, '右思琪', '13900000002', 520, '白银会员'),
    (3, '张瑞瑞', '13900000003', 1880, '黄金会员')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    points = VALUES(points),
    level = VALUES(level);

-- 7. 销售订单表 (SalesOrder)
CREATE TABLE SalesOrder (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    emp_id INT NOT NULL,
    order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0.0 CHECK (total_amount >= 0),
    status VARCHAR(20) DEFAULT '已完成',
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id) ON DELETE RESTRICT,
    FOREIGN KEY (emp_id) REFERENCES Employee(emp_id) ON DELETE RESTRICT
);

-- 7.1 进货单表 (PurchaseOrder)
CREATE TABLE PurchaseOrder (
    purchase_id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_id INT NOT NULL,
    emp_id INT NOT NULL,
    purchase_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    total_cost DECIMAL(12, 2) NOT NULL DEFAULT 0.0 CHECK (total_cost >= 0),
    status VARCHAR(20) DEFAULT '已入库',
    FOREIGN KEY (supplier_id) REFERENCES Supplier(supplier_id) ON DELETE RESTRICT,
    FOREIGN KEY (emp_id) REFERENCES Employee(emp_id) ON DELETE RESTRICT
);

-- 8. 订单明细表 (OrderDetail)
CREATE TABLE OrderDetail (
    detail_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    unit_cost DECIMAL(10, 2) NOT NULL DEFAULT 0 CHECK (unit_cost >= 0),
    FOREIGN KEY (order_id) REFERENCES SalesOrder(order_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES Product(product_id) ON DELETE RESTRICT
);

-- 8.1 进货明细表 (PurchaseDetail)
CREATE TABLE PurchaseDetail (
    purchase_detail_id INT AUTO_INCREMENT PRIMARY KEY,
    purchase_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_cost DECIMAL(10, 2) NOT NULL CHECK (unit_cost >= 0),
    FOREIGN KEY (purchase_id) REFERENCES PurchaseOrder(purchase_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES Product(product_id) ON DELETE RESTRICT
);

-- 9. 库存流水表 (StockLedger)
-- change_qty：正数=入库/盘盈，负数=出库/盘亏/销售；ref_type/ref_id 用于追溯来源单据
CREATE TABLE StockLedger (
    ledger_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    change_qty INT NOT NULL,
    unit_cost DECIMAL(10, 2) DEFAULT NULL CHECK (unit_cost IS NULL OR unit_cost >= 0),
    ref_type VARCHAR(20) NOT NULL,
    ref_id INT DEFAULT NULL,
    sale_order_id INT DEFAULT NULL,
    purchase_id INT DEFAULT NULL,
    note VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES Product(product_id) ON DELETE RESTRICT,
    FOREIGN KEY (sale_order_id) REFERENCES SalesOrder(order_id) ON DELETE SET NULL,
    FOREIGN KEY (purchase_id) REFERENCES PurchaseOrder(purchase_id) ON DELETE SET NULL
);

-- ====================
-- 创建视图（给前端/联调用）
-- ====================
CREATE OR REPLACE VIEW v_product_detail AS
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
JOIN Category c ON p.category_id = c.category_id;

CREATE OR REPLACE VIEW v_sales_summary AS
SELECT 
    o.order_id,
    o.order_date,
    c.name AS customer_name,
    e.name AS emp_name,
    o.total_amount,
    o.status
FROM SalesOrder o
JOIN Customer c ON o.customer_id = c.customer_id
JOIN Employee e ON o.emp_id = e.emp_id;

-- ====================
-- 创建索引（用于性能对比）
-- ====================
CREATE INDEX idx_product_name ON Product(name);
CREATE INDEX idx_product_price ON Product(price);
CREATE INDEX idx_product_stock ON Product(stock_quantity);
CREATE INDEX idx_order_date ON SalesOrder(order_date);
CREATE INDEX idx_orderdetail_product ON OrderDetail(product_id);
CREATE INDEX idx_orderdetail_order ON OrderDetail(order_id);
CREATE INDEX idx_purchase_date ON PurchaseOrder(purchase_date);
CREATE INDEX idx_purchasedetail_purchase ON PurchaseDetail(purchase_id);
CREATE INDEX idx_purchasedetail_product ON PurchaseDetail(product_id);
CREATE INDEX idx_ledger_product_time ON StockLedger(product_id, created_at);
CREATE INDEX idx_ledger_ref ON StockLedger(ref_type, ref_id);
CREATE INDEX idx_ledger_sale ON StockLedger(sale_order_id);
CREATE INDEX idx_ledger_purchase ON StockLedger(purchase_id);
