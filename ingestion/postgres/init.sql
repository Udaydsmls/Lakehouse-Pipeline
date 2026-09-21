-- Source database for the pipeline. This stands in for the application's
-- OLTP database: products, users, orders and order items.
--
-- Postgres also hosts the Airflow metadata DB and the `analytics` schema that
-- the Spark jobs write their results into.

CREATE DATABASE airflow;

\connect ecommerce;

-- Spark writes its curated tables here; dbt reads from them.
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS products (
    product_id  VARCHAR(50)    PRIMARY KEY,
    name        VARCHAR(200)   NOT NULL,
    category    VARCHAR(100)   NOT NULL,
    subcategory VARCHAR(100),
    brand       VARCHAR(100),
    base_price  DECIMAL(10, 2) NOT NULL,
    is_active   BOOLEAN        DEFAULT TRUE,
    created_at  TIMESTAMPTZ    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    user_id             VARCHAR(50)  PRIMARY KEY,
    email               VARCHAR(200) UNIQUE NOT NULL,
    country             CHAR(2),
    acquisition_channel VARCHAR(50),
    segment             VARCHAR(50)  DEFAULT 'new',
    email_verified      BOOLEAN      DEFAULT FALSE,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id         VARCHAR(50)    PRIMARY KEY,
    user_id          VARCHAR(50)    REFERENCES users(user_id),
    status           VARCHAR(50)    NOT NULL,
    total_amount     DECIMAL(10, 2) NOT NULL,
    discount_amount  DECIMAL(10, 2) DEFAULT 0,
    payment_method   VARCHAR(50),
    shipping_country CHAR(2),
    created_at       TIMESTAMPTZ    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id VARCHAR(50)    PRIMARY KEY,
    order_id      VARCHAR(50)    REFERENCES orders(order_id),
    product_id    VARCHAR(50)    REFERENCES products(product_id),
    quantity      INT            NOT NULL,
    unit_price    DECIMAL(10, 2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);

-- Product catalogue. The clickstream producer sends product ids in the same
-- PROD-001..PROD-050 range so the two data sets join up.
INSERT INTO products (product_id, name, category, subcategory, brand, base_price) VALUES
    ('PROD-001', 'Samsung Galaxy S24 Ultra',    'electronics', 'smartphones', 'Samsung',        1199.99),
    ('PROD-002', 'Apple iPhone 15 Pro',         'electronics', 'smartphones', 'Apple',          1099.99),
    ('PROD-003', 'Google Pixel 8 Pro',          'electronics', 'smartphones', 'Google',          999.99),
    ('PROD-004', 'OnePlus 12',                  'electronics', 'smartphones', 'OnePlus',         799.99),
    ('PROD-005', 'Xiaomi 14',                   'electronics', 'smartphones', 'Xiaomi',          699.99),
    ('PROD-006', 'Sony Xperia 1 VI',            'electronics', 'smartphones', 'Sony',            949.99),
    ('PROD-007', 'Motorola Edge 50 Pro',        'electronics', 'smartphones', 'Motorola',        549.99),
    ('PROD-008', 'Nothing Phone 2a',            'electronics', 'smartphones', 'Nothing',         399.99),
    ('PROD-009', 'Asus ROG Phone 8',            'electronics', 'smartphones', 'Asus',            999.99),
    ('PROD-010', 'Fairphone 5',                 'electronics', 'smartphones', 'Fairphone',       699.99),
    ('PROD-011', 'Apple MacBook Pro 16 M3',     'electronics', 'laptops',     'Apple',          3499.99),
    ('PROD-012', 'Dell XPS 15',                 'electronics', 'laptops',     'Dell',           1899.99),
    ('PROD-013', 'Lenovo ThinkPad X1 Carbon',   'electronics', 'laptops',     'Lenovo',         1799.99),
    ('PROD-014', 'Asus ZenBook Pro Duo',        'electronics', 'laptops',     'Asus',           1599.99),
    ('PROD-015', 'HP Spectre x360',             'electronics', 'laptops',     'HP',             1499.99),
    ('PROD-016', 'Microsoft Surface Laptop 6',  'electronics', 'laptops',     'Microsoft',      1299.99),
    ('PROD-017', 'Razer Blade 16',              'electronics', 'laptops',     'Razer',          3299.99),
    ('PROD-018', 'LG Gram 17',                  'electronics', 'laptops',     'LG',             1399.99),
    ('PROD-019', 'Samsung Galaxy Book4 Pro',    'electronics', 'laptops',     'Samsung',        1499.99),
    ('PROD-020', 'Acer Swift 5',                'electronics', 'laptops',     'Acer',            999.99),
    ('PROD-021', 'Sony WH-1000XM5',             'electronics', 'audio',       'Sony',            349.99),
    ('PROD-022', 'Apple AirPods Pro 2',         'electronics', 'audio',       'Apple',           249.99),
    ('PROD-023', 'Bose QuietComfort 45',        'electronics', 'audio',       'Bose',            329.99),
    ('PROD-024', 'Sennheiser Momentum 4',       'electronics', 'audio',       'Sennheiser',      349.99),
    ('PROD-025', 'JBL Flip 6',                  'electronics', 'audio',       'JBL',              99.99),
    ('PROD-026', 'Marshall Emberton III',       'electronics', 'audio',       'Marshall',        129.99),
    ('PROD-027', 'Sonos Era 100',               'electronics', 'audio',       'Sonos',           249.99),
    ('PROD-028', 'Audio-Technica ATH-M50xBT2',  'electronics', 'audio',       'Audio-Technica',  149.99),
    ('PROD-029', 'Jabra Elite 10',              'electronics', 'audio',       'Jabra',           249.99),
    ('PROD-030', 'Samsung Galaxy Buds3 Pro',    'electronics', 'audio',       'Samsung',         229.99),
    ('PROD-031', 'Sony Alpha 7 IV',             'electronics', 'cameras',     'Sony',           2499.99),
    ('PROD-032', 'Canon EOS R6 Mark II',        'electronics', 'cameras',     'Canon',          2499.99),
    ('PROD-033', 'Nikon Z6 III',                'electronics', 'cameras',     'Nikon',          2499.99),
    ('PROD-034', 'Fujifilm X-T5',               'electronics', 'cameras',     'Fujifilm',       1699.99),
    ('PROD-035', 'OM System OM-5',              'electronics', 'cameras',     'OM System',       999.99),
    ('PROD-036', 'GoPro Hero 12 Black',         'electronics', 'cameras',     'GoPro',           399.99),
    ('PROD-037', 'DJI Osmo Action 4',           'electronics', 'cameras',     'DJI',             349.99),
    ('PROD-038', 'Insta360 X4',                 'electronics', 'cameras',     'Insta360',        499.99),
    ('PROD-039', 'Ricoh GR IIIx',               'electronics', 'cameras',     'Ricoh',           999.99),
    ('PROD-040', 'Leica Q3',                    'electronics', 'cameras',     'Leica',          5995.99),
    ('PROD-041', 'Levi 501 Original Jeans',     'clothing',    'jeans',       'Levis',            98.00),
    ('PROD-042', 'Nike Dri-FIT T-Shirt',        'clothing',    't-shirts',    'Nike',             55.00),
    ('PROD-043', 'Adidas Tiro 23 Track Jacket', 'clothing',    'jackets',     'Adidas',           80.00),
    ('PROD-044', 'Ralph Lauren Oxford Shirt',   'clothing',    'shirts',      'Ralph Lauren',     89.50),
    ('PROD-045', 'Tommy Hilfiger Chinos',       'clothing',    'trousers',    'Tommy Hilfiger',   85.00),
    ('PROD-046', 'Zara Smart Blazer',           'clothing',    'blazers',     'Zara',             99.90),
    ('PROD-047', 'H&M Slim Fit Suit',           'clothing',    'suits',       'H&M',             149.99),
    ('PROD-048', 'Uniqlo Merino Sweater',       'clothing',    'knitwear',    'Uniqlo',           59.90),
    ('PROD-049', 'Calvin Klein Underwear Pack', 'clothing',    'underwear',   'Calvin Klein',     39.99),
    ('PROD-050', 'Barbour Wax Jacket',          'clothing',    'jackets',     'Barbour',         299.00)
ON CONFLICT (product_id) DO NOTHING;
