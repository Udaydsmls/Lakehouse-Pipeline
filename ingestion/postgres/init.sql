CREATE DATABASE airflow;

\connect ecommerce;

CREATE TABLE IF NOT EXISTS categories (
    category_id       SERIAL PRIMARY KEY,
    name              VARCHAR(100) NOT NULL,
    parent_category_id INT REFERENCES categories(category_id),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id   VARCHAR(50)     PRIMARY KEY,
    name         VARCHAR(200)    NOT NULL,
    category_id  INT             REFERENCES categories(category_id),
    subcategory  VARCHAR(100),
    brand        VARCHAR(100),
    base_price   DECIMAL(10, 2)  NOT NULL,
    is_active    BOOLEAN         DEFAULT TRUE,
    created_at   TIMESTAMPTZ     DEFAULT NOW(),
    updated_at   TIMESTAMPTZ     DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    user_id              VARCHAR(50)  PRIMARY KEY,
    email                VARCHAR(200) UNIQUE NOT NULL,
    country              CHAR(2),
    acquisition_channel  VARCHAR(50),
    segment              VARCHAR(50)  DEFAULT 'new',
    email_verified       BOOLEAN      DEFAULT FALSE,
    created_at           TIMESTAMPTZ  DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id                 VARCHAR(50)    PRIMARY KEY,
    user_id                  VARCHAR(50)    REFERENCES users(user_id),
    status                   VARCHAR(50)    NOT NULL,
    total_amount             DECIMAL(10, 2) NOT NULL,
    discount_amount          DECIMAL(10, 2) DEFAULT 0,
    payment_method           VARCHAR(50),
    shipping_address_country CHAR(2),
    created_at               TIMESTAMPTZ    DEFAULT NOW(),
    updated_at               TIMESTAMPTZ    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id       VARCHAR(50)    PRIMARY KEY,
    order_id            VARCHAR(50)    REFERENCES orders(order_id),
    product_id          VARCHAR(50)    REFERENCES products(product_id),
    quantity            INT            NOT NULL,
    unit_price          DECIMAL(10, 2) NOT NULL,
    discount_percentage DECIMAL(5, 4)  DEFAULT 0,
    created_at          TIMESTAMPTZ    DEFAULT NOW(),
    updated_at          TIMESTAMPTZ    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ab_test_assignments (
    assignment_id SERIAL PRIMARY KEY,
    user_id       VARCHAR(50) REFERENCES users(user_id),
    test_name     VARCHAR(100) NOT NULL,
    variant       VARCHAR(50)  NOT NULL CHECK (variant IN ('control', 'treatment')),
    assigned_at   TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id   ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_ab_test_name_user ON ab_test_assignments(test_name, user_id);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_order_items_updated_at
    BEFORE UPDATE ON order_items
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

INSERT INTO categories (category_id, name, parent_category_id) VALUES
    (1,  'Electronics',      NULL),
    (2,  'Clothing',         NULL),
    (3,  'Home & Garden',    NULL),
    (4,  'Sports',           NULL),
    (5,  'Beauty',           NULL),
    (6,  'Smartphones',      1),
    (7,  'Laptops',          1),
    (8,  'Audio',            1),
    (9,  'Cameras',          1),
    (10, 'Mens Clothing',    2),
    (11, 'Womens Clothing',  2),
    (12, 'Kids Clothing',    2),
    (13, 'Footwear',         2),
    (14, 'Furniture',        3),
    (15, 'Kitchen',          3),
    (16, 'Garden Tools',     3),
    (17, 'Running',          4),
    (18, 'Cycling',          4),
    (19, 'Skincare',         5),
    (20, 'Haircare',         5)
ON CONFLICT (category_id) DO NOTHING;

SELECT setval('categories_category_id_seq', 20, true);

INSERT INTO products (product_id, name, category_id, subcategory, brand, base_price) VALUES
    ('PROD-001', 'Samsung Galaxy S24 Ultra',       6,  'Smartphones',     'Samsung',  1199.99),
    ('PROD-002', 'Apple iPhone 15 Pro',            6,  'Smartphones',     'Apple',    1099.99),
    ('PROD-003', 'Google Pixel 8 Pro',             6,  'Smartphones',     'Google',    999.99),
    ('PROD-004', 'OnePlus 12',                     6,  'Smartphones',     'OnePlus',   799.99),
    ('PROD-005', 'Xiaomi 14',                      6,  'Smartphones',     'Xiaomi',    699.99),
    ('PROD-006', 'Sony Xperia 1 VI',               6,  'Smartphones',     'Sony',      949.99),
    ('PROD-007', 'Motorola Edge 50 Pro',           6,  'Smartphones',     'Motorola',  549.99),
    ('PROD-008', 'Nothing Phone 2a',               6,  'Smartphones',     'Nothing',   399.99),
    ('PROD-009', 'Asus ROG Phone 8',               6,  'Smartphones',     'Asus',      999.99),
    ('PROD-010', 'Fairphone 5',                    6,  'Smartphones',     'Fairphone', 699.99),
    ('PROD-011', 'Apple MacBook Pro 16 M3 Max',   7,  'Laptops',         'Apple',    3499.99),
    ('PROD-012', 'Dell XPS 15',                    7,  'Laptops',         'Dell',     1899.99),
    ('PROD-013', 'Lenovo ThinkPad X1 Carbon',     7,  'Laptops',         'Lenovo',   1799.99),
    ('PROD-014', 'ASUS ZenBook Pro Duo',           7,  'Laptops',         'Asus',     1599.99),
    ('PROD-015', 'HP Spectre x360',               7,  'Laptops',         'HP',       1499.99),
    ('PROD-016', 'Microsoft Surface Laptop 6',    7,  'Laptops',         'Microsoft',1299.99),
    ('PROD-017', 'Razer Blade 16',                7,  'Laptops',         'Razer',    3299.99),
    ('PROD-018', 'LG Gram 17',                    7,  'Laptops',         'LG',       1399.99),
    ('PROD-019', 'Samsung Galaxy Book4 Pro',      7,  'Laptops',         'Samsung',  1499.99),
    ('PROD-020', 'Acer Swift 5',                  7,  'Laptops',         'Acer',      999.99),
    ('PROD-021', 'Sony WH-1000XM5',              8,  'Headphones',      'Sony',      349.99),
    ('PROD-022', 'Apple AirPods Pro 2',           8,  'Earbuds',         'Apple',     249.99),
    ('PROD-023', 'Bose QuietComfort 45',          8,  'Headphones',      'Bose',      329.99),
    ('PROD-024', 'Sennheiser Momentum 4',         8,  'Headphones',      'Sennheiser',349.99),
    ('PROD-025', 'JBL Flip 6',                   8,  'Speakers',        'JBL',        99.99),
    ('PROD-026', 'Marshall Emberton III',         8,  'Speakers',        'Marshall',  129.99),
    ('PROD-027', 'Sonos Era 100',                8,  'Speakers',        'Sonos',      249.99),
    ('PROD-028', 'Audio-Technica ATH-M50xBT2',  8,  'Headphones',      'Audio-Technica',149.99),
    ('PROD-029', 'Jabra Elite 10',               8,  'Earbuds',         'Jabra',      249.99),
    ('PROD-030', 'Samsung Galaxy Buds3 Pro',     8,  'Earbuds',         'Samsung',    229.99),
    ('PROD-031', 'Sony Alpha 7 IV',              9,  'Mirrorless',      'Sony',      2499.99),
    ('PROD-032', 'Canon EOS R6 Mark II',         9,  'Mirrorless',      'Canon',     2499.99),
    ('PROD-033', 'Nikon Z6III',                  9,  'Mirrorless',      'Nikon',     2499.99),
    ('PROD-034', 'Fujifilm X-T5',               9,  'Mirrorless',      'Fujifilm',  1699.99),
    ('PROD-035', 'OM System OM-5',              9,  'Mirrorless',      'OM System',  999.99),
    ('PROD-036', 'GoPro Hero 12 Black',         9,  'Action Cameras',  'GoPro',      399.99),
    ('PROD-037', 'DJI Osmo Action 4',           9,  'Action Cameras',  'DJI',        349.99),
    ('PROD-038', 'Insta360 X4',                 9,  '360 Cameras',     'Insta360',   499.99),
    ('PROD-039', 'Ricoh GR IIIx',              9,  'Compact',         'Ricoh',      999.99),
    ('PROD-040', 'Leica Q3',                    9,  'Compact',         'Leica',     5995.99),
    ('PROD-041', 'Levi 501 Original Fit Jeans',10, 'Jeans',           'Levis',       98.00),
    ('PROD-042', 'Nike Dri-FIT ADV T-Shirt',   10, 'T-Shirts',        'Nike',        55.00),
    ('PROD-043', 'Adidas Tiro 23 Track Jacket',10, 'Jackets',         'Adidas',      80.00),
    ('PROD-044', 'Ralph Lauren Oxford Shirt',   10, 'Shirts',          'Ralph Lauren',89.50),
    ('PROD-045', 'Tommy Hilfiger Chino Pants',  10, 'Trousers',        'Tommy Hilfiger',85.00),
    ('PROD-046', 'Zara Smart Blazer',           10, 'Blazers',         'Zara',        99.90),
    ('PROD-047', 'H&M Slim Fit Suit',          10, 'Suits',           'H&M',        149.99),
    ('PROD-048', 'Uniqlo Merino Sweater',       10, 'Knitwear',        'Uniqlo',      59.90),
    ('PROD-049', 'Calvin Klein Underwear Pack', 10, 'Underwear',       'Calvin Klein',39.99),
    ('PROD-050', 'Barbour Wax Jacket',          10, 'Jackets',         'Barbour',    299.00)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO users (user_id, email, country, acquisition_channel, segment, email_verified) VALUES
    ('USR-001', 'alice.johnson@example.com',   'US', 'organic_search',   'champion',      TRUE),
    ('USR-002', 'bob.smith@example.com',       'GB', 'paid_social',      'loyal',         TRUE),
    ('USR-003', 'carol.white@example.com',     'CA', 'email',            'at_risk',       TRUE),
    ('USR-004', 'david.brown@example.com',     'AU', 'direct',           'new',           FALSE),
    ('USR-005', 'emma.davis@example.com',      'DE', 'referral',         'promising',     TRUE),
    ('USR-006', 'frank.miller@example.com',    'FR', 'organic_search',   'loyal',         TRUE),
    ('USR-007', 'grace.wilson@example.com',    'US', 'paid_search',      'champion',      TRUE),
    ('USR-008', 'henry.moore@example.com',     'IN', 'organic_social',   'hibernating',   FALSE),
    ('USR-009', 'isabella.taylor@example.com', 'BR', 'affiliate',        'need_attention',TRUE),
    ('USR-010', 'james.anderson@example.com',  'JP', 'direct',           'loyal',         TRUE)
ON CONFLICT (user_id) DO NOTHING;
