#!python config

oltp_conn_string = "postgresql://postgres:123456@0.0.0.0:5432/ftde_project01_oltp"
warehouse_conn_string = "postgresql://postgres:123456@0.0.0.0:5432/ftde_project01_dwh"

oltp_tables = {
    "users": "tb_users",
    "payments": "tb_payments",
    "shippers": "tb_shippers",
    "ratings": "tb_ratings",
    "vouchers": "tb_vouchers",
    "products": "tb_products",
    "product_category": "tb_product_category",
    "orders": "tb_orders",
    "order_items": "tb_order_items",
}

warehouse_tables = {
    "users": "dim_user",
    "payments": "dim_payment",
    "shippers": "dim_shipper",
    "ratings": "dim_rating",
    "vouchers": "dim_voucher",
    "products": "dim_product",
    "product_category": "dim_product_category",
    "orders": "fact_orders",
    "order_items": "fact_order_items",
}

datamart_tables = {
    "sales": "dm_sales",
    "sales_per_category_product": "dm_sales_per_category_product",
    "sales_per_payment_method": "dm_sales_per_payment_method",
    "sales_per_shipper": "dm_sales_per_shipper",
    "sales_per_user": "dm_sales_per_user",
    "discount_voucher_trend": "dm_discount_voucher_trend",
    "sales_performance_per_region": "dm_sales_performance_per_region",
    "profit_margin_per_category": "dm_profit_margin_per_category",
    "average_order_value_per_user": "dm_average_order_value_per_user",
    "voucher_conversion_rate": "dm_voucher_conversion_rate",
}

dimension_columns = {
    "dim_user": ["user_id", "user_first_name", "user_last_name", "user_gender", "user_address", "user_birthday", "user_join"],
    "dim_payment": ["payment_id", "payment_name", "payment_status"],
    "dim_shipper": ["shipper_id", "shipper_name"],
    "dim_rating": ["rating_id", "rating_level", "rating_status"],
    "dim_voucher": ["voucher_id", "voucher_name", "voucher_price", "voucher_created", "user_id"], 
    "dim_product": ["product_id", "product_category_id", "product_name", "product_created", "product_price", "product_discount"], 
    "dim_product_category": ["product_category_id", "product_category_name"], 
    "fact_orders": ['order_id', 'order_date', 'user_id', 'payment_id', 'shipper_id', 'order_price', 'order_discount', 'voucher_id', 'order_total', 'rating_id'],
    "fact_order_items": ['order_item_id', 'order_id', 'product_id', 'order_item_quantity', 'product_discount', 'product_subdiscount', 'product_price', 'product_subprice']
}

ddl_statements = {
    "dim_user": """
        CREATE TABLE IF NOT EXISTS dim_user (
            user_id INT NOT NULL PRIMARY KEY,
            user_first_name VARCHAR(255) NOT NULL,
            user_last_name VARCHAR(255) NOT NULL,
            user_gender VARCHAR(50) NOT NULL,
            user_address VARCHAR(255),
            user_birthday DATE NOT NULL,
            user_join DATE NOT NULL
        );
    """,
    "dim_payment": """
        CREATE TABLE IF NOT EXISTS dim_payment (
            payment_id INT NOT NULL PRIMARY KEY,
            payment_name VARCHAR(255) NOT NULL,
            payment_status BOOLEAN NOT NULL
        );
    """,
    "dim_shipper": """
        CREATE TABLE IF NOT EXISTS dim_shipper (
            shipper_id INT NOT NULL PRIMARY KEY,
            shipper_name VARCHAR(255) NOT NULL
        );
    """,
    "dim_rating": """
        CREATE TABLE IF NOT EXISTS dim_rating (
            rating_id INT NOT NULL PRIMARY KEY,
            rating_level INT NOT NULL,
            rating_status VARCHAR(255) NOT NULL
        );
    """,
    "dim_voucher": """
        CREATE TABLE IF NOT EXISTS dim_voucher (
            voucher_id INT NOT NULL PRIMARY KEY,
            voucher_name VARCHAR(255) NOT NULL,
            voucher_price INT,
            voucher_created DATE NOT NULL,
            user_id INT NOT NULL
        );
    """,
    "dim_product": """
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id INT NOT NULL PRIMARY KEY,
            product_category_id INT NOT NULL,
            product_name VARCHAR(255),
            product_created DATE NOT NULL,
            product_price INT NOT NULL,
            product_discount INT
        );
    """,
    "dim_product_category": """
        CREATE TABLE IF NOT EXISTS dim_product_category (
            product_category_id INT NOT NULL PRIMARY KEY,
            product_category_name VARCHAR(255) NOT NULL
        );
    """,
    "fact_orders": """
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id INT NOT NULL PRIMARY KEY,
            order_date DATE NOT NULL,
            user_id INT NOT NULL,
            payment_id INT NOT NULL,
            shipper_id INT NOT NULL,
            order_price INT NOT NULL,
            order_discount INT,
            voucher_id INT,
            order_total INT NOT NULL,
            rating_id INT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
            FOREIGN KEY (payment_id) REFERENCES dim_payment(payment_id),
            FOREIGN KEY (shipper_id) REFERENCES dim_shipper(shipper_id),
            FOREIGN KEY (voucher_id) REFERENCES dim_voucher(voucher_id),
            FOREIGN KEY (rating_id) REFERENCES dim_rating(rating_id)
        );
    """,
    "fact_order_items": """
        CREATE TABLE IF NOT EXISTS fact_order_items (
            order_item_id INT NOT NULL PRIMARY KEY,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            order_item_quantity INT NOT NULL,
            product_discount INT,
            product_subdiscount INT,
            product_price INT NOT NULL,
            product_subprice INT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
            FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
        );
    """
}

# Data mart definitions
ddl_marts = {
    "dm_sales": """
        CREATE TABLE IF NOT EXISTS dm_sales (
            order_id INT NOT NULL PRIMARY KEY,
            order_date DATE NOT NULL,
            user_id INT NOT NULL,
            user_name VARCHAR(255),
            payment_type VARCHAR(255),
            shipper_name VARCHAR(255),
            order_price INT NOT NULL,
            order_discount INT,
            voucher_name VARCHAR(255),
            order_total INT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
            FOREIGN KEY (user_id) REFERENCES dim_user(user_id)
        );
        TRUNCATE TABLE dm_sales; 
        INSERT INTO dm_sales (order_id, order_date, user_id, user_name, payment_type, shipper_name,
                                order_price, order_discount, voucher_name, order_total)
        SELECT fo.order_id, fo.order_date, fo.user_id, 
                du.user_first_name || ' ' || du.user_last_name, 
                dp.payment_name, ds.shipper_name, fo.order_price, 
                fo.order_discount, dv.voucher_name, fo.order_total
        FROM fact_orders fo
        INNER JOIN dim_user du ON fo.user_id = du.user_id
        INNER JOIN dim_payment dp ON fo.payment_id = dp.payment_id
        INNER JOIN dim_shipper ds ON fo.shipper_id = ds.shipper_id
        LEFT JOIN dim_voucher dv ON fo.voucher_id = dv.voucher_id;
    """,
    "dm_sales_per_category": """
        CREATE TABLE IF NOT EXISTS dm_sales_per_category_product (
            product_category_id INT PRIMARY KEY,
            category_name VARCHAR(255),
            total_sales INT
        );
        TRUNCATE TABLE dm_sales_per_category_product; 
        INSERT INTO dm_sales_per_category_product (product_category_id, category_name, total_sales)
        SELECT p.product_category_id, pc.product_category_name, SUM(foi.order_item_quantity)
        FROM fact_order_items foi
        INNER JOIN dim_product p ON foi.product_id = p.product_id
        INNER JOIN dim_product_category pc ON p.product_category_id = pc.product_category_id
        GROUP BY p.product_category_id, pc.product_category_name;
    """,
    "dm_sales_per_payment_method": """
        CREATE TABLE IF NOT EXISTS dm_sales_per_payment_method (
            payment_method VARCHAR(255) PRIMARY KEY,
            total_sales INT
        );
        TRUNCATE TABLE dm_sales_per_payment_method;
        INSERT INTO dm_sales_per_payment_method (payment_method, total_sales)
        SELECT dp.payment_name, COUNT(fo.order_id)
        FROM fact_orders fo
        INNER JOIN dim_payment dp ON fo.payment_id = dp.payment_id
        GROUP BY dp.payment_name;
    """,
    "dm_sales_per_shipper": """
        CREATE TABLE IF NOT EXISTS dm_sales_per_shipper (
            shipper_id INT PRIMARY KEY,
            shipper_name VARCHAR(255),
            total_sales INT
        );
        TRUNCATE TABLE dm_sales_per_shipper;
        INSERT INTO dm_sales_per_shipper (shipper_id, shipper_name, total_sales)
        SELECT fo.shipper_id, ds.shipper_name, COUNT(fo.order_id)
        FROM fact_orders fo
        INNER JOIN dim_shipper ds ON fo.shipper_id = ds.shipper_id
        GROUP BY fo.shipper_id, ds.shipper_name;
    """,
    "dm_sales_per_user": """
        CREATE TABLE IF NOT EXISTS dm_sales_per_user (
            user_id INT PRIMARY KEY,
            user_name VARCHAR(255),
            total_sales INT
        );
        TRUNCATE TABLE dm_sales_per_user;
        INSERT INTO dm_sales_per_user (user_id, user_name, total_sales)
        SELECT fo.user_id, CONCAT(du.user_first_name, ' ', du.user_last_name) AS user_name, COUNT(fo.order_id)
        FROM fact_orders fo
        INNER JOIN dim_user du ON fo.user_id = du.user_id
        GROUP BY fo.user_id, user_name;
    """,
    "dm_discount_voucher_trends": """
        CREATE TABLE IF NOT EXISTS dm_discount_voucher_trend (
            id SERIAL PRIMARY KEY,
            month_year DATE,
            voucher_name VARCHAR(255),
            total_usage INT,
            total_voucher_discounts INT
        );
        TRUNCATE TABLE dm_discount_voucher_trend;
        INSERT INTO dm_discount_voucher_trend (month_year, voucher_name, total_usage, total_voucher_discounts)
        SELECT
            DATE_TRUNC('month', fo.order_date) AS month_year,
            dv.voucher_name,
            COUNT(fo.order_id) AS total_usage, 
            SUM(dv.voucher_price) AS total_voucher_discounts
        FROM
            fact_orders fo
        JOIN dim_voucher dv on
            fo.voucher_id = dv.voucher_id
        GROUP BY  fo.order_date, dv.voucher_name
    """,
    "dm_sales_performance_per_region": """
        CREATE TABLE IF NOT EXISTS dm_sales_performance_per_region (
            region_name VARCHAR(255) PRIMARY KEY,
            total_sales INT
        );
        TRUNCATE TABLE dm_sales_performance_per_region;
        INSERT INTO dm_sales_performance_per_region (region_name, total_sales)
        SELECT split_part(du.user_address, ', ', -1) AS region_name, COUNT(fo.order_id) AS total_sales
        FROM fact_orders fo
        INNER JOIN dim_user du ON fo.user_id = du.user_id
        GROUP BY split_part(du.user_address, ', ', -1);
    """,
    "dm_profit_margin_per_category": """
        CREATE TABLE IF NOT EXISTS dm_profit_margin_per_category (
            product_category_id INT PRIMARY KEY,
            category_name VARCHAR(255),
            total_profit_margin INT
        );
        TRUNCATE TABLE dm_profit_margin_per_category;
        INSERT INTO dm_profit_margin_per_category (product_category_id, category_name, total_profit_margin)
        SELECT p.product_category_id, pc.product_category_name,
               SUM((foi.product_price - foi.product_discount) * foi.order_item_quantity)
        FROM fact_order_items foi
        INNER JOIN dim_product p ON foi.product_id = p.product_id
        INNER JOIN dim_product_category pc ON p.product_category_id = pc.product_category_id
        GROUP BY p.product_category_id, pc.product_category_name;
    """,
    "dm_average_order_value_per_user": """
        CREATE TABLE IF NOT EXISTS dm_average_order_value_per_user (
            user_id INT PRIMARY KEY,
            user_name VARCHAR(255),
            average_order_value DECIMAL(10, 2)
        );
        TRUNCATE TABLE dm_average_order_value_per_user;
        INSERT INTO dm_average_order_value_per_user (user_id, user_name, average_order_value)
        SELECT fo.user_id, CONCAT(du.user_first_name, ' ', du.user_last_name) AS user_name,
                AVG(fo.order_total) AS average_order_value
        FROM fact_orders fo
        INNER JOIN dim_user du ON fo.user_id = du.user_id
        GROUP BY fo.user_id, user_name;
    """,
    "dm_voucher_conversion_rate": """
        CREATE TABLE IF NOT EXISTS dm_voucher_conversion_rate (
            voucher_name VARCHAR(255) PRIMARY KEY,
            conversion_rate DECIMAL(10, 2)
        );
        TRUNCATE TABLE dm_voucher_conversion_rate;
        INSERT INTO dm_voucher_conversion_rate (voucher_name, conversion_rate)
        SELECT dv.voucher_name,
            COALESCE(SUM(CASE WHEN fo.voucher_id IS NOT NULL THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)::DECIMAL, 0) AS conversion_rate
        FROM dim_voucher dv
        LEFT JOIN fact_orders fo ON dv.voucher_id = fo.voucher_id
        GROUP BY dv.voucher_name;
    """
}