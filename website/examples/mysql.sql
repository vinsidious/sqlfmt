-- MySQL example file: exercises MySQL-specific syntax across DML, DDL, functions, and session commands.

/* ===== Session and utility ===== */

SET NAMES utf8mb4;

SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';

SHOW VARIABLES LIKE 'innodb_buffer_pool_size';

SHOW CREATE TABLE orders;

EXPLAIN FORMAT=JSON SELECT * FROM orders WHERE customer_id = 42;

EXPLAIN FORMAT=TREE SELECT o.id, o.total FROM orders o INNER JOIN customers c ON c.id = o.customer_id WHERE c.region = 'EMEA';

/* ===== DDL: CREATE TABLE with MySQL types and options ===== */

CREATE TABLE users (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, username VARCHAR(64) NOT NULL, email VARCHAR(255) NOT NULL, bio TINYTEXT, description MEDIUMTEXT, profile_html LONGTEXT, avatar MEDIUMBLOB, role ENUM('admin', 'editor', 'viewer') NOT NULL DEFAULT 'viewer', tags SET('featured', 'verified', 'premium'), status TINYINT UNSIGNED NOT NULL DEFAULT 1, login_count INT UNSIGNED NOT NULL DEFAULT 0, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (id), UNIQUE KEY uk_username (username), UNIQUE KEY uk_email (email), KEY idx_status (status), KEY idx_created (created_at)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE products (id INT UNSIGNED NOT NULL AUTO_INCREMENT, sku VARCHAR(32) NOT NULL, name VARCHAR(200) NOT NULL, description TEXT, price DECIMAL(10,2) UNSIGNED NOT NULL, weight_kg DECIMAL(8,3), metadata JSON, search_body TEXT, name_norm VARCHAR(200) GENERATED ALWAYS AS (LOWER(TRIM(name))) STORED, price_with_tax DECIMAL(12,2) GENERATED ALWAYS AS (price * 1.20) VIRTUAL, is_active TINYINT(1) NOT NULL DEFAULT 1, PRIMARY KEY (id), UNIQUE KEY uk_sku (sku), FULLTEXT KEY ft_search (name, description, search_body), KEY idx_price (price), CHECK (price >= 0), CHECK (weight_kg IS NULL OR weight_kg > 0)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Spatial and fulltext index table
CREATE TABLE locations (id INT UNSIGNED NOT NULL AUTO_INCREMENT, name VARCHAR(128) NOT NULL, coords POINT NOT NULL SRID 4326, notes TEXT, PRIMARY KEY (id), SPATIAL KEY idx_coords (coords), FULLTEXT KEY ft_notes (notes)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* Partitioned tables */
CREATE TABLE audit_log (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, event_type VARCHAR(50) NOT NULL, payload JSON, created_at DATE NOT NULL, PRIMARY KEY (id, created_at), KEY idx_event (event_type)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 PARTITION BY RANGE (YEAR(created_at)) (PARTITION p2022 VALUES LESS THAN (2023), PARTITION p2023 VALUES LESS THAN (2024), PARTITION p2024 VALUES LESS THAN (2025), PARTITION p_future VALUES LESS THAN MAXVALUE);

CREATE TABLE cache_entries (id INT UNSIGNED NOT NULL AUTO_INCREMENT, cache_key VARCHAR(255) NOT NULL, value BLOB, PRIMARY KEY (id, cache_key)) ENGINE=InnoDB PARTITION BY KEY(cache_key) PARTITIONS 8;

CREATE TABLE event_codes (id INT UNSIGNED NOT NULL AUTO_INCREMENT, code TINYINT UNSIGNED NOT NULL, label VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB PARTITION BY LIST(code) (PARTITION p_info VALUES IN (1, 2, 3), PARTITION p_warn VALUES IN (4, 5), PARTITION p_error VALUES IN (6, 7, 8, 9));

CREATE TABLE metrics (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, sensor_id INT UNSIGNED NOT NULL, reading DOUBLE NOT NULL, recorded_at DATETIME NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB PARTITION BY HASH(sensor_id) PARTITIONS 16;

/* ===== DDL: ALTER TABLE ===== */

ALTER TABLE products ADD FULLTEXT KEY ft_name (name);

ALTER TABLE products ADD INDEX idx_active_price (is_active, price);

ALTER TABLE products MODIFY COLUMN description MEDIUMTEXT;

ALTER TABLE users ADD COLUMN phone VARCHAR(20) DEFAULT NULL AFTER email;

CREATE INDEX idx_sku_active ON products (sku, is_active) USING BTREE;

/* ===== DML: INSERT variants ===== */

INSERT INTO users (username, email, role) VALUES ('alice', 'alice@example.com', 'admin'), ('bob', 'bob@example.com', 'editor'), ('carol', 'carol@example.com', 'viewer');

-- ON DUPLICATE KEY UPDATE with VALUES() form
INSERT INTO products (sku, name, price) VALUES ('WIDGET-01', 'Widget', 9.99) ON DUPLICATE KEY UPDATE name = VALUES(name), price = VALUES(price);

-- ON DUPLICATE KEY UPDATE with alias form (MySQL 8.0.19+)
INSERT INTO products (sku, name, price) VALUES ('GADGET-01', 'Gadget', 19.99) AS new_row ON DUPLICATE KEY UPDATE name = new_row.name, price = new_row.price;

INSERT INTO products (sku, name, price) VALUES ('BOLT-05', 'Hex Bolt M5', 0.15), ('BOLT-08', 'Hex Bolt M8', 0.22), ('NUT-05', 'Hex Nut M5', 0.08) AS new_vals ON DUPLICATE KEY UPDATE price = new_vals.price;

REPLACE INTO products (sku, name, price) VALUES ('WIDGET-01', 'Widget Pro', 12.99);

REPLACE INTO cache_entries (id, cache_key, value) VALUES (1, 'session:abc', X'DEADBEEF');

/* ===== DML: SELECT with MySQL-specific features ===== */

SELECT STRAIGHT_JOIN c.username, o.id, o.total FROM customers c INNER JOIN orders o ON o.customer_id = c.id WHERE c.region = 'NA' ORDER BY o.total DESC LIMIT 20;

-- RLIKE / REGEXP operators
SELECT id, username FROM users WHERE username RLIKE '^[a-z]{3,}[0-9]*$';

SELECT id, email FROM users WHERE email REGEXP '.*@(example\\.com|test\\.org)$';

-- GROUP_CONCAT with ORDER BY and SEPARATOR
SELECT customer_id, GROUP_CONCAT(product_name ORDER BY product_name ASC SEPARATOR ', ') AS product_list FROM order_items GROUP BY customer_id;

SELECT department_id, GROUP_CONCAT(DISTINCT role ORDER BY role SEPARATOR ' | ') AS roles FROM employees GROUP BY department_id HAVING COUNT(*) > 3;

-- IFNULL / IF() / COALESCE
SELECT id, IFNULL(nickname, username) AS display_name, IF(status = 1, 'Active', 'Inactive') AS status_label, IF(login_count > 100, 'power_user', IF(login_count > 10, 'regular', 'new')) AS user_tier FROM users;

-- Date functions
SELECT id, username, CURDATE() AS today, CURTIME() AS current_time_val, NOW() AS current_timestamp_val, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') AS formatted_date, DATEDIFF(CURDATE(), created_at) AS days_since_registration FROM users WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 90 DAY);

-- LIMIT with offset
SELECT id, name, price FROM products WHERE is_active = 1 ORDER BY created_at DESC LIMIT 20, 10;

SELECT id, name FROM products ORDER BY price DESC LIMIT 0, 50;

-- Full-text search with MATCH ... AGAINST
SELECT id, name, MATCH(name, description, search_body) AGAINST ('ergonomic keyboard' IN NATURAL LANGUAGE MODE) AS relevance FROM products WHERE MATCH(name, description, search_body) AGAINST ('ergonomic keyboard' IN NATURAL LANGUAGE MODE) ORDER BY relevance DESC LIMIT 25;

SELECT id, name FROM products WHERE MATCH(name, description) AGAINST ('+wireless -bluetooth' IN BOOLEAN MODE);

SELECT id, name FROM products WHERE MATCH(name) AGAINST ('keyboard' WITH QUERY EXPANSION);

-- SELECT INTO variable
SELECT COUNT(*), MAX(price) INTO @total_products, @max_price FROM products WHERE is_active = 1;

SELECT GROUP_CONCAT(sku ORDER BY price DESC SEPARATOR ',') INTO @top_skus FROM products WHERE price > 100;

-- SELECT INTO OUTFILE
SELECT id, sku, name, price FROM products WHERE is_active = 1 INTO OUTFILE '/tmp/active_products.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

/* ===== JSON operations ===== */

SELECT id, JSON_EXTRACT(metadata, '$.color') AS color, JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.brand')) AS brand, metadata->>'$.dimensions.width' AS width, metadata->'$.tags' AS tags_json FROM products WHERE JSON_EXTRACT(metadata, '$.weight') > 2.5;

SELECT p.id, p.name, jt.tag_name, jt.tag_priority FROM products p, JSON_TABLE(p.metadata, '$.tags[*]' COLUMNS (tag_name VARCHAR(64) PATH '$.name', tag_priority INT PATH '$.priority' DEFAULT '0' ON EMPTY)) AS jt WHERE jt.tag_priority >= 5;

SELECT p.id, p.name, attr.attr_key, attr.attr_val FROM products p, JSON_TABLE(p.metadata, '$.attributes[*]' COLUMNS (attr_key VARCHAR(100) PATH '$.key', attr_val VARCHAR(255) PATH '$.value', attr_type VARCHAR(50) PATH '$.type' DEFAULT '"string"' ON EMPTY NULL ON ERROR)) AS attr WHERE attr.attr_key IS NOT NULL ORDER BY p.id, attr.attr_key;

/* ===== Window functions ===== */

SELECT id, name, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS price_rank, RANK() OVER (ORDER BY price DESC) AS price_rank_with_ties, DENSE_RANK() OVER (ORDER BY price DESC) AS dense_price_rank, NTILE(4) OVER (ORDER BY price) AS price_quartile FROM products WHERE is_active = 1;

SELECT id, name, category_id, price, LAG(price, 1) OVER (PARTITION BY category_id ORDER BY price) AS prev_price, LEAD(price, 1) OVER (PARTITION BY category_id ORDER BY price) AS next_price, FIRST_VALUE(name) OVER (PARTITION BY category_id ORDER BY price ASC) AS cheapest_in_category, LAST_VALUE(name) OVER (PARTITION BY category_id ORDER BY price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS priciest_in_category FROM products;

-- Running totals
SELECT order_date, daily_total, SUM(daily_total) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total, AVG(daily_total) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS seven_day_avg FROM daily_sales;

-- Named WINDOW clause
SELECT id, name, category_id, price, ROW_NUMBER() OVER w AS rn, RANK() OVER w AS rnk, SUM(price) OVER w AS category_running_total FROM products WINDOW w AS (PARTITION BY category_id ORDER BY price);

SELECT id, customer_id, total, order_date, SUM(total) OVER monthly AS month_total, COUNT(*) OVER monthly AS month_orders, AVG(total) OVER yearly AS year_avg FROM orders WINDOW monthly AS (PARTITION BY customer_id, YEAR(order_date), MONTH(order_date) ORDER BY order_date), yearly AS (PARTITION BY customer_id, YEAR(order_date));

/* ===== Recursive CTE for hierarchy ===== */

WITH RECURSIVE category_tree AS (SELECT id, name, parent_id, 0 AS depth, CAST(name AS CHAR(1000)) AS path FROM categories WHERE parent_id IS NULL UNION ALL SELECT c.id, c.name, c.parent_id, ct.depth + 1, CONCAT(ct.path, ' > ', c.name) FROM categories c INNER JOIN category_tree ct ON ct.id = c.parent_id WHERE ct.depth < 10) SELECT id, name, depth, path FROM category_tree ORDER BY path;

WITH RECURSIVE employee_chain AS (SELECT id, name, manager_id, 1 AS level FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id, ec.level + 1 FROM employees e INNER JOIN employee_chain ec ON ec.id = e.manager_id) SELECT id, name, level FROM employee_chain ORDER BY level, name;

/* ===== Upsert patterns ===== */

INSERT INTO product_stats (product_id, view_count, last_viewed) VALUES (101, 1, NOW()) ON DUPLICATE KEY UPDATE view_count = view_count + 1, last_viewed = NOW();

INSERT INTO inventory (warehouse_id, product_id, quantity) VALUES (1, 101, 50), (1, 102, 30), (2, 101, 75) AS incoming ON DUPLICATE KEY UPDATE quantity = inventory.quantity + incoming.quantity;

INSERT INTO daily_aggregates (metric_date, total_orders, total_revenue) SELECT DATE(order_date) AS metric_date, COUNT(*) AS total_orders, SUM(total) AS total_revenue FROM orders WHERE order_date >= CURDATE() - INTERVAL 1 DAY AND order_date < CURDATE() GROUP BY DATE(order_date) ON DUPLICATE KEY UPDATE total_orders = VALUES(total_orders), total_revenue = VALUES(total_revenue);

/* ===== Pagination pattern ===== */

SELECT SQL_CALC_FOUND_ROWS p.id, p.name, p.price, c.name AS category_name FROM products p LEFT JOIN categories c ON c.id = p.category_id WHERE p.is_active = 1 AND p.price BETWEEN 10 AND 500 ORDER BY p.name ASC LIMIT 50, 25;

/* ===== DELIMITER blocks: triggers, procedures, functions ===== */

DELIMITER $$

CREATE TRIGGER trg_users_before_insert
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    SET NEW.username = LOWER(TRIM(NEW.username));
    SET NEW.email = LOWER(TRIM(NEW.email));
    IF NEW.created_at IS NULL THEN
        SET NEW.created_at = NOW();
    END IF;
END$$

DELIMITER ;

DELIMITER $$

CREATE TRIGGER trg_orders_after_update
AFTER UPDATE ON orders
FOR EACH ROW
BEGIN
    IF OLD.status <> NEW.status THEN
        INSERT INTO order_status_log (order_id, old_status, new_status, changed_at)
        VALUES (NEW.id, OLD.status, NEW.status, NOW());
    END IF;
    IF NEW.status = 'shipped' AND OLD.status <> 'shipped' THEN
        UPDATE inventory SET quantity = quantity - NEW.quantity WHERE product_id = NEW.product_id AND warehouse_id = NEW.warehouse_id;
    END IF;
END$$

DELIMITER ;

DELIMITER $$

CREATE PROCEDURE sp_get_user_orders(IN p_user_id BIGINT, IN p_status VARCHAR(20), OUT p_total DECIMAL(12,2))
BEGIN
    DECLARE v_count INT DEFAULT 0;
    DECLARE v_avg DECIMAL(10,2);
    SELECT COUNT(*), AVG(total) INTO v_count, v_avg FROM orders WHERE customer_id = p_user_id AND (p_status IS NULL OR status = p_status);
    IF v_count = 0 THEN
        SET p_total = 0;
    ELSE
        SELECT SUM(total) INTO p_total FROM orders WHERE customer_id = p_user_id AND (p_status IS NULL OR status = p_status);
    END IF;
    SELECT o.id, o.total, o.status, o.order_date, GROUP_CONCAT(oi.product_name ORDER BY oi.product_name SEPARATOR ', ') AS items FROM orders o LEFT JOIN order_items oi ON oi.order_id = o.id WHERE o.customer_id = p_user_id AND (p_status IS NULL OR o.status = p_status) GROUP BY o.id, o.total, o.status, o.order_date ORDER BY o.order_date DESC;
END$$

DELIMITER ;

DELIMITER $$

CREATE FUNCTION fn_calculate_discount(p_order_total DECIMAL(12,2), p_customer_tier VARCHAR(20))
RETURNS DECIMAL(12,2)
DETERMINISTIC
BEGIN
    DECLARE v_discount DECIMAL(5,4);
    IF p_customer_tier = 'platinum' THEN
        SET v_discount = 0.15;
    ELSEIF p_customer_tier = 'gold' THEN
        SET v_discount = 0.10;
    ELSEIF p_customer_tier = 'silver' THEN
        SET v_discount = 0.05;
    ELSE
        SET v_discount = 0.00;
    END IF;
    IF p_order_total > 1000 THEN
        SET v_discount = v_discount + 0.02;
    END IF;
    RETURN ROUND(p_order_total * v_discount, 2);
END$$

DELIMITER ;

/* ===== More DML patterns ===== */

-- Multi-table UPDATE
UPDATE products p INNER JOIN categories c ON c.id = p.category_id SET p.is_active = 0 WHERE c.name = 'Discontinued' AND p.price < 5.00;

-- Multi-table DELETE
DELETE oi FROM order_items oi INNER JOIN orders o ON o.id = oi.order_id WHERE o.status = 'cancelled' AND o.order_date < DATE_SUB(CURDATE(), INTERVAL 1 YEAR);

-- EXISTS subquery with correlated reference
SELECT c.id, c.username, c.email FROM users c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.total > 500 AND o.order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY));

-- CASE expression with nested IF
SELECT id, name, price, CASE WHEN price < 10 THEN 'budget' WHEN price BETWEEN 10 AND 99.99 THEN 'mid-range' WHEN price >= 100 THEN 'premium' ELSE 'unknown' END AS price_tier, IF(price > (SELECT AVG(price) FROM products), 'above_avg', 'at_or_below_avg') AS vs_average FROM products WHERE is_active = 1 ORDER BY price DESC;

/* ===== Additional JSON document queries ===== */

SELECT id, name, metadata->>'$.manufacturer' AS manufacturer, JSON_LENGTH(metadata->'$.tags') AS tag_count, JSON_CONTAINS(metadata->'$.tags', '"wireless"') AS is_wireless, JSON_KEYS(metadata->'$.dimensions') AS dimension_keys FROM products WHERE JSON_VALID(metadata) AND JSON_TYPE(metadata->'$.price_history') = 'ARRAY';

UPDATE products SET metadata = JSON_SET(metadata, '$.last_reviewed', NOW(), '$.review_count', IFNULL(JSON_EXTRACT(metadata, '$.review_count'), 0) + 1) WHERE id = 42;

UPDATE products SET metadata = JSON_ARRAY_APPEND(metadata, '$.tags', JSON_OBJECT('name', 'clearance', 'added', NOW())) WHERE price < 5.00 AND is_active = 1;

/* ===== UNION and set operations ===== */

SELECT 'active' AS segment, COUNT(*) AS cnt, AVG(price) AS avg_price FROM products WHERE is_active = 1 UNION ALL SELECT 'inactive', COUNT(*), AVG(price) FROM products WHERE is_active = 0 UNION ALL SELECT 'all', COUNT(*), AVG(price) FROM products;

/* ===== Subqueries and derived tables ===== */

SELECT ranked.id, ranked.name, ranked.price, ranked.category_rank FROM (SELECT p.id, p.name, p.price, p.category_id, ROW_NUMBER() OVER (PARTITION BY p.category_id ORDER BY p.price DESC) AS category_rank FROM products p WHERE p.is_active = 1) ranked WHERE ranked.category_rank <= 3 ORDER BY ranked.category_id, ranked.category_rank;

/* ===== EXPLAIN with FORMAT ===== */

EXPLAIN ANALYZE SELECT p.id, p.name, p.price FROM products p WHERE MATCH(p.name, p.description) AGAINST ('mechanical keyboard' IN BOOLEAN MODE) AND p.is_active = 1 ORDER BY p.price LIMIT 10;
