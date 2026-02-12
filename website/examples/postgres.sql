-- PostgreSQL feature tour: dense unformatted SQL for formatter exercising

/* ===== EXTENSIONS AND SESSION CONFIG ===== */

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE EXTENSION IF NOT EXISTS btree_gist;

SET search_path TO public, extensions;

/* ===== ENUM AND COMPOSITE TYPES ===== */

CREATE TYPE order_status AS ENUM ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned');

CREATE TYPE address_type AS ENUM ('billing', 'shipping', 'office', 'warehouse');

CREATE TYPE currency_code AS ENUM ('USD', 'EUR', 'GBP', 'JPY', 'CAD');

/* ===== TABLE DEFINITIONS WITH POSTGRESQL-SPECIFIC TYPES ===== */

CREATE TABLE organizations (id UUID DEFAULT uuid_generate_v4() PRIMARY KEY, name TEXT NOT NULL, slug TEXT NOT NULL UNIQUE, metadata JSONB DEFAULT '{}'::jsonb, search_vector TSVECTOR, allowed_ips INET[], internal_network CIDR, mac_address MACADDR, active_period TSTZRANGE, created_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now());

CREATE TABLE users (id BIGSERIAL PRIMARY KEY, org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE, email TEXT NOT NULL, display_name TEXT NOT NULL, password_hash BYTEA NOT NULL, avatar_url TEXT, preferences JSONB DEFAULT '{"theme": "light", "notifications": true}'::jsonb, tags TEXT[] DEFAULT '{}', role TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner', 'admin', 'member', 'viewer')), last_login_ip INET, total_logins INTEGER DEFAULT 0 GENERATED ALWAYS AS IDENTITY, search_doc TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', coalesce(display_name, '') || ' ' || coalesce(email, ''))) STORED, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, UNIQUE (org_id, email));

CREATE TABLE products (id SERIAL PRIMARY KEY, sku TEXT NOT NULL UNIQUE, name TEXT NOT NULL, description TEXT, category TEXT NOT NULL, price NUMERIC(10, 2) NOT NULL CHECK (price > 0), cost NUMERIC(10, 2), weight_kg REAL, dimensions JSONB, attributes JSONB DEFAULT '{}', tags TEXT[] DEFAULT '{}', search_vector TSVECTOR, is_active BOOLEAN DEFAULT true, stock_count INTEGER DEFAULT 0, price_range INT4RANGE, created_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now());

CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, org_id UUID NOT NULL REFERENCES organizations (id), user_id BIGINT NOT NULL REFERENCES users (id), status order_status NOT NULL DEFAULT 'pending', currency currency_code NOT NULL DEFAULT 'USD', subtotal NUMERIC(12, 2) NOT NULL, tax NUMERIC(12, 2) NOT NULL DEFAULT 0, total NUMERIC(12, 2) GENERATED ALWAYS AS (subtotal + tax) STORED, shipping_address JSONB NOT NULL, billing_address JSONB, notes TEXT, line_items JSONB NOT NULL DEFAULT '[]', tracking_number TEXT, placed_at TIMESTAMPTZ DEFAULT now(), shipped_at TIMESTAMPTZ, delivered_at TIMESTAMPTZ);

CREATE TABLE order_events (id BIGSERIAL PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES orders (id) ON DELETE CASCADE, event_type TEXT NOT NULL, payload JSONB DEFAULT '{}', created_at TIMESTAMPTZ DEFAULT now(), created_by BIGINT REFERENCES users (id));

CREATE TABLE audit_log (id BIGSERIAL PRIMARY KEY, table_name TEXT NOT NULL, record_id TEXT NOT NULL, action TEXT NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')), old_data JSONB, new_data JSONB, changed_by BIGINT, ip_address INET, session_id UUID, created_at TIMESTAMPTZ DEFAULT now());

-- Job queue table for SKIP LOCKED pattern
CREATE TABLE job_queue (id BIGSERIAL PRIMARY KEY, queue_name TEXT NOT NULL DEFAULT 'default', payload JSONB NOT NULL, status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'dead')), priority INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3, locked_by TEXT, locked_at TIMESTAMPTZ, scheduled_for TIMESTAMPTZ NOT NULL DEFAULT now(), completed_at TIMESTAMPTZ, last_error TEXT, created_at TIMESTAMPTZ DEFAULT now());

/* ===== INDEXES: GIN, GiST, BRIN, PARTIAL, EXPRESSION, CONCURRENTLY ===== */

CREATE INDEX idx_org_metadata_gin ON organizations USING GIN (metadata);

CREATE INDEX idx_org_search ON organizations USING GIN (search_vector);

CREATE INDEX CONCURRENTLY idx_users_email_lower ON users ((lower(email)));

CREATE INDEX idx_users_preferences_gin ON users USING GIN (preferences jsonb_path_ops);

CREATE INDEX idx_users_tags_gin ON users USING GIN (tags);

CREATE INDEX idx_products_search ON products USING GIN (search_vector);

CREATE INDEX idx_products_attributes ON products USING GIN (attributes);

CREATE INDEX idx_products_active ON products (category, price) WHERE is_active = true;

CREATE INDEX idx_products_price_range ON products USING GiST (price_range);

CREATE INDEX idx_orders_placed ON orders USING BRIN (placed_at);

CREATE INDEX idx_orders_status ON orders (status) WHERE status NOT IN ('delivered', 'cancelled');

CREATE INDEX CONCURRENTLY idx_orders_user_status ON orders (user_id, status);

CREATE INDEX idx_audit_created ON audit_log USING BRIN (created_at);

CREATE INDEX idx_audit_table_record ON audit_log (table_name, record_id);

CREATE INDEX idx_job_queue_pending ON job_queue (queue_name, priority DESC, scheduled_for) WHERE status = 'pending';

CREATE INDEX CONCURRENTLY idx_job_queue_cleanup ON job_queue (completed_at) WHERE status IN ('completed', 'dead');

/* ===== COMMENTS ===== */

COMMENT ON TABLE organizations IS 'Multi-tenant organizations with JSONB metadata and network info';

COMMENT ON COLUMN users.search_doc IS 'Auto-generated tsvector for full-text search across user fields';

COMMENT ON INDEX idx_job_queue_pending IS 'Partial index for efficient queue polling with SKIP LOCKED';

/* ===== PUBLICATIONS ===== */

CREATE PUBLICATION analytics_pub FOR TABLE orders, order_events, audit_log;

ALTER PUBLICATION analytics_pub ADD TABLE products;

/* ===== BASIC DML: INSERT WITH RETURNING ===== */

INSERT INTO organizations (name, slug, metadata, allowed_ips, internal_network) VALUES ('Acme Corp', 'acme-corp', '{"plan": "enterprise", "seats": 500}'::jsonb, ARRAY['10.0.0.0/8'::inet, '192.168.1.0/24'::inet], '10.0.0.0/8'::cidr) RETURNING id, name, created_at;

INSERT INTO products (sku, name, description, category, price, cost, attributes, tags) VALUES ('WIDGET-001', 'Premium Widget', 'A high-quality widget for discerning customers', 'widgets', 49.99, 12.50, '{"color": "blue", "material": "titanium", "warranty_months": 24}'::jsonb, ARRAY['premium', 'new-arrival', 'featured']), ('GADGET-002', 'Standard Gadget', 'Reliable everyday gadget', 'gadgets', 29.99, 8.00, '{"color": "black", "material": "aluminum"}'::jsonb, ARRAY['standard', 'bestseller']), ('GIZMO-003', 'Compact Gizmo', 'Space-saving gizmo', 'gizmos', 19.99, 5.50, '{"color": "silver", "foldable": true}'::jsonb, ARRAY['compact', 'sale']) RETURNING id, sku, name;

/* ===== UPSERTS: ON CONFLICT ===== */

INSERT INTO products (sku, name, category, price, attributes) VALUES ('WIDGET-001', 'Premium Widget v2', 'widgets', 54.99, '{"color": "blue", "material": "carbon-fiber", "warranty_months": 36}'::jsonb) ON CONFLICT (sku) DO UPDATE SET name = EXCLUDED.name, price = EXCLUDED.price, attributes = products.attributes || EXCLUDED.attributes, updated_at = now() RETURNING id, sku, name, price;

INSERT INTO users (org_id, email, display_name, password_hash) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'alice@acme.com', 'Alice Smith', '\xDEADBEEF') ON CONFLICT (org_id, email) DO NOTHING;

INSERT INTO products (sku, name, category, price) VALUES ('TEMP-999', 'Temporary Product', 'misc', 9.99) ON CONFLICT (sku) DO UPDATE SET name = EXCLUDED.name, price = EXCLUDED.price WHERE products.price <> EXCLUDED.price RETURNING *;

/* ===== UPDATE WITH FROM AND RETURNING ===== */

UPDATE products SET search_vector = to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, '') || ' ' || coalesce(category, '')) WHERE search_vector IS NULL RETURNING id, sku;

UPDATE orders SET status = 'shipped', shipped_at = now(), tracking_number = s.tracking FROM (SELECT order_id, tracking_number AS tracking FROM external_shipments WHERE shipped_date = CURRENT_DATE) s WHERE orders.id = s.order_id AND orders.status = 'confirmed' RETURNING orders.id, orders.tracking_number;

/* ===== DELETE WITH USING AND RETURNING ===== */

DELETE FROM order_events USING orders WHERE order_events.order_id = orders.id AND orders.status = 'cancelled' AND orders.placed_at < now() - INTERVAL '90 days' RETURNING order_events.id, order_events.event_type;

DELETE FROM audit_log WHERE created_at < now() - INTERVAL '2 years' RETURNING id, table_name, action;

/* ===== DISTINCT ON ===== */

SELECT DISTINCT ON (o.user_id) o.user_id, o.id AS order_id, o.total, o.placed_at, o.status FROM orders o WHERE o.status <> 'cancelled' ORDER BY o.user_id, o.placed_at DESC;

SELECT DISTINCT ON (p.category) p.category, p.name, p.price, p.attributes->>'color' AS color FROM products p WHERE p.is_active = true ORDER BY p.category, p.price ASC;

SELECT DISTINCT ON (date_trunc('hour', e.created_at)) date_trunc('hour', e.created_at) AS hour, e.event_type, e.payload FROM order_events e WHERE e.created_at >= now() - INTERVAL '24 hours' ORDER BY date_trunc('hour', e.created_at), e.created_at DESC;

/* ===== JSONB OPERATORS AND FUNCTIONS ===== */

SELECT p.sku, p.name, p.attributes->'color' AS color_json, p.attributes->>'material' AS material_text, p.attributes#>'{warranty_months}' AS warranty_path, p.attributes#>>'{color}' AS color_path_text FROM products p WHERE p.attributes @> '{"material": "titanium"}'::jsonb AND p.attributes ? 'warranty_months';

SELECT o.id, o.shipping_address->>'city' AS city, o.shipping_address->>'state' AS state, jsonb_array_length(o.line_items) AS item_count, o.line_items->0->>'product_name' AS first_item FROM orders o WHERE o.shipping_address @> '{"country": "US"}'::jsonb AND o.line_items @> '[{"category": "widgets"}]'::jsonb;

SELECT p.sku, p.name FROM products p WHERE p.attributes ?| ARRAY['warranty_months', 'extended_warranty'] AND p.attributes ?& ARRAY['color', 'material'];

SELECT o.id, jsonb_build_object('order_id', o.id, 'status', o.status, 'total', o.total, 'items', o.line_items, 'placed', o.placed_at) AS order_summary FROM orders o WHERE o.placed_at >= now() - INTERVAL '7 days';

SELECT e.key, e.value, jsonb_typeof(e.value) AS val_type FROM products p, jsonb_each(p.attributes) e WHERE p.sku = 'WIDGET-001';

SELECT p.sku, item.value->>'name' AS item_name, (item.value->>'quantity')::integer AS qty FROM orders p, jsonb_array_elements(p.line_items) AS item WHERE p.status = 'pending';

SELECT p.id, jsonb_path_query_array(p.attributes, '$.* ? (@ > 10)') AS large_values FROM products p WHERE jsonb_path_query_array(p.attributes, '$.warranty_months ? (@ >= 24)') <> '[]'::jsonb;

SELECT jsonb_agg(jsonb_build_object('id', p.id, 'sku', p.sku, 'price', p.price) ORDER BY p.price DESC) AS product_catalog FROM products p WHERE p.is_active = true;

/* ===== ARRAY OPERATORS AND FUNCTIONS ===== */

SELECT p.sku, p.name, p.tags, array_length(p.tags, 1) AS tag_count FROM products p WHERE p.tags && ARRAY['premium', 'featured'] AND NOT p.tags @> ARRAY['discontinued'];

SELECT p.sku, p.name FROM products p WHERE p.tags <@ ARRAY['premium', 'new-arrival', 'featured', 'sale', 'bestseller'];

SELECT p.sku, p.name FROM products p WHERE 'featured' = ANY(p.tags) AND 'discontinued' <> ALL(p.tags);

SELECT unnest(p.tags) AS tag, COUNT(*) AS product_count FROM products p GROUP BY unnest(p.tags) ORDER BY product_count DESC;

SELECT p.category, array_agg(DISTINCT p.sku ORDER BY p.sku) AS skus, array_to_string(array_agg(DISTINCT p.name ORDER BY p.name), ', ') AS product_names FROM products p WHERE p.is_active = true GROUP BY p.category;

/* ===== LATERAL JOINS ===== */

SELECT o.id, o.status, o.total, latest_events.event_type, latest_events.created_at AS event_time FROM orders o LEFT JOIN LATERAL (SELECT oe.event_type, oe.created_at FROM order_events oe WHERE oe.order_id = o.id ORDER BY oe.created_at DESC LIMIT 3) latest_events ON true WHERE o.placed_at >= now() - INTERVAL '30 days';

SELECT u.display_name, u.email, top_orders.order_count, top_orders.total_spent FROM users u INNER JOIN LATERAL (SELECT COUNT(*) AS order_count, COALESCE(SUM(o.total), 0) AS total_spent FROM orders o WHERE o.user_id = u.id AND o.status <> 'cancelled') top_orders ON true WHERE top_orders.order_count > 0 ORDER BY top_orders.total_spent DESC;

SELECT p.sku, p.name, recent_orders.order_id, recent_orders.qty FROM products p CROSS JOIN LATERAL (SELECT o.id AS order_id, (li.value->>'quantity')::int AS qty FROM orders o, jsonb_array_elements(o.line_items) li WHERE li.value->>'sku' = p.sku ORDER BY o.placed_at DESC LIMIT 5) recent_orders;

/* ===== GENERATE_SERIES ===== */

SELECT d.day::date, COALESCE(order_stats.cnt, 0) AS order_count, COALESCE(order_stats.rev, 0) AS revenue FROM generate_series(CURRENT_DATE - INTERVAL '30 days', CURRENT_DATE, '1 day'::interval) d(day) LEFT JOIN (SELECT date_trunc('day', o.placed_at) AS day, COUNT(*) AS cnt, SUM(o.total) AS rev FROM orders o WHERE o.placed_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY date_trunc('day', o.placed_at)) order_stats ON d.day = order_stats.day ORDER BY d.day;

SELECT gs.hour, COALESCE(events.cnt, 0) AS event_count FROM generate_series(now() - INTERVAL '24 hours', now(), '1 hour'::interval) gs(hour) LEFT JOIN (SELECT date_trunc('hour', created_at) AS hour, COUNT(*) AS cnt FROM order_events GROUP BY 1) events ON gs.hour = events.hour ORDER BY gs.hour;

SELECT n FROM generate_series(1, 100) n WHERE n % 3 = 0 OR n % 5 = 0;

/* ===== WINDOW FUNCTIONS WITH ADVANCED FRAMING ===== */

SELECT o.id, o.user_id, o.total, o.placed_at, SUM(o.total) OVER (PARTITION BY o.user_id ORDER BY o.placed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total, AVG(o.total) OVER (PARTITION BY o.user_id ORDER BY o.placed_at ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS moving_avg, ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.placed_at DESC) AS recency_rank FROM orders o WHERE o.status <> 'cancelled';

SELECT p.category, p.name, p.price, RANK() OVER w AS price_rank, DENSE_RANK() OVER w AS dense_rank, PERCENT_RANK() OVER w AS pct_rank, NTILE(4) OVER w AS quartile, FIRST_VALUE(p.name) OVER w AS cheapest_in_category, LAST_VALUE(p.name) OVER (w RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS most_expensive, LAG(p.price, 1) OVER w AS prev_price, LEAD(p.price, 1) OVER w AS next_price FROM products p WHERE p.is_active = true WINDOW w AS (PARTITION BY p.category ORDER BY p.price ASC);

SELECT o.id, o.placed_at, o.total, SUM(o.total) OVER (ORDER BY o.placed_at RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW) AS week_rolling_sum, COUNT(*) OVER (ORDER BY o.placed_at RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS week_orders_excl_current, SUM(o.total) OVER (ORDER BY o.placed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) AS cumulative_excl_ties FROM orders o WHERE o.status = 'delivered';

/* ===== FILTER CLAUSE ON AGGREGATES ===== */

SELECT date_trunc('month', o.placed_at) AS month, COUNT(*) AS total_orders, COUNT(*) FILTER (WHERE o.status = 'delivered') AS delivered, COUNT(*) FILTER (WHERE o.status = 'cancelled') AS cancelled, COUNT(*) FILTER (WHERE o.status = 'returned') AS returned, SUM(o.total) FILTER (WHERE o.status = 'delivered') AS delivered_revenue, AVG(o.total) FILTER (WHERE o.total > 100) AS avg_large_order, SUM(o.total) FILTER (WHERE o.currency = 'USD') AS usd_revenue, SUM(o.total) FILTER (WHERE o.currency = 'EUR') AS eur_revenue FROM orders o GROUP BY date_trunc('month', o.placed_at) ORDER BY month DESC;

/* ===== STRING FUNCTIONS AND PATTERN MATCHING ===== */

SELECT u.email, regexp_replace(u.email, '^(.{2}).*(@.*)$', '\1***\2') AS masked_email, split_part(u.email, '@', 2) AS domain, u.display_name FROM users u WHERE u.email ILIKE '%@acme%' OR u.display_name SIMILAR TO '%(Smith|Jones|Williams)%';

SELECT p.sku, p.name, regexp_matches(p.sku, '^([A-Z]+)-(\d+)$') AS sku_parts, regexp_replace(p.name, '\s+', '-', 'g') AS slug FROM products p WHERE p.sku ~ '^[A-Z]+-\d{3}$';

SELECT u.org_id, string_agg(u.display_name, ', ' ORDER BY u.display_name) AS member_names, string_agg(DISTINCT u.role, ', ' ORDER BY u.role) AS roles, COUNT(*) AS member_count FROM users u GROUP BY u.org_id;

/* ===== DATE/TIME FUNCTIONS ===== */

SELECT o.id, o.placed_at, date_trunc('week', o.placed_at) AS order_week, date_part('dow', o.placed_at) AS day_of_week, date_part('hour', o.placed_at) AS hour_of_day, to_char(o.placed_at, 'YYYY-MM-DD HH24:MI:SS TZ') AS formatted_date, to_char(o.placed_at, 'Day, DD Mon YYYY') AS display_date, o.placed_at + INTERVAL '3 business days' AS est_processing, EXTRACT(EPOCH FROM now() - o.placed_at) / 3600 AS hours_since_placed FROM orders o WHERE o.placed_at >= date_trunc('quarter', CURRENT_DATE);

/* ===== RECURSIVE CTE WITH SEARCH AND CYCLE ===== */

WITH RECURSIVE org_hierarchy AS (SELECT id, name, parent_id, 0 AS depth, ARRAY[id] AS path, name::text AS full_path FROM departments WHERE parent_id IS NULL UNION ALL SELECT d.id, d.name, d.parent_id, oh.depth + 1, oh.path || d.id, oh.full_path || ' > ' || d.name FROM departments d INNER JOIN org_hierarchy oh ON d.parent_id = oh.id) SEARCH DEPTH FIRST BY name SET search_order CYCLE id SET is_cycle USING cycle_path SELECT * FROM org_hierarchy ORDER BY search_order;

WITH RECURSIVE subordinates AS (SELECT id, display_name, manager_id, 1 AS level FROM users WHERE id = 1 UNION ALL SELECT u.id, u.display_name, u.manager_id, s.level + 1 FROM users u INNER JOIN subordinates s ON u.manager_id = s.id WHERE s.level < 10) SELECT s.id, s.display_name, s.level, REPEAT('  ', s.level - 1) || s.display_name AS indented_name FROM subordinates s ORDER BY s.level, s.display_name;

/* ===== FULL-TEXT SEARCH ===== */

SELECT p.sku, p.name, p.description, ts_rank(p.search_vector, q.query) AS rank, ts_headline('english', p.description, q.query, 'StartSel=<b>, StopSel=</b>, MaxWords=35') AS headline FROM products p, to_tsquery('english', 'premium & (widget | gadget) & !discontinued') q WHERE p.search_vector @@ q.query ORDER BY rank DESC LIMIT 20;

SELECT o.name, ts_rank_cd(o.search_vector, plainto_tsquery('english', 'enterprise cloud platform')) AS relevance FROM organizations o WHERE o.search_vector @@ plainto_tsquery('english', 'enterprise cloud platform') ORDER BY relevance DESC;

/* ===== QUEUE POLLING WITH FOR UPDATE SKIP LOCKED ===== */

SELECT jq.id, jq.queue_name, jq.payload, jq.priority, jq.attempts FROM job_queue jq WHERE jq.status = 'pending' AND jq.scheduled_for <= now() AND jq.queue_name = 'email-notifications' ORDER BY jq.priority DESC, jq.scheduled_for ASC LIMIT 10 FOR UPDATE SKIP LOCKED;

SELECT o.id, o.status, o.total FROM orders o WHERE o.status = 'pending' AND o.placed_at < now() - INTERVAL '1 hour' ORDER BY o.placed_at FOR UPDATE OF o NOWAIT;

SELECT p.id, p.sku, p.stock_count FROM products p WHERE p.stock_count > 0 AND p.sku = 'WIDGET-001' FOR SHARE;

/* ===== CTE WITH INSERT, UPDATE, DELETE ===== */

WITH expired_orders AS (UPDATE orders SET status = 'cancelled' WHERE status = 'pending' AND placed_at < now() - INTERVAL '48 hours' RETURNING id, user_id, total), notifications AS (INSERT INTO order_events (order_id, event_type, payload) SELECT eo.id, 'auto_cancelled', jsonb_build_object('reason', 'expired', 'original_total', eo.total) FROM expired_orders eo RETURNING order_id, event_type) SELECT COUNT(*) AS cancelled_count, SUM(eo.total) AS cancelled_total FROM expired_orders eo;

/* ===== COMPLEX SUBQUERIES AND SET OPERATIONS ===== */

SELECT u.display_name, u.email FROM users u WHERE u.id IN (SELECT o.user_id FROM orders o WHERE o.total > 500 AND o.status = 'delivered' GROUP BY o.user_id HAVING COUNT(*) >= 3) UNION ALL SELECT 'VIP: ' || u.display_name, u.email FROM users u WHERE u.id = ANY(ARRAY(SELECT DISTINCT o.user_id FROM orders o WHERE o.total > 1000)) EXCEPT SELECT u.display_name, u.email FROM users u WHERE u.role = 'viewer';

/* ===== COPY ===== */

COPY products (sku, name, category, price) TO '/tmp/products_export.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

COPY audit_log (table_name, record_id, action, new_data, created_at) FROM '/tmp/audit_import.csv' WITH (FORMAT csv, HEADER true, NULL '\N');

/* ===== EXPLAIN ANALYZE ===== */

EXPLAIN ANALYZE SELECT p.category, COUNT(*) AS product_count, AVG(p.price) AS avg_price FROM products p WHERE p.is_active = true AND p.tags && ARRAY['premium'] GROUP BY p.category HAVING COUNT(*) > 5 ORDER BY avg_price DESC;

/* ===== LISTEN / NOTIFY ===== */

LISTEN order_updates;

NOTIFY order_updates, '{"order_id": 12345, "status": "shipped"}';

/* ===== VACUUM AND ANALYZE ===== */

VACUUM (VERBOSE, ANALYZE) orders;

ANALYZE products;

VACUUM FULL audit_log;

/* ===== MATERIALIZED VIEW ===== */

CREATE MATERIALIZED VIEW mv_daily_revenue AS SELECT date_trunc('day', o.placed_at)::date AS revenue_date, o.currency, COUNT(*) AS order_count, SUM(o.subtotal) AS gross_subtotal, SUM(o.tax) AS total_tax, SUM(o.total) AS gross_revenue, AVG(o.total) AS avg_order_value, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.total) AS median_order_value FROM orders o WHERE o.status IN ('confirmed', 'shipped', 'delivered') GROUP BY date_trunc('day', o.placed_at)::date, o.currency WITH DATA;

CREATE UNIQUE INDEX idx_mv_daily_revenue ON mv_daily_revenue (revenue_date, currency);

REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_revenue;

/* ===== VIEWS ===== */

CREATE OR REPLACE VIEW v_active_users AS SELECT u.id, u.display_name, u.email, u.role, org.name AS org_name, u.preferences->>'theme' AS theme, array_length(u.tags, 1) AS tag_count, u.created_at FROM users u INNER JOIN organizations org ON u.org_id = org.id WHERE u.created_at >= now() - INTERVAL '1 year';

/* ===== ADVANCED AGGREGATION PATTERNS ===== */

SELECT p.category, GROUPING(p.category) AS is_total, COUNT(*) AS cnt, SUM(p.price) AS total_price, AVG(p.price) AS avg_price, MIN(p.price) AS min_price, MAX(p.price) AS max_price FROM products p WHERE p.is_active = true GROUP BY ROLLUP (p.category) ORDER BY GROUPING(p.category), p.category;

SELECT date_trunc('month', o.placed_at) AS month, o.currency, o.status, COUNT(*) AS cnt, SUM(o.total) AS revenue FROM orders o WHERE o.placed_at >= '2024-01-01' GROUP BY CUBE (date_trunc('month', o.placed_at), o.currency, o.status) HAVING COUNT(*) > 0 ORDER BY month, o.currency, o.status;

/* ===== COMPLEX CASE EXPRESSIONS ===== */

SELECT o.id, o.total, o.status, CASE WHEN o.total >= 1000 THEN 'platinum' WHEN o.total >= 500 THEN 'gold' WHEN o.total >= 100 THEN 'silver' ELSE 'bronze' END AS tier, CASE o.status WHEN 'pending' THEN 'Awaiting confirmation' WHEN 'confirmed' THEN 'Order confirmed' WHEN 'shipped' THEN 'In transit since ' || to_char(o.shipped_at, 'Mon DD') WHEN 'delivered' THEN 'Delivered on ' || to_char(o.delivered_at, 'Mon DD') WHEN 'cancelled' THEN 'Cancelled' ELSE 'Unknown' END AS status_display FROM orders o ORDER BY o.placed_at DESC LIMIT 50 OFFSET 0;

/* ===== MERGE STATEMENT ===== */

MERGE INTO products p USING (SELECT sku, name, price, category FROM staging_products) s ON p.sku = s.sku WHEN MATCHED AND p.price <> s.price THEN UPDATE SET price = s.price, updated_at = now() WHEN NOT MATCHED THEN INSERT (sku, name, price, category) VALUES (s.sku, s.name, s.price, s.category);

/* ===== TABLESAMPLE ===== */

SELECT p.id, p.sku, p.name, p.price FROM products p TABLESAMPLE BERNOULLI (10) WHERE p.is_active = true;

/* ===== PG-SPECIFIC CASTS AND INTERVAL ARITHMETIC ===== */

SELECT o.id, o.placed_at, o.placed_at::date AS order_date, (now() - o.placed_at)::interval AS age, EXTRACT(DAY FROM now() - o.placed_at) AS days_old, o.total::text || ' ' || o.currency::text AS formatted_total FROM orders o WHERE o.placed_at::date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE;

/* ===== VALUES AS TABLE SOURCE ===== */

SELECT v.status, v.label FROM (VALUES ('pending', 'Pending Review'), ('confirmed', 'Confirmed'), ('shipped', 'In Transit'), ('delivered', 'Delivered'), ('cancelled', 'Cancelled'), ('returned', 'Returned')) AS v(status, label);

/* ===== COMPLEX JOIN PATTERNS ===== */

SELECT o.id AS order_id, u.display_name, org.name AS org_name, p_info.sku, p_info.name AS product_name, p_info.qty FROM orders o INNER JOIN users u ON o.user_id = u.id INNER JOIN organizations org ON o.org_id = org.id LEFT JOIN LATERAL (SELECT (li.value->>'sku') AS sku, (li.value->>'name') AS name, (li.value->>'quantity')::int AS qty FROM jsonb_array_elements(o.line_items) li) p_info ON true FULL OUTER JOIN order_events oe ON oe.order_id = o.id AND oe.event_type = 'shipped' CROSS JOIN (SELECT setting FROM pg_settings WHERE name = 'timezone') tz WHERE o.placed_at >= now() - INTERVAL '7 days' ORDER BY o.placed_at DESC;

/* ===== EXISTS AND NOT EXISTS ===== */

SELECT u.id, u.display_name, u.email FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'delivered' AND o.total > 200) AND NOT EXISTS (SELECT 1 FROM audit_log al WHERE al.changed_by = u.id AND al.action = 'DELETE' AND al.created_at >= now() - INTERVAL '30 days');

/* ===== ALTER TABLE STATEMENTS ===== */

ALTER TABLE products ADD COLUMN IF NOT EXISTS discontinued_at TIMESTAMPTZ;

ALTER TABLE products ALTER COLUMN description SET DEFAULT '';

ALTER TABLE users ADD CONSTRAINT chk_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'pending';

ALTER TABLE organizations OWNER TO app_admin;

ALTER TABLE audit_log SET SCHEMA archive;

/* ===== DROP STATEMENTS ===== */

DROP INDEX CONCURRENTLY IF EXISTS idx_temp_products;

DROP MATERIALIZED VIEW IF EXISTS mv_old_stats CASCADE;

DROP TABLE IF EXISTS temp_import CASCADE;

/* ===== GRANT / REVOKE ===== */

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO app_readwrite;

GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO app_readwrite;

REVOKE DELETE ON audit_log FROM app_readwrite;

/* ===== TRANSACTION BLOCK ===== */

BEGIN;

UPDATE products SET stock_count = stock_count - 1 WHERE sku = 'WIDGET-001' AND stock_count > 0;

INSERT INTO order_events (order_id, event_type, payload) VALUES (1, 'stock_decremented', '{"sku": "WIDGET-001", "new_count": 41}'::jsonb);

COMMIT;

/* ===== COMPLEX REAL-WORLD ANALYTICS QUERY ===== */

WITH monthly_cohorts AS (SELECT u.id AS user_id, date_trunc('month', u.created_at) AS cohort_month FROM users u), cohort_orders AS (SELECT mc.cohort_month, date_trunc('month', o.placed_at) AS order_month, mc.user_id, SUM(o.total) AS user_total FROM monthly_cohorts mc INNER JOIN orders o ON mc.user_id = o.user_id WHERE o.status IN ('confirmed', 'shipped', 'delivered') GROUP BY mc.cohort_month, date_trunc('month', o.placed_at), mc.user_id) SELECT co.cohort_month, co.order_month, (EXTRACT(YEAR FROM co.order_month) - EXTRACT(YEAR FROM co.cohort_month)) * 12 + EXTRACT(MONTH FROM co.order_month) - EXTRACT(MONTH FROM co.cohort_month) AS months_since_signup, COUNT(DISTINCT co.user_id) AS active_users, SUM(co.user_total) AS cohort_revenue, AVG(co.user_total) AS avg_revenue_per_user FROM cohort_orders co GROUP BY co.cohort_month, co.order_month ORDER BY co.cohort_month, co.order_month;

/* ===== DATE BUCKET AGGREGATION WITH GENERATE_SERIES ===== */

WITH date_buckets AS (SELECT bucket_start, bucket_start + INTERVAL '1 week' AS bucket_end FROM generate_series('2024-01-01'::timestamptz, '2024-12-31'::timestamptz, '1 week'::interval) bucket_start) SELECT db.bucket_start::date AS week_start, db.bucket_end::date AS week_end, COUNT(o.id) AS orders, COALESCE(SUM(o.total), 0) AS revenue, COUNT(DISTINCT o.user_id) AS unique_customers, COUNT(*) FILTER (WHERE o.status = 'delivered') AS delivered, COUNT(*) FILTER (WHERE o.status = 'cancelled') AS cancelled FROM date_buckets db LEFT JOIN orders o ON o.placed_at >= db.bucket_start AND o.placed_at < db.bucket_end GROUP BY db.bucket_start, db.bucket_end ORDER BY db.bucket_start;

/* ===== JSON DOCUMENT QUERY PATTERNS ===== */

SELECT o.id, o.status, (o.shipping_address->>'street') AS street, (o.shipping_address->>'city') AS city, (o.shipping_address->>'state') AS state, (o.shipping_address->>'zip') AS zip, (o.shipping_address->'coordinates'->>'lat')::numeric AS lat, (o.shipping_address->'coordinates'->>'lng')::numeric AS lng FROM orders o WHERE o.shipping_address @> '{"state": "CA"}'::jsonb AND (o.shipping_address->>'zip') LIKE '90%' AND jsonb_typeof(o.shipping_address->'coordinates') = 'object' ORDER BY o.placed_at DESC;

SELECT p.sku, kv.key AS attr_name, kv.value AS attr_value FROM products p CROSS JOIN LATERAL jsonb_each_text(p.attributes) kv WHERE p.is_active = true ORDER BY p.sku, kv.key;
