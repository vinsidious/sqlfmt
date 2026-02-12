-- ANSI SQL feature tour: exercises every formatting path of the SQL formatter.

/* DDL: table creation with diverse column types and constraints */

CREATE TABLE departments (department_id INTEGER NOT NULL PRIMARY KEY, department_name VARCHAR(100) NOT NULL UNIQUE, budget DECIMAL(15, 2) DEFAULT 0.00, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_active BOOLEAN DEFAULT TRUE, CHECK (budget >= 0));

CREATE TABLE employees (employee_id INTEGER NOT NULL PRIMARY KEY, first_name VARCHAR(50) NOT NULL, last_name VARCHAR(50) NOT NULL, email VARCHAR(255) NOT NULL UNIQUE, hire_date DATE NOT NULL, salary NUMERIC(10, 2) NOT NULL CHECK (salary > 0), commission_pct DECIMAL(5, 2) DEFAULT NULL, department_id INTEGER NOT NULL, manager_id INTEGER, job_title VARCHAR(100) DEFAULT 'Staff', CONSTRAINT fk_emp_dept FOREIGN KEY (department_id) REFERENCES departments (department_id) ON DELETE CASCADE ON UPDATE SET NULL, CONSTRAINT fk_emp_mgr FOREIGN KEY (manager_id) REFERENCES employees (employee_id) ON DELETE SET NULL);

CREATE TABLE orders (order_id INTEGER NOT NULL PRIMARY KEY, customer_id INTEGER NOT NULL, employee_id INTEGER, order_date DATE NOT NULL, shipped_date DATE, status VARCHAR(20) DEFAULT 'pending' NOT NULL, total_amount DECIMAL(12, 2) NOT NULL, notes VARCHAR(1000), CONSTRAINT fk_ord_emp FOREIGN KEY (employee_id) REFERENCES employees (employee_id) ON DELETE SET NULL ON UPDATE NO ACTION, CHECK (total_amount >= 0), CHECK (shipped_date IS NULL OR shipped_date >= order_date));

CREATE TABLE order_items (item_id INTEGER NOT NULL, order_id INTEGER NOT NULL, product_name VARCHAR(200) NOT NULL, quantity INTEGER NOT NULL DEFAULT 1, unit_price DECIMAL(10, 2) NOT NULL, discount DECIMAL(5, 2) DEFAULT 0.00, PRIMARY KEY (item_id, order_id), CONSTRAINT fk_item_order FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE CASCADE);

CREATE TABLE audit_log (log_id INTEGER NOT NULL PRIMARY KEY, table_name VARCHAR(128) NOT NULL, operation VARCHAR(10) NOT NULL, old_values VARCHAR(4000), new_values VARCHAR(4000), changed_by VARCHAR(100) NOT NULL, changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')));

/* DDL: views */

CREATE VIEW active_employees AS SELECT e.employee_id, e.first_name, e.last_name, e.email, e.salary, d.department_name FROM employees e INNER JOIN departments d ON e.department_id = d.department_id WHERE e.salary > 0;

CREATE OR REPLACE VIEW department_summary AS SELECT d.department_id, d.department_name, COUNT(e.employee_id) AS employee_count, COALESCE(SUM(e.salary), 0) AS total_salary, COALESCE(AVG(e.salary), 0) AS avg_salary, MIN(e.hire_date) AS earliest_hire, MAX(e.hire_date) AS latest_hire FROM departments d LEFT JOIN employees e ON d.department_id = e.department_id GROUP BY d.department_id, d.department_name;

/* DDL: indexes */

CREATE INDEX idx_emp_dept ON employees (department_id);

CREATE UNIQUE INDEX idx_emp_email ON employees (email);

CREATE INDEX idx_orders_date_status ON orders (order_date, status);

CREATE INDEX idx_orders_customer ON orders (customer_id) WHERE status <> 'cancelled';

/* DDL: alter table */

ALTER TABLE employees ADD COLUMN termination_date DATE;

ALTER TABLE employees ADD COLUMN phone VARCHAR(20);

ALTER TABLE employees DROP COLUMN phone;

ALTER TABLE orders ADD CONSTRAINT chk_status CHECK (status IN ('pending', 'shipped', 'delivered', 'cancelled', 'returned'));

/* DDL: drop statements */

DROP TABLE IF EXISTS audit_log;

DROP VIEW IF EXISTS department_summary;

-- DML: simple selects

SELECT employee_id, first_name, last_name, salary FROM employees WHERE department_id = 10 AND salary > 50000 ORDER BY last_name ASC, first_name ASC;

SELECT DISTINCT department_id FROM employees WHERE hire_date >= DATE '2020-01-01';

SELECT ALL e.first_name, e.last_name, e.salary * 12 AS annual_salary FROM employees e WHERE e.salary BETWEEN 40000 AND 80000 ORDER BY annual_salary DESC;

SELECT first_name, last_name FROM employees WHERE last_name LIKE 'Sm%' AND email IS NOT NULL;

SELECT first_name, last_name FROM employees WHERE commission_pct IS NULL AND department_id IN (10, 20, 30);

/* DML: aliases and expressions */

SELECT e.first_name AS "First Name", e.last_name AS "Last Name", e.salary + COALESCE(e.salary * e.commission_pct, 0) AS total_compensation, NULLIF(e.commission_pct, 0) AS effective_commission, CAST(e.hire_date AS VARCHAR(10)) AS hire_date_text, EXTRACT(YEAR FROM e.hire_date) AS hire_year, EXTRACT(MONTH FROM e.hire_date) AS hire_month FROM employees e WHERE EXTRACT(YEAR FROM e.hire_date) >= 2018;

-- DML: JOIN variations

SELECT e.first_name, e.last_name, d.department_name FROM employees e INNER JOIN departments d ON e.department_id = d.department_id WHERE d.is_active = TRUE;

SELECT e.first_name, e.last_name, d.department_name FROM employees e LEFT JOIN departments d ON e.department_id = d.department_id;

SELECT e.first_name, e.last_name, d.department_name FROM employees e RIGHT JOIN departments d ON e.department_id = d.department_id;

SELECT e.first_name, e.last_name, d.department_name FROM employees e FULL OUTER JOIN departments d ON e.department_id = d.department_id;

SELECT e.first_name, d.department_name FROM employees e CROSS JOIN departments d;

SELECT e.first_name, e.last_name, o.order_id, o.total_amount, oi.product_name, oi.quantity FROM employees e INNER JOIN orders o ON e.employee_id = o.employee_id LEFT JOIN order_items oi ON o.order_id = oi.order_id WHERE o.status = 'shipped' ORDER BY o.order_date DESC, oi.product_name ASC;

SELECT e.first_name, m.first_name AS manager_name FROM employees e LEFT JOIN employees m ON e.manager_id = m.employee_id;

/* DML: subqueries in WHERE, FROM, SELECT list */

SELECT first_name, last_name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees);

SELECT first_name, last_name FROM employees WHERE department_id IN (SELECT department_id FROM departments WHERE budget > 100000);

SELECT first_name, last_name FROM employees WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.employee_id = employees.employee_id);

SELECT first_name, last_name FROM employees WHERE EXISTS (SELECT 1 FROM orders WHERE orders.employee_id = employees.employee_id AND orders.total_amount > 10000);

SELECT e.first_name, e.last_name, dept_stats.avg_salary FROM employees e INNER JOIN (SELECT department_id, AVG(salary) AS avg_salary FROM employees GROUP BY department_id) dept_stats ON e.department_id = dept_stats.department_id WHERE e.salary > dept_stats.avg_salary;

SELECT e.first_name, e.last_name, (SELECT COUNT(*) FROM orders o WHERE o.employee_id = e.employee_id) AS order_count FROM employees e;

SELECT e.first_name, e.last_name, e.salary FROM employees e WHERE e.salary > ALL (SELECT salary FROM employees WHERE department_id = 20);

SELECT e.first_name, e.last_name, e.salary FROM employees e WHERE e.salary > ANY (SELECT salary FROM employees WHERE department_id = 30);

/* DML: common table expressions */

WITH high_earners AS (SELECT employee_id, first_name, last_name, salary, department_id FROM employees WHERE salary > 75000) SELECT h.first_name, h.last_name, h.salary, d.department_name FROM high_earners h INNER JOIN departments d ON h.department_id = d.department_id ORDER BY h.salary DESC;

WITH dept_totals AS (SELECT department_id, SUM(salary) AS total_salary, COUNT(*) AS emp_count FROM employees GROUP BY department_id), company_avg AS (SELECT AVG(total_salary) AS avg_dept_salary FROM dept_totals) SELECT d.department_name, dt.total_salary, dt.emp_count, ca.avg_dept_salary FROM dept_totals dt INNER JOIN departments d ON dt.department_id = d.department_id CROSS JOIN company_avg ca WHERE dt.total_salary > ca.avg_dept_salary;

-- Recursive CTE: organizational hierarchy
WITH RECURSIVE org_chart (employee_id, first_name, last_name, manager_id, level, path) AS (SELECT employee_id, first_name, last_name, manager_id, 1, CAST(first_name AS VARCHAR(1000)) FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.employee_id, e.first_name, e.last_name, e.manager_id, oc.level + 1, CAST(oc.path || ' > ' || e.first_name AS VARCHAR(1000)) FROM employees e INNER JOIN org_chart oc ON e.manager_id = oc.employee_id) SELECT employee_id, first_name, last_name, level, path FROM org_chart ORDER BY path;

/* DML: window functions */

SELECT employee_id, first_name, last_name, salary, department_id, ROW_NUMBER() OVER (ORDER BY salary DESC) AS salary_rank_overall, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dept_salary_rank, RANK() OVER (ORDER BY salary DESC) AS salary_rank, DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_dense_rank, NTILE(4) OVER (ORDER BY salary DESC) AS salary_quartile FROM employees;

SELECT employee_id, first_name, salary, department_id, LAG(salary, 1) OVER (PARTITION BY department_id ORDER BY hire_date) AS prev_salary, LEAD(salary, 1) OVER (PARTITION BY department_id ORDER BY hire_date) AS next_salary, FIRST_VALUE(first_name) OVER (PARTITION BY department_id ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS highest_paid_in_dept, LAST_VALUE(first_name) OVER (PARTITION BY department_id ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lowest_paid_in_dept, NTH_VALUE(first_name, 2) OVER (PARTITION BY department_id ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_highest FROM employees;

-- Running totals and moving averages
SELECT employee_id, hire_date, salary, SUM(salary) OVER (ORDER BY hire_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total, AVG(salary) OVER (ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3, COUNT(*) OVER (ORDER BY hire_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_count FROM employees ORDER BY hire_date;

-- WINDOW clause (named windows)
SELECT employee_id, first_name, salary, department_id, SUM(salary) OVER w AS dept_running_total, AVG(salary) OVER w AS dept_running_avg, COUNT(*) OVER w AS dept_running_count FROM employees WINDOW w AS (PARTITION BY department_id ORDER BY hire_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ORDER BY department_id, hire_date;

/* DML: aggregate functions and GROUP BY variations */

SELECT department_id, COUNT(*) AS emp_count, COUNT(DISTINCT job_title) AS distinct_titles, SUM(salary) AS total_salary, AVG(salary) AS avg_salary, MIN(salary) AS min_salary, MAX(salary) AS max_salary, MIN(hire_date) AS earliest_hire, MAX(hire_date) AS latest_hire FROM employees GROUP BY department_id HAVING COUNT(*) > 3 ORDER BY total_salary DESC;

SELECT department_id, job_title, COUNT(*) AS emp_count, SUM(salary) AS total_salary FROM employees GROUP BY ROLLUP (department_id, job_title);

SELECT department_id, job_title, COUNT(*) AS emp_count, SUM(salary) AS total_salary FROM employees GROUP BY CUBE (department_id, job_title);

SELECT department_id, job_title, COUNT(*) AS emp_count, SUM(salary) AS total_salary FROM employees GROUP BY GROUPING SETS ((department_id, job_title), (department_id), (job_title), ());

-- HAVING clause
SELECT department_id, AVG(salary) AS avg_salary FROM employees GROUP BY department_id HAVING AVG(salary) > (SELECT AVG(salary) FROM employees);

/* DML: CASE expressions */

SELECT employee_id, first_name, last_name, salary, CASE WHEN salary >= 100000 THEN 'Executive' WHEN salary >= 75000 THEN 'Senior' WHEN salary >= 50000 THEN 'Mid-Level' WHEN salary >= 30000 THEN 'Junior' ELSE 'Entry' END AS salary_band FROM employees;

SELECT employee_id, first_name, department_id, CASE department_id WHEN 10 THEN 'Engineering' WHEN 20 THEN 'Marketing' WHEN 30 THEN 'Sales' WHEN 40 THEN 'Finance' ELSE 'Other' END AS department_label FROM employees;

-- Pivoting with CASE
SELECT department_id, SUM(CASE WHEN EXTRACT(YEAR FROM hire_date) = 2020 THEN 1 ELSE 0 END) AS hires_2020, SUM(CASE WHEN EXTRACT(YEAR FROM hire_date) = 2021 THEN 1 ELSE 0 END) AS hires_2021, SUM(CASE WHEN EXTRACT(YEAR FROM hire_date) = 2022 THEN 1 ELSE 0 END) AS hires_2022, SUM(CASE WHEN EXTRACT(YEAR FROM hire_date) = 2023 THEN 1 ELSE 0 END) AS hires_2023 FROM employees GROUP BY department_id ORDER BY department_id;

/* DML: set operations */

SELECT first_name, last_name FROM employees WHERE department_id = 10 UNION SELECT first_name, last_name FROM employees WHERE salary > 80000;

SELECT first_name, last_name FROM employees WHERE department_id = 10 UNION ALL SELECT first_name, last_name FROM employees WHERE department_id = 20;

SELECT employee_id FROM employees WHERE department_id = 10 INTERSECT SELECT employee_id FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2023;

SELECT employee_id FROM employees WHERE department_id = 10 EXCEPT SELECT DISTINCT employee_id FROM orders WHERE employee_id IS NOT NULL;

(SELECT first_name, last_name, 'high' AS tier FROM employees WHERE salary > 90000) UNION ALL (SELECT first_name, last_name, 'mid' AS tier FROM employees WHERE salary BETWEEN 50000 AND 90000) UNION ALL (SELECT first_name, last_name, 'low' AS tier FROM employees WHERE salary < 50000) ORDER BY tier, last_name;

/* DML: FETCH FIRST and pagination */

SELECT employee_id, first_name, last_name, salary FROM employees ORDER BY salary DESC FETCH FIRST 10 ROWS ONLY;

SELECT employee_id, first_name, last_name, salary FROM employees ORDER BY salary DESC OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY;

SELECT employee_id, first_name, last_name, salary FROM employees ORDER BY salary DESC FETCH FIRST 5 ROWS WITH TIES;

/* DML: LISTAGG and ordered-set aggregate functions */

SELECT department_id, LISTAGG(first_name, ', ') WITHIN GROUP (ORDER BY last_name) AS employee_list FROM employees GROUP BY department_id;

SELECT department_id, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary_disc FROM employees GROUP BY department_id;

/* DML: COALESCE, NULLIF, CAST, EXTRACT */

SELECT employee_id, COALESCE(commission_pct, 0) AS commission, NULLIF(job_title, 'Staff') AS non_default_title, CAST(salary AS INTEGER) AS salary_int, CAST(hire_date AS TIMESTAMP) AS hire_timestamp, EXTRACT(DOW FROM hire_date) AS day_of_week, EXTRACT(QUARTER FROM hire_date) AS hire_quarter FROM employees;

-- DML: INSERT statements

INSERT INTO departments (department_id, department_name, budget) VALUES (50, 'Research', 500000.00);

INSERT INTO departments (department_id, department_name, budget, is_active) VALUES (60, 'Legal', 300000.00, TRUE), (70, 'Operations', 450000.00, TRUE), (80, 'Support', 200000.00, FALSE);

INSERT INTO audit_log (log_id, table_name, operation, new_values, changed_by) SELECT 1000 + ROW_NUMBER() OVER (ORDER BY employee_id), 'employees', 'INSERT', first_name || ' ' || last_name, 'system' FROM employees WHERE hire_date >= DATE '2023-01-01';

-- DML: UPDATE statements

UPDATE employees SET salary = salary * 1.05, termination_date = NULL WHERE department_id = 10 AND hire_date < DATE '2020-01-01';

UPDATE employees SET salary = dept_avg.avg_sal * 1.1 FROM (SELECT department_id, AVG(salary) AS avg_sal FROM employees GROUP BY department_id) dept_avg WHERE employees.department_id = dept_avg.department_id AND employees.salary < dept_avg.avg_sal;

-- DML: DELETE statements

DELETE FROM orders WHERE status = 'cancelled' AND order_date < DATE '2020-01-01';

DELETE FROM employees WHERE employee_id IN (SELECT e.employee_id FROM employees e LEFT JOIN orders o ON e.employee_id = o.employee_id WHERE o.order_id IS NULL AND e.termination_date IS NOT NULL);

/* DML: MERGE statements */

MERGE INTO employees tgt USING (SELECT 999 AS employee_id, 'Jane' AS first_name, 'Doe' AS last_name, 'jane.doe@example.com' AS email, DATE '2024-01-15' AS hire_date, 65000.00 AS salary, 10 AS department_id) src ON tgt.employee_id = src.employee_id WHEN MATCHED THEN UPDATE SET salary = src.salary, email = src.email WHEN NOT MATCHED THEN INSERT (employee_id, first_name, last_name, email, hire_date, salary, department_id) VALUES (src.employee_id, src.first_name, src.last_name, src.email, src.hire_date, src.salary, src.department_id);

MERGE INTO audit_log tgt USING (SELECT log_id, table_name, operation, old_values, new_values, changed_by FROM audit_log WHERE changed_at < CURRENT_TIMESTAMP) src ON tgt.log_id = src.log_id WHEN MATCHED AND src.operation = 'DELETE' THEN DELETE WHEN MATCHED THEN UPDATE SET old_values = src.old_values, new_values = src.new_values WHEN NOT MATCHED THEN INSERT (log_id, table_name, operation, new_values, changed_by) VALUES (src.log_id, src.table_name, src.operation, src.new_values, src.changed_by);

/* Real-world patterns: gap-and-island detection */

WITH numbered AS (SELECT employee_id, department_id, hire_date, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY hire_date) AS rn, hire_date - CAST(ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY hire_date) AS INTEGER) AS grp FROM employees), islands AS (SELECT department_id, MIN(hire_date) AS island_start, MAX(hire_date) AS island_end, COUNT(*) AS consecutive_hires FROM numbered GROUP BY department_id, grp) SELECT department_id, island_start, island_end, consecutive_hires FROM islands WHERE consecutive_hires >= 3 ORDER BY department_id, island_start;

/* Real-world pattern: date bucketing */

SELECT CASE WHEN EXTRACT(MONTH FROM order_date) BETWEEN 1 AND 3 THEN 'Q1' WHEN EXTRACT(MONTH FROM order_date) BETWEEN 4 AND 6 THEN 'Q2' WHEN EXTRACT(MONTH FROM order_date) BETWEEN 7 AND 9 THEN 'Q3' ELSE 'Q4' END AS quarter, EXTRACT(YEAR FROM order_date) AS order_year, COUNT(*) AS order_count, SUM(total_amount) AS quarterly_revenue, AVG(total_amount) AS avg_order_value FROM orders GROUP BY EXTRACT(YEAR FROM order_date), CASE WHEN EXTRACT(MONTH FROM order_date) BETWEEN 1 AND 3 THEN 'Q1' WHEN EXTRACT(MONTH FROM order_date) BETWEEN 4 AND 6 THEN 'Q2' WHEN EXTRACT(MONTH FROM order_date) BETWEEN 7 AND 9 THEN 'Q3' ELSE 'Q4' END ORDER BY order_year, quarter;

/* Real-world pattern: top-N per group */

WITH ranked_employees AS (SELECT employee_id, first_name, last_name, salary, department_id, DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dept_rank FROM employees) SELECT re.employee_id, re.first_name, re.last_name, re.salary, d.department_name, re.dept_rank FROM ranked_employees re INNER JOIN departments d ON re.department_id = d.department_id WHERE re.dept_rank <= 3 ORDER BY d.department_name, re.dept_rank;

/* Real-world pattern: cumulative distribution */

SELECT employee_id, first_name, salary, department_id, ROUND(CAST(RANK() OVER (ORDER BY salary) AS DECIMAL(10, 4)) / CAST(COUNT(*) OVER () AS DECIMAL(10, 4)), 4) AS percentile, SUM(salary) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary, SUM(salary) OVER () AS grand_total FROM employees ORDER BY salary;

/* Complex multi-join with mixed conditions */

SELECT e.first_name, e.last_name, d.department_name, m.first_name AS manager_first, m.last_name AS manager_last, o.order_id, o.order_date, o.total_amount, oi.product_name, oi.quantity, oi.unit_price, oi.quantity * oi.unit_price * (1 - COALESCE(oi.discount, 0)) AS line_total FROM employees e INNER JOIN departments d ON e.department_id = d.department_id LEFT JOIN employees m ON e.manager_id = m.employee_id LEFT JOIN orders o ON e.employee_id = o.employee_id AND o.status IN ('shipped', 'delivered') LEFT JOIN order_items oi ON o.order_id = oi.order_id WHERE d.is_active = TRUE AND (e.termination_date IS NULL OR e.termination_date > CURRENT_DATE) ORDER BY d.department_name, e.last_name, o.order_date DESC;

/* Correlated subquery with multiple conditions */

SELECT e.first_name, e.last_name, e.salary, e.department_id FROM employees e WHERE e.salary = (SELECT MAX(e2.salary) FROM employees e2 WHERE e2.department_id = e.department_id) AND e.department_id IN (SELECT d.department_id FROM departments d WHERE d.budget > 200000 AND d.is_active = TRUE);

/* Deeply nested boolean expressions */

SELECT employee_id, first_name, last_name FROM employees WHERE (department_id = 10 OR department_id = 20) AND (salary > 50000 OR (commission_pct IS NOT NULL AND commission_pct > 0.1)) AND hire_date BETWEEN DATE '2018-01-01' AND DATE '2023-12-31' AND NOT (job_title = 'Intern' OR job_title = 'Contractor') AND employee_id NOT IN (SELECT employee_id FROM orders WHERE status = 'cancelled');

/* CTE with INSERT */

WITH new_employees AS (SELECT first_name, last_name, email, department_id, salary FROM employees WHERE hire_date >= DATE '2024-01-01') INSERT INTO audit_log (log_id, table_name, operation, new_values, changed_by) SELECT ROW_NUMBER() OVER (ORDER BY first_name), 'employees', 'REVIEW', first_name || ' ' || last_name || ' (' || CAST(salary AS VARCHAR(20)) || ')', 'audit_system' FROM new_employees;

/* Multiple CASE inside aggregate */

SELECT department_id, COUNT(*) AS total, SUM(CASE WHEN salary >= 80000 THEN 1 ELSE 0 END) AS senior_count, SUM(CASE WHEN salary < 80000 AND salary >= 50000 THEN 1 ELSE 0 END) AS mid_count, SUM(CASE WHEN salary < 50000 THEN 1 ELSE 0 END) AS junior_count, CAST(SUM(CASE WHEN commission_pct IS NOT NULL THEN 1 ELSE 0 END) AS DECIMAL(5, 2)) / CAST(COUNT(*) AS DECIMAL(5, 2)) AS commission_pct_ratio FROM employees GROUP BY department_id HAVING COUNT(*) >= 2 ORDER BY department_id;

/* INTERSECT ALL and complex set operation */

SELECT employee_id, department_id FROM employees WHERE salary > 60000 INTERSECT ALL SELECT employee_id, department_id FROM employees WHERE hire_date < DATE '2021-01-01';

/* NOT IN with subquery */

SELECT first_name, last_name, department_id FROM employees WHERE department_id NOT IN (SELECT department_id FROM departments WHERE budget < 150000);

/* BETWEEN with expressions */

SELECT order_id, customer_id, total_amount, order_date FROM orders WHERE total_amount NOT BETWEEN 100 AND 500 AND order_date BETWEEN DATE '2023-01-01' AND CURRENT_DATE;

/* LIKE with ESCAPE */

SELECT employee_id, email FROM employees WHERE email LIKE '%\_%' ESCAPE '\';

/* IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE */

SELECT department_id, department_name, is_active FROM departments WHERE is_active IS TRUE;

SELECT department_id, department_name, is_active FROM departments WHERE is_active IS NOT FALSE;

/* Complex window frame specifications */

SELECT employee_id, hire_date, salary, AVG(salary) OVER (ORDER BY hire_date ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS windowed_avg, SUM(salary) OVER (ORDER BY hire_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_sum, MAX(salary) OVER (PARTITION BY department_id ORDER BY hire_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS future_max FROM employees;

/* Recursive CTE: generating a number series */

WITH RECURSIVE numbers (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 100) SELECT n FROM numbers;

/* Multi-level CTE with window functions */

WITH monthly_orders AS (SELECT EXTRACT(YEAR FROM order_date) AS yr, EXTRACT(MONTH FROM order_date) AS mo, COUNT(*) AS order_count, SUM(total_amount) AS revenue FROM orders WHERE status <> 'cancelled' GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)), with_growth AS (SELECT yr, mo, order_count, revenue, LAG(revenue) OVER (ORDER BY yr, mo) AS prev_revenue, CASE WHEN LAG(revenue) OVER (ORDER BY yr, mo) > 0 THEN (revenue - LAG(revenue) OVER (ORDER BY yr, mo)) / LAG(revenue) OVER (ORDER BY yr, mo) ELSE NULL END AS growth_rate FROM monthly_orders) SELECT yr, mo, order_count, revenue, prev_revenue, ROUND(CAST(growth_rate * 100 AS DECIMAL(10, 2)), 2) AS growth_pct FROM with_growth ORDER BY yr, mo;

/* POSITION, SUBSTRING, TRIM, OVERLAY */

SELECT employee_id, email, POSITION('@' IN email) AS at_position, SUBSTRING(email FROM 1 FOR POSITION('@' IN email) - 1) AS username, TRIM(BOTH ' ' FROM first_name) AS trimmed_name, TRIM(LEADING '0' FROM CAST(employee_id AS VARCHAR(10))) AS trimmed_id FROM employees WHERE POSITION('@' IN email) > 0;

/* DISTINCT ON emulated via ROW_NUMBER */

WITH ranked AS (SELECT employee_id, first_name, last_name, department_id, salary, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rn FROM employees) SELECT employee_id, first_name, last_name, department_id, salary FROM ranked WHERE rn = 1;

/* Multiple named windows */

SELECT employee_id, first_name, salary, department_id, ROW_NUMBER() OVER dept_ordered AS dept_row, SUM(salary) OVER dept_ordered AS dept_running_sum, RANK() OVER company_wide AS company_rank, DENSE_RANK() OVER company_wide AS company_dense_rank FROM employees WINDOW dept_ordered AS (PARTITION BY department_id ORDER BY salary DESC), company_wide AS (ORDER BY salary DESC);

/* EXISTS with correlated subquery and OR */

SELECT d.department_id, d.department_name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department_id = d.department_id AND e.salary > 90000) OR d.budget > 500000;

/* GRANT and REVOKE */

GRANT SELECT, INSERT, UPDATE ON employees TO hr_role;

GRANT ALL ON departments TO admin_role WITH GRANT OPTION;

REVOKE DELETE ON orders FROM readonly_role;

/* TRUNCATE */

TRUNCATE TABLE audit_log;

/* Comprehensive final query: many features combined */

WITH RECURSIVE management_chain (employee_id, manager_id, chain_length, root_manager) AS (SELECT employee_id, manager_id, 0, employee_id FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.employee_id, e.manager_id, mc.chain_length + 1, mc.root_manager FROM employees e INNER JOIN management_chain mc ON e.manager_id = mc.employee_id WHERE mc.chain_length < 10), employee_metrics AS (SELECT e.employee_id, e.first_name, e.last_name, e.salary, e.department_id, e.hire_date, COALESCE(e.commission_pct, 0) AS commission_pct, mc.chain_length AS reporting_depth, ROW_NUMBER() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS dept_rank, PERCENT_RANK() OVER (ORDER BY e.salary) AS salary_percentile, SUM(e.salary) OVER (PARTITION BY e.department_id) AS dept_total_salary, COUNT(*) OVER (PARTITION BY e.department_id) AS dept_size FROM employees e LEFT JOIN management_chain mc ON e.employee_id = mc.employee_id) SELECT em.first_name, em.last_name, em.salary, d.department_name, em.dept_rank, ROUND(CAST(em.salary_percentile * 100 AS DECIMAL(5, 2)), 2) AS salary_pctl, em.reporting_depth, em.dept_total_salary, em.dept_size, CASE WHEN em.dept_rank = 1 THEN 'Top Earner' WHEN em.dept_rank <= 3 THEN 'Top 3' WHEN CAST(em.dept_rank AS DECIMAL(5, 2)) / CAST(em.dept_size AS DECIMAL(5, 2)) <= 0.25 THEN 'Top Quartile' ELSE 'Standard' END AS compensation_tier, COALESCE((SELECT COUNT(*) FROM orders o WHERE o.employee_id = em.employee_id AND o.status = 'delivered'), 0) AS delivered_orders FROM employee_metrics em INNER JOIN departments d ON em.department_id = d.department_id WHERE em.salary > 30000 AND d.is_active IS TRUE AND em.hire_date BETWEEN DATE '2015-01-01' AND CURRENT_DATE ORDER BY d.department_name ASC, em.salary DESC FETCH FIRST 50 ROWS ONLY;
