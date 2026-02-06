import { describe, it, expect } from 'bun:test';
import { formatSQL } from '../src/format';

function assertFormat(name: string, input: string, expected: string) {
  it(name, () => {
    const result = formatSQL(input).trimEnd();
    const exp = expected.trimEnd();
    expect(result).toBe(exp);
  });
}

describe('Category 1: Basic SELECT Queries', () => {
  assertFormat('1.1 — Simple single-table SELECT',
    `select file_hash from file_system where file_name = '.vimrc';`,
    `SELECT file_hash
  FROM file_system
 WHERE file_name = '.vimrc';`
  );

  assertFormat('1.2 — Multiple columns, single table',
    `select a.title, a.release_date, a.recording_date from albums as a where a.title = 'Charcoal Lane' or a.title = 'The New Danger';`,
    `SELECT a.title, a.release_date, a.recording_date
  FROM albums AS a
 WHERE a.title = 'Charcoal Lane'
    OR a.title = 'The New Danger';`
  );

  assertFormat('1.3 — Column list wrapping with logical grouping',
    `select a.title, a.release_date, a.recording_date, a.production_date from albums as a where a.title = 'Charcoal Lane' or a.title = 'The New Danger';`,
    `SELECT a.title,
       a.release_date, a.recording_date, a.production_date
  FROM albums AS a
 WHERE a.title = 'Charcoal Lane'
    OR a.title = 'The New Danger';`
  );

  assertFormat('1.4 — Simple SELECT with alias',
    `select first_name as fn from staff;`,
    `SELECT first_name AS fn
  FROM staff;`
  );

  assertFormat('1.5 — Aggregate with alias',
    `select sum(s.monitor_tally) as monitor_total from staff as s;`,
    `SELECT SUM(s.monitor_tally) AS monitor_total
  FROM staff AS s;`
  );

  assertFormat('1.6 — WHERE with AND',
    `select model_num from phones as p where p.release_date > '2014-09-30' and p.manufacturer = 'Apple';`,
    `SELECT model_num
  FROM phones AS p
 WHERE p.release_date > '2014-09-30'
   AND p.manufacturer = 'Apple';`
  );

  assertFormat('1.7 — SELECT with no WHERE clause',
    `select first_name from staff;`,
    `SELECT first_name
  FROM staff;`
  );
});

describe('Category 2: JOINs', () => {
  assertFormat('2.1 — Single INNER JOIN',
    `select r.last_name from riders as r inner join bikes as b on r.bike_vin_num = b.vin_num and b.engine_tally > 2 inner join crew as c on r.crew_chief_last_name = c.last_name and c.chief = 'Y';`,
    `SELECT r.last_name
  FROM riders AS r
       INNER JOIN bikes AS b
       ON r.bike_vin_num = b.vin_num
          AND b.engine_tally > 2

       INNER JOIN crew AS c
       ON r.crew_chief_last_name = c.last_name
          AND c.chief = 'Y';`
  );

  assertFormat('2.2 — Plain JOIN (no INNER keyword)',
    `select r.last_name from riders as r join bikes as b on r.bike_vin_num = b.vin_num;`,
    `SELECT r.last_name
  FROM riders AS r
  JOIN bikes AS b
    ON r.bike_vin_num = b.vin_num;`
  );

  assertFormat('2.3 — LEFT OUTER JOIN',
    `select e.employee_name, d.department_name from employees as e left outer join departments as d on e.department_id = d.department_id;`,
    `SELECT e.employee_name, d.department_name
  FROM employees AS e
       LEFT OUTER JOIN departments AS d
       ON e.department_id = d.department_id;`
  );

  assertFormat('2.4 — Multiple mixed JOINs',
    `select o.order_id, c.customer_name, p.product_name, s.shipper_name from orders as o inner join customers as c on o.customer_id = c.customer_id left join order_details as od on o.order_id = od.order_id inner join products as p on od.product_id = p.product_id left outer join shippers as s on o.shipper_id = s.shipper_id where o.order_date > '2023-01-01' and c.country = 'USA';`,
    `SELECT o.order_id, c.customer_name, p.product_name, s.shipper_name
  FROM orders AS o
       INNER JOIN customers AS c
       ON o.customer_id = c.customer_id

       LEFT JOIN order_details AS od
       ON o.order_id = od.order_id

       INNER JOIN products AS p
       ON od.product_id = p.product_id

       LEFT OUTER JOIN shippers AS s
       ON o.shipper_id = s.shipper_id
 WHERE o.order_date > '2023-01-01'
   AND c.country = 'USA';`
  );

  assertFormat('2.5 — CROSS JOIN',
    `select a.name, b.name from table_a as a cross join table_b as b;`,
    `SELECT a.name, b.name
  FROM table_a AS a
       CROSS JOIN table_b AS b;`
  );
});

describe('Category 3: Subqueries', () => {
  assertFormat('3.1 — Subquery in WHERE with IN',
    `select r.last_name, (select max(year(championship_date)) from champions as c where c.last_name = r.last_name and c.confirmed = 'Y') as last_championship_year from riders as r where r.last_name in (select c.last_name from champions as c where year(championship_date) > '2008' and c.confirmed = 'Y');`,
    `SELECT r.last_name,
       (SELECT MAX(YEAR(championship_date))
          FROM champions AS c
         WHERE c.last_name = r.last_name
           AND c.confirmed = 'Y') AS last_championship_year
  FROM riders AS r
 WHERE r.last_name IN
       (SELECT c.last_name
          FROM champions AS c
         WHERE YEAR(championship_date) > '2008'
           AND c.confirmed = 'Y');`
  );

  assertFormat('3.2 — Scalar subquery in SELECT list',
    `select e.employee_name, (select count(*) from orders as o where o.employee_id = e.employee_id) as order_count from employees as e;`,
    `SELECT e.employee_name,
       (SELECT COUNT(*)
          FROM orders AS o
         WHERE o.employee_id = e.employee_id) AS order_count
  FROM employees AS e;`
  );

  assertFormat('3.3 — Subquery in FROM (derived table)',
    `select dept_name, avg_salary from (select d.department_name as dept_name, avg(e.salary) as avg_salary from employees as e inner join departments as d on e.department_id = d.department_id group by d.department_name) as dept_stats where avg_salary > 50000;`,
    `SELECT dept_name, avg_salary
  FROM (SELECT d.department_name AS dept_name,
               AVG(e.salary) AS avg_salary
          FROM employees AS e
               INNER JOIN departments AS d
               ON e.department_id = d.department_id
         GROUP BY d.department_name) AS dept_stats
 WHERE avg_salary > 50000;`
  );

  assertFormat('3.4 — EXISTS subquery',
    `select c.customer_name from customers as c where exists (select 1 from orders as o where o.customer_id = c.customer_id and o.order_date > '2024-01-01');`,
    `SELECT c.customer_name
  FROM customers AS c
 WHERE EXISTS
       (SELECT 1
          FROM orders AS o
         WHERE o.customer_id = c.customer_id
           AND o.order_date > '2024-01-01');`
  );
});

describe('Category 4: CASE Expressions', () => {
  assertFormat('4.1 — Simple CASE',
    `select case postcode when 'BN1' then 'Brighton' when 'EH1' then 'Edinburgh' end as city from office_locations where country = 'United Kingdom' and opening_time between 8 and 9 and postcode in ('EH1', 'BN1', 'NN1', 'KW1');`,
    `SELECT CASE postcode
       WHEN 'BN1' THEN 'Brighton'
       WHEN 'EH1' THEN 'Edinburgh'
       END AS city
  FROM office_locations
 WHERE country = 'United Kingdom'
   AND opening_time BETWEEN 8 AND 9
   AND postcode IN ('EH1', 'BN1', 'NN1', 'KW1');`
  );

  assertFormat('4.2 — Searched CASE with ELSE',
    `select employee_name, case when salary > 100000 then 'Senior' when salary > 50000 then 'Mid' else 'Junior' end as level from employees;`,
    `SELECT employee_name,
       CASE
       WHEN salary > 100000 THEN 'Senior'
       WHEN salary > 50000 THEN 'Mid'
       ELSE 'Junior'
       END AS level
  FROM employees;`
  );

  assertFormat('4.3 — Nested CASE',
    `select product_name, case category when 'Electronics' then case when price > 1000 then 'Premium' else 'Standard' end when 'Books' then 'Literature' else 'Other' end as classification from products;`,
    `SELECT product_name,
       CASE category
       WHEN 'Electronics' THEN CASE
                               WHEN price > 1000 THEN 'Premium'
                               ELSE 'Standard'
                               END
       WHEN 'Books' THEN 'Literature'
       ELSE 'Other'
       END AS classification
  FROM products;`
  );
});

describe('Category 5: INSERT / UPDATE / DELETE', () => {
  assertFormat('5.1 — Simple INSERT',
    `insert into albums (title, release_date, recording_date) values ('Charcoal Lane', '1990-01-01 01:01:01.00000', '1990-01-01 01:01:01.00000'), ('The New Danger', '2008-01-01 01:01:01.00000', '1990-01-01 01:01:01.00000');`,
    `INSERT INTO albums (title, release_date, recording_date)
VALUES ('Charcoal Lane', '1990-01-01 01:01:01.00000', '1990-01-01 01:01:01.00000'),
       ('The New Danger', '2008-01-01 01:01:01.00000', '1990-01-01 01:01:01.00000');`
  );

  assertFormat('5.2 — UPDATE with multiple SET',
    `update file_system set file_modified_date = '1980-02-22 13:19:01.00000', file_size = 209732 where file_name = '.vimrc';`,
    `UPDATE file_system
   SET file_modified_date = '1980-02-22 13:19:01.00000',
       file_size = 209732
 WHERE file_name = '.vimrc';`
  );

  assertFormat('5.3 — Simple DELETE',
    `delete from albums where title = 'The New Danger';`,
    `DELETE
  FROM albums
 WHERE title = 'The New Danger';`
  );

  assertFormat('5.4 — INSERT with SELECT',
    `insert into archive_albums (title, release_date) select title, release_date from albums where release_date < '2000-01-01';`,
    `INSERT INTO archive_albums (title, release_date)
SELECT title, release_date
  FROM albums
 WHERE release_date < '2000-01-01';`
  );
});

describe('Category 6: UNION / INTERSECT / EXCEPT', () => {
  assertFormat('6.1 — UNION ALL with parentheses',
    `(select f.species_name, avg(f.height) as average_height, avg(f.diameter) as average_diameter from flora as f where f.species_name = 'Banksia' or f.species_name = 'Sheoak' or f.species_name = 'Wattle' group by f.species_name, f.observation_date) union all (select b.species_name, avg(b.height) as average_height, avg(b.diameter) as average_diameter from botanic_garden_flora as b where b.species_name = 'Banksia' or b.species_name = 'Sheoak' or b.species_name = 'Wattle' group by b.species_name, b.observation_date);`,
    `(SELECT f.species_name,
        AVG(f.height) AS average_height, AVG(f.diameter) AS average_diameter
   FROM flora AS f
  WHERE f.species_name = 'Banksia'
     OR f.species_name = 'Sheoak'
     OR f.species_name = 'Wattle'
  GROUP BY f.species_name, f.observation_date)

  UNION ALL

(SELECT b.species_name,
        AVG(b.height) AS average_height, AVG(b.diameter) AS average_diameter
   FROM botanic_garden_flora AS b
  WHERE b.species_name = 'Banksia'
     OR b.species_name = 'Sheoak'
     OR b.species_name = 'Wattle'
  GROUP BY b.species_name, b.observation_date);`
  );

  assertFormat('6.2 — Simple UNION (no parens in input)',
    `select name, email from customers where country = 'US' union select name, email from partners where country = 'US';`,
    `SELECT name, email
  FROM customers
 WHERE country = 'US'

 UNION

SELECT name, email
  FROM partners
 WHERE country = 'US';`
  );

  assertFormat('6.3 — EXCEPT',
    `select employee_id from all_staff except select employee_id from terminated_staff;`,
    `SELECT employee_id
  FROM all_staff

EXCEPT

SELECT employee_id
  FROM terminated_staff;`
  );
});

describe('Category 7: CREATE TABLE', () => {
  assertFormat('7.1 — Full CREATE TABLE with constraints',
    `create table staff (primary key (staff_num), staff_num int(5) not null, first_name varchar(100) not null, pens_in_drawer int(2) not null, constraint pens_in_drawer_range check(pens_in_drawer between 1 and 99));`,
    `CREATE TABLE staff (
    PRIMARY KEY (staff_num),
    staff_num      INT(5)       NOT NULL,
    first_name     VARCHAR(100) NOT NULL,
    pens_in_drawer INT(2)       NOT NULL,
                   CONSTRAINT pens_in_drawer_range
                   CHECK(pens_in_drawer BETWEEN 1 AND 99)
);`
  );

  assertFormat('7.2 — CREATE TABLE with FOREIGN KEY',
    `create table order_items (primary key (item_id), item_id int not null, order_id int not null, product_id int not null, quantity int default 1 not null, constraint fk_order foreign key (order_id) references orders (order_id) on delete cascade on update cascade, constraint fk_product foreign key (product_id) references products (product_id));`,
    `CREATE TABLE order_items (
    PRIMARY KEY (item_id),
    item_id    INT NOT NULL,
    order_id   INT NOT NULL,
    product_id INT NOT NULL,
    quantity   INT DEFAULT 1 NOT NULL,
    CONSTRAINT fk_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT fk_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id)
);`
  );
});

describe('Category 8: CTEs (WITH clause)', () => {
  assertFormat('8.1 — Single CTE',
    `with regional_sales as (select region, sum(amount) as total_sales from orders group by region) select region, total_sales from regional_sales where total_sales > 1000000;`,
    `  WITH regional_sales AS (
           SELECT region, SUM(amount) AS total_sales
             FROM orders
            GROUP BY region
       )
SELECT region, total_sales
  FROM regional_sales
 WHERE total_sales > 1000000;`
  );

  assertFormat('8.2 — Multiple CTEs',
    `with regional_sales as (select region, sum(amount) as total_sales from orders group by region), top_regions as (select region from regional_sales where total_sales > (select sum(total_sales) / 10 from regional_sales)) select region, product, sum(quantity) as product_units, sum(amount) as product_sales from orders where region in (select region from top_regions) group by region, product;`,
    `  WITH regional_sales AS (
           SELECT region, SUM(amount) AS total_sales
             FROM orders
            GROUP BY region
       ),
       top_regions AS (
           SELECT region
             FROM regional_sales
            WHERE total_sales > (SELECT SUM(total_sales) / 10
                                   FROM regional_sales)
       )
SELECT region,
       product,
       SUM(quantity) AS product_units,
       SUM(amount) AS product_sales
  FROM orders
 WHERE region IN (SELECT region
                    FROM top_regions)
 GROUP BY region, product;`
  );
});

describe('Category 9: Window Functions', () => {
  assertFormat('9.1 — ROW_NUMBER with PARTITION BY',
    `select employee_name, department, salary, row_number() over (partition by department order by salary desc) as rank_num from employees;`,
    `SELECT employee_name,
       department,
       salary,
       ROW_NUMBER() OVER (PARTITION BY department
                              ORDER BY salary DESC) AS rank_num
  FROM employees;`
  );

  assertFormat('9.2 — Multiple window functions',
    `select employee_name, salary, avg(salary) over (partition by department) as dept_avg, rank() over (order by salary desc) as salary_rank, sum(salary) over (order by hire_date rows between unbounded preceding and current row) as running_total from employees;`,
    `SELECT employee_name,
       salary,
       AVG(salary) OVER (PARTITION BY department) AS dept_avg,
       RANK() OVER (ORDER BY salary DESC) AS salary_rank,
       SUM(salary) OVER (ORDER BY hire_date
                         ROWS BETWEEN UNBOUNDED PRECEDING
                                  AND CURRENT ROW) AS running_total
  FROM employees;`
  );
});

describe('Category 10: Comments', () => {
  assertFormat('10.1 — Inline comment',
    `select file_hash -- stored ssdeep hash
from file_system where file_name = '.vimrc';`,
    `SELECT file_hash  -- stored ssdeep hash
  FROM file_system
 WHERE file_name = '.vimrc';`
  );

  assertFormat('10.2 — Block comment before statement',
    `/* Updating the file record after writing to the file */
update file_system set file_modified_date = '1980-02-22 13:19:01.00000', file_size = 209732 where file_name = '.vimrc';`,
    `/* Updating the file record after writing to the file */
UPDATE file_system
   SET file_modified_date = '1980-02-22 13:19:01.00000',
       file_size = 209732
 WHERE file_name = '.vimrc';`
  );

  assertFormat('10.3 — Multiple inline comments',
    `select a.title, -- the album name
a.release_date, a.recording_date, a.production_date -- grouped dates together
from albums as a where a.title = 'Charcoal Lane' or a.title = 'The New Danger';`,
    `SELECT a.title,  -- the album name
       a.release_date, a.recording_date, a.production_date  -- grouped dates together
  FROM albums AS a
 WHERE a.title = 'Charcoal Lane'
    OR a.title = 'The New Danger';`
  );
});

describe('Category 11: GROUP BY and HAVING', () => {
  assertFormat('11.1 — GROUP BY with HAVING',
    `select department, count(*) as employee_count, avg(salary) as avg_salary from employees group by department having count(*) > 5 and avg(salary) > 60000 order by avg_salary desc;`,
    `SELECT department,
       COUNT(*) AS employee_count,
       AVG(salary) AS avg_salary
  FROM employees
 GROUP BY department
HAVING COUNT(*) > 5
   AND AVG(salary) > 60000
 ORDER BY avg_salary DESC;`
  );

  assertFormat('11.2 — GROUP BY multiple columns',
    `select region, department, sum(revenue) as total_revenue from sales group by region, department order by region, total_revenue desc;`,
    `SELECT region, department, SUM(revenue) AS total_revenue
  FROM sales
 GROUP BY region, department
 ORDER BY region, total_revenue DESC;`
  );
});

describe('Category 12: Edge Cases and Stress Tests', () => {
  assertFormat('12.1 — Already formatted (idempotency)',
    `SELECT file_hash
  FROM file_system
 WHERE file_name = '.vimrc';`,
    `SELECT file_hash
  FROM file_system
 WHERE file_name = '.vimrc';`
  );

  assertFormat('12.2 — Everything on one line, very long',
    `select e.employee_id, e.first_name, e.last_name, d.department_name, j.job_title, l.city, l.state_province, l.country_id from employees as e inner join departments as d on e.department_id = d.department_id inner join jobs as j on e.job_id = j.job_id inner join locations as l on d.location_id = l.location_id where e.salary > 50000 and d.department_name in ('Sales', 'Marketing', 'Engineering') and l.country_id = 'US' order by e.last_name, e.first_name;`,
    `SELECT e.employee_id,
       e.first_name,
       e.last_name,
       d.department_name,
       j.job_title,
       l.city,
       l.state_province,
       l.country_id
  FROM employees AS e
       INNER JOIN departments AS d
       ON e.department_id = d.department_id

       INNER JOIN jobs AS j
       ON e.job_id = j.job_id

       INNER JOIN locations AS l
       ON d.location_id = l.location_id
 WHERE e.salary > 50000
   AND d.department_name IN ('Sales', 'Marketing', 'Engineering')
   AND l.country_id = 'US'
 ORDER BY e.last_name, e.first_name;`
  );

  assertFormat('12.3 — Multiple statements',
    `select 1; select 2;`,
    `SELECT 1;

SELECT 2;`
  );

  assertFormat('12.4 — Empty/whitespace input',
    `   `,
    ``
  );

  assertFormat('12.5 — Deeply nested subqueries',
    `select a.name from accounts as a where a.balance > (select avg(b.balance) from accounts as b where b.region = (select c.region from offices as c where c.office_id = a.office_id));`,
    `SELECT a.name
  FROM accounts AS a
 WHERE a.balance >
       (SELECT AVG(b.balance)
          FROM accounts AS b
         WHERE b.region =
               (SELECT c.region
                  FROM offices AS c
                 WHERE c.office_id = a.office_id));`
  );

  assertFormat('12.6 — Mixed case keywords in input',
    `Select First_Name As FN From Staff Where Department = 'Sales' Order By First_Name Asc;`,
    `SELECT first_name AS fn
  FROM staff
 WHERE department = 'Sales'
 ORDER BY first_name ASC;`
  );

  assertFormat('12.7 — String literals containing keywords',
    `select message from logs where message like '%SELECT FROM%' and level = 'ERROR';`,
    `SELECT message
  FROM logs
 WHERE message LIKE '%SELECT FROM%'
   AND level = 'ERROR';`
  );

  assertFormat('12.8 — Quoted identifiers',
    `select "Order ID", "Customer Name" from "Order Table" where "Order Date" > '2024-01-01';`,
    `SELECT "Order ID", "Customer Name"
  FROM "Order Table"
 WHERE "Order Date" > '2024-01-01';`
  );

  assertFormat('12.9 — DISTINCT keyword',
    `select distinct department, city from employees where country = 'US' order by department;`,
    `SELECT DISTINCT department, city
  FROM employees
 WHERE country = 'US'
 ORDER BY department;`
  );

  assertFormat('12.10 — SELECT with LIMIT and OFFSET',
    `select employee_name, salary from employees order by salary desc limit 10 offset 20;`,
    `SELECT employee_name, salary
  FROM employees
 ORDER BY salary DESC
 LIMIT 10
OFFSET 20;`
  );

  assertFormat('12.11 — COALESCE and NULLIF',
    `select coalesce(preferred_name, first_name) as display_name, nullif(middle_name, '') as middle_name from staff;`,
    `SELECT COALESCE(preferred_name, first_name) AS display_name,
       NULLIF(middle_name, '') AS middle_name
  FROM staff;`
  );

  assertFormat('12.12 — CAST expression',
    `select cast(order_date as date) as order_day, cast(amount as decimal(10, 2)) as formatted_amount from orders;`,
    `SELECT CAST(order_date AS DATE) AS order_day,
       CAST(amount AS DECIMAL(10, 2)) AS formatted_amount
  FROM orders;`
  );

  assertFormat('12.13 — Arithmetic expressions',
    `select product_name, price * quantity as line_total, (price * quantity) - discount as net_total from order_items where (price * quantity) > 100;`,
    `SELECT product_name,
       price * quantity AS line_total,
       (price * quantity) - discount AS net_total
  FROM order_items
 WHERE (price * quantity) > 100;`
  );

  assertFormat('12.14 — IS NULL / IS NOT NULL',
    `select employee_name from employees where manager_id is null or termination_date is not null;`,
    `SELECT employee_name
  FROM employees
 WHERE manager_id IS NULL
    OR termination_date IS NOT NULL;`
  );

  assertFormat('12.15 — BETWEEN in WHERE',
    `select order_id, order_date from orders where order_date between '2024-01-01' and '2024-12-31' and total_amount between 100 and 5000;`,
    `SELECT order_id, order_date
  FROM orders
 WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31'
   AND total_amount BETWEEN 100 AND 5000;`
  );

  assertFormat('12.16 — NOT IN',
    `select product_name from products where category_id not in (select category_id from discontinued_categories);`,
    `SELECT product_name
  FROM products
 WHERE category_id NOT IN
       (SELECT category_id
          FROM discontinued_categories);`
  );

  assertFormat('12.17 — SELECT * (star)',
    `select * from employees where department = 'Sales';`,
    `SELECT *
  FROM employees
 WHERE department = 'Sales';`
  );

  assertFormat('12.18 — Multiple aggregate functions',
    `select department, count(*) as cnt, min(salary) as min_sal, max(salary) as max_sal, avg(salary) as avg_sal, sum(salary) as total_sal from employees group by department;`,
    `SELECT department,
       COUNT(*) AS cnt,
       MIN(salary) AS min_sal,
       MAX(salary) AS max_sal,
       AVG(salary) AS avg_sal,
       SUM(salary) AS total_sal
  FROM employees
 GROUP BY department;`
  );

  assertFormat('12.19 — LIKE with wildcards',
    `select first_name, last_name from customers where last_name like 'Mc%' and first_name not like '_a%';`,
    `SELECT first_name, last_name
  FROM customers
 WHERE last_name LIKE 'Mc%'
   AND first_name NOT LIKE '_a%';`
  );

  assertFormat('12.20 — Subquery as JOIN target',
    `select s.store_name, m.month_name, m.total_sales from stores as s inner join (select store_id, date_trunc('month', sale_date) as month_name, sum(amount) as total_sales from transactions group by store_id, date_trunc('month', sale_date)) as m on s.store_id = m.store_id order by s.store_name, m.month_name;`,
    `SELECT s.store_name, m.month_name, m.total_sales
  FROM stores AS s
       INNER JOIN (SELECT store_id,
                          DATE_TRUNC('month', sale_date) AS month_name,
                          SUM(amount) AS total_sales
                     FROM transactions
                    GROUP BY store_id, DATE_TRUNC('month', sale_date)) AS m
       ON s.store_id = m.store_id
 ORDER BY s.store_name, m.month_name;`
  );
});

describe('Category 13: Negative Examples', () => {
  assertFormat('13.1 — Leading commas',
    `SELECT manufacturer
       , model
       , engine_size
  FROM motorbikes;`,
    `SELECT manufacturer, model, engine_size
  FROM motorbikes;`
  );

  assertFormat('13.2 — Lowercase keywords',
    `select e.name from employees as e where e.active = true and e.department = 'Sales';`,
    `SELECT e.name
  FROM employees AS e
 WHERE e.active = TRUE
   AND e.department = 'Sales';`
  );

  assertFormat('13.3 — No river alignment',
    `SELECT file_hash
FROM file_system
WHERE file_name = '.vimrc';`,
    `SELECT file_hash
  FROM file_system
 WHERE file_name = '.vimrc';`
  );

  assertFormat('13.4 — Tabs instead of spaces',
    `SELECT    file_hash
    FROM    file_system
    WHERE    file_name = '.vimrc';`,
    `SELECT file_hash
  FROM file_system
 WHERE file_name = '.vimrc';`
  );

  assertFormat('13.5 — No spaces around operators',
    `select price*quantity as total,price+tax as with_tax from items where price>100 and quantity>=5;`,
    `SELECT price * quantity AS total, price + tax AS with_tax
  FROM items
 WHERE price > 100
   AND quantity >= 5;`
  );

  assertFormat('13.6 — Missing semicolon',
    `select name from staff`,
    `SELECT name
  FROM staff;`
  );

  assertFormat('13.7 — Everything on one line with terrible spacing',
    `SELECT   a.id ,a.name,    a.email   FROM  accounts   a    WHERE     a.active=1     AND a.created>'2024-01-01'    ORDER BY    a.name`,
    `SELECT a.id, a.name, a.email
  FROM accounts AS a
 WHERE a.active = 1
   AND a.created > '2024-01-01'
 ORDER BY a.name;`
  );

  assertFormat('13.8 — Mixed indentation chaos',
    `  select
    e.name,
      e.salary
        from employees e
    where
  e.salary > 50000
      order by e.salary desc;`,
    `SELECT e.name, e.salary
  FROM employees AS e
 WHERE e.salary > 50000
 ORDER BY e.salary DESC;`
  );
});

describe('Category 14: ALTER TABLE and DROP', () => {
  assertFormat('14.1 — ALTER TABLE ADD COLUMN',
    `alter table staff add column email varchar(255) not null default '';`,
    `ALTER TABLE staff
        ADD COLUMN email VARCHAR(255) NOT NULL DEFAULT '';`
  );

  assertFormat('14.2 — DROP TABLE',
    `drop table if exists temporary_data;`,
    `DROP TABLE IF EXISTS temporary_data;`
  );
});

describe('Category 15: Complex Real-World Queries', () => {
  assertFormat('15.1 — Analytics query with CTE, window function, and CASE',
    `with monthly_metrics as (select date_trunc('month', order_date) as month, department, sum(revenue) as total_revenue, count(distinct customer_id) as unique_customers from orders where order_date >= '2024-01-01' group by date_trunc('month', order_date), department) select m.month, m.department, m.total_revenue, m.unique_customers, case when m.total_revenue > 1000000 then 'High' when m.total_revenue > 500000 then 'Medium' else 'Low' end as revenue_tier, row_number() over (partition by m.month order by m.total_revenue desc) as dept_rank from monthly_metrics as m order by m.month, dept_rank;`,
    `  WITH monthly_metrics AS (
           SELECT DATE_TRUNC('month', order_date) AS month,
                  department,
                  SUM(revenue) AS total_revenue,
                  COUNT(DISTINCT customer_id) AS unique_customers
             FROM orders
            WHERE order_date >= '2024-01-01'
            GROUP BY DATE_TRUNC('month', order_date), department
       )
SELECT m.month,
       m.department,
       m.total_revenue,
       m.unique_customers,
       CASE
       WHEN m.total_revenue > 1000000 THEN 'High'
       WHEN m.total_revenue > 500000 THEN 'Medium'
       ELSE 'Low'
       END AS revenue_tier,
       ROW_NUMBER() OVER (PARTITION BY m.month
                              ORDER BY m.total_revenue DESC) AS dept_rank
  FROM monthly_metrics AS m
 ORDER BY m.month, dept_rank;`
  );

  assertFormat('15.2 — Complex multi-CTE hospital analytics query with VALUES, subqueries, CASE, EXTRACT',
    `-- ============================================================================\n-- STACKED BAR CHART: Monthly Patient Volume by Category\n-- ============================================================================\n-- This query produces three volume columns for a stacked bar chart:\n--   1. Inpatient Volume     - from fully settled admissions (existing logic)\n--   2. Recurring Trials     - monthly clinical trial enrollment income\n--   3. One-time Equipment   - incidental/non-recurring equipment purchases\n--\n-- OUTPUT COLUMNS:\n--   Month                    (YYYYMM format)\n--   Inpatient_Volume         (sum of procedure/penalty/adjustment fees)\n--   Recurring_Trial_Revenue\n--   Onetime_Equipment_Revenue\n-- ============================================================================\n\n\n-- ############################################################################\n-- SYNTHETIC RESEARCH REVENUE SECTION\n-- ############################################################################\n-- Add new research revenue entries in the CTEs below.\n-- Each entry needs: record_date (DATE) and amount (NUMERIC)\n-- ############################################################################\n\nWITH\n\n-- ****************************************************************************\n-- ONE-TIME EQUIPMENT PURCHASES\n-- ****************************************************************************\n-- Add new one-time equipment purchases here.\n-- Format: (DATE 'YYYY-MM-DD', amount)\n--\n-- Example entries:\n--   (DATE '2025-12-19', 50000),    -- December 2025 purchase\n--   (DATE '2026-01-15', 25000),    -- January 2026 purchase\n--   (DATE '2026-02-01', 10000)     -- February 2026 purchase (no trailing comma on last entry)\n-- ****************************************************************************\nonetime_equipment_purchase_entries (record_date, amount) AS (\n    VALUES\n        -- \u2193\u2193\u2193 ADD NEW ONE-TIME EQUIPMENT PURCHASE ENTRIES BELOW THIS LINE \u2193\u2193\u2193\n        (DATE '2025-12-19', 50000.00)    -- Initial equipment order - December 2025\n        -- \u2191\u2191\u2191 ADD NEW ONE-TIME EQUIPMENT PURCHASE ENTRIES ABOVE THIS LINE \u2191\u2191\u2191\n        -- Remember: separate multiple entries with commas, no comma after the last one\n),\n\n-- ****************************************************************************\n-- RECURRING TRIAL REVENUE\n-- ****************************************************************************\n-- Add new recurring/subscription trial revenue here.\n-- Format: (DATE 'YYYY-MM-DD', amount)\n--\n-- Example entries:\n--   (DATE '2025-12-01', 5000),     -- December 2025 enrollment\n--   (DATE '2026-01-01', 5000),     -- January 2026 enrollment\n--   (DATE '2026-02-01', 5500)      -- February 2026 enrollment (rate increase)\n-- ****************************************************************************\nrecurring_trial_revenue_entries (record_date, amount) AS (\n    VALUES\n        -- \u2193\u2193\u2193 ADD NEW RECURRING TRIAL REVENUE ENTRIES BELOW THIS LINE \u2193\u2193\u2193\n        (DATE '1900-01-01', 0.00)        -- Placeholder (no trial revenue yet)\n        -- \u2191\u2191\u2191 ADD NEW RECURRING TRIAL REVENUE ENTRIES ABOVE THIS LINE \u2191\u2191\u2191\n        -- Remember: separate multiple entries with commas, no comma after the last one\n        -- Delete the placeholder row above once you add real entries\n),\n\n-- Aggregate one-time equipment revenue by month\nonetime_equipment_monthly AS (\n    SELECT\n        TO_CHAR(record_date, 'YYYYMM') AS month,\n        SUM(amount) AS onetime_equipment_revenue\n    FROM onetime_equipment_purchase_entries\n    WHERE record_date > DATE '1900-01-01'  -- Exclude placeholder\n    GROUP BY TO_CHAR(record_date, 'YYYYMM')\n),\n\n-- Aggregate recurring trial revenue by month\nrecurring_trial_monthly AS (\n    SELECT\n        TO_CHAR(record_date, 'YYYYMM') AS month,\n        SUM(amount) AS recurring_trial_revenue\n    FROM recurring_trial_revenue_entries\n    WHERE record_date > DATE '1900-01-01'  -- Exclude placeholder\n    GROUP BY TO_CHAR(record_date, 'YYYYMM')\n),\n\n\n-- ############################################################################\n-- INPATIENT VOLUME SECTION (existing logic - no changes needed)\n-- ############################################################################\n\nbase AS (\n    SELECT\n        patient_stay.admission_number,\n        a.stay_id,\n        patient_stay.stay_duration AS duration,\n        f.charges AS charges,\n        f.collection,\n        patient_stay.balance AS bal,\n        c.treatment_start_date,\n        CASE\n            WHEN lp.last_collection_date >= DATE '2025-10-01'\n                THEN lp.last_collection_date\n            ELSE fp.first_collection_date\n        END AS settle_date\n    FROM (SELECT DISTINCT stay_id FROM billing_ledger) AS a\n    LEFT JOIN patient_stay\n        ON patient_stay.uuid = a.stay_id\n    LEFT JOIN (\n        SELECT stay_id, action_date AS treatment_start_date\n        FROM treatment_ledger\n        WHERE action = 'treatment'\n    ) AS c\n        ON c.stay_id = a.stay_id\n    LEFT JOIN (\n        SELECT stay_id, MIN(action_date) AS first_collection_date\n        FROM collection_ledger\n        WHERE action = 'payment_received' AND action_amount > 1\n        GROUP BY stay_id\n    ) AS fp\n        ON fp.stay_id = a.stay_id\n    LEFT JOIN (\n        SELECT stay_id, MAX(action_date) AS last_collection_date\n        FROM collection_ledger\n        WHERE action = 'payment_received' AND action_amount > 1\n        GROUP BY stay_id\n    ) AS lp\n        ON lp.stay_id = a.stay_id\n    LEFT JOIN (\n        SELECT\n            stay_id,\n            SUM(CASE WHEN action IN ('procedure_fee_added','penalty_fee_added','adjustment_fee_added')\n                     THEN amount ELSE 0 END) AS charges,\n            SUM(CASE WHEN action = 'payment_received' THEN amount\n                     WHEN action = 'payment_reversed' THEN -1*amount\n                     ELSE 0 END) AS collection\n        FROM billing_ledger\n        GROUP BY stay_id\n    ) AS f\n        ON patient_stay.uuid = f.stay_id\n    WHERE patient_stay.billing_status = 'fully_settled'\n),\n\nenriched AS (\n    SELECT\n        admission_number,\n        stay_id,\n        duration,\n        charges,\n        collection,\n        bal,\n        treatment_start_date,\n        settle_date,\n        TO_CHAR(treatment_start_date,'YYYYMM') AS treatment_yearmon,\n        TO_CHAR(settle_date,'YYYYMM') AS settle_yearmon,\n        EXTRACT(DAY FROM (settle_date - treatment_start_date)) AS actual_duration,\n        (charges / NULLIF(bal,0)) * 365\n            / NULLIF(EXTRACT(DAY FROM (settle_date - treatment_start_date)), 0) AS APR\n    FROM base\n),\n\n-- Aggregate inpatient volume by month\ninpatient_monthly AS (\n    SELECT\n        settle_yearmon AS month,\n        SUM(charges) AS inpatient_volume\n    FROM enriched\n    GROUP BY settle_yearmon\n),\n\n\n-- ############################################################################\n-- COMBINE ALL VOLUME STREAMS\n-- ############################################################################\n\n-- Get all unique months across all volume types\nall_months AS (\n    SELECT month FROM inpatient_monthly\n    UNION\n    SELECT month FROM onetime_equipment_monthly\n    UNION\n    SELECT month FROM recurring_trial_monthly\n)\n\n-- ============================================================================\n-- FINAL OUTPUT: Stacked Bar Chart Data\n-- ============================================================================\nSELECT\n    am.month AS "Month",\n    COALESCE(im.inpatient_volume, 0) AS "Inpatient_Volume",\n    COALESCE(rtm.recurring_trial_revenue, 0) AS "Recurring_Trial_Revenue",\n    COALESCE(oem.onetime_equipment_revenue, 0) AS "Onetime_Equipment_Revenue"\nFROM all_months am\nLEFT JOIN inpatient_monthly im ON am.month = im.month\nLEFT JOIN recurring_trial_monthly rtm ON am.month = rtm.month\nLEFT JOIN onetime_equipment_monthly oem ON am.month = oem.month\nORDER BY am.month;`,
    `-- ============================================================================\n-- STACKED BAR CHART: Monthly Patient Volume by Category\n-- ============================================================================\n-- This query produces three volume columns for a stacked bar chart:\n--   1. Inpatient Volume     - from fully settled admissions (existing logic)\n--   2. Recurring Trials     - monthly clinical trial enrollment income\n--   3. One-time Equipment   - incidental/non-recurring equipment purchases\n--\n-- OUTPUT COLUMNS:\n--   Month                    (YYYYMM format)\n--   Inpatient_Volume         (sum of procedure/penalty/adjustment fees)\n--   Recurring_Trial_Revenue\n--   Onetime_Equipment_Revenue\n-- ============================================================================\n\n\n-- ############################################################################\n-- SYNTHETIC RESEARCH REVENUE SECTION\n-- ############################################################################\n-- Add new research revenue entries in the CTEs below.\n-- Each entry needs: record_date (DATE) and amount (NUMERIC)\n-- ############################################################################\n\n-- ****************************************************************************\n-- ONE-TIME EQUIPMENT PURCHASES\n-- ****************************************************************************\n-- Add new one-time equipment purchases here.\n-- Format: (DATE 'YYYY-MM-DD', amount)\n--\n-- Example entries:\n--   (DATE '2025-12-19', 50000),    -- December 2025 purchase\n--   (DATE '2026-01-15', 25000),    -- January 2026 purchase\n--   (DATE '2026-02-01', 10000)     -- February 2026 purchase (no trailing comma on last entry)\n-- ****************************************************************************\n\n  WITH onetime_equipment_purchase_entries (record_date, amount) AS (\n           VALUES\n               -- \u2193\u2193\u2193 ADD NEW ONE-TIME EQUIPMENT PURCHASE ENTRIES BELOW THIS LINE \u2193\u2193\u2193\n               (DATE '2025-12-19', 50000.00)  -- Initial equipment order - December 2025\n               -- \u2191\u2191\u2191 ADD NEW ONE-TIME EQUIPMENT PURCHASE ENTRIES ABOVE THIS LINE \u2191\u2191\u2191\n               -- Remember: separate multiple entries with commas, no comma after the last one\n       ),\n\n-- ****************************************************************************\n-- RECURRING TRIAL REVENUE\n-- ****************************************************************************\n-- Add new recurring/subscription trial revenue here.\n-- Format: (DATE 'YYYY-MM-DD', amount)\n--\n-- Example entries:\n--   (DATE '2025-12-01', 5000),     -- December 2025 enrollment\n--   (DATE '2026-01-01', 5000),     -- January 2026 enrollment\n--   (DATE '2026-02-01', 5500)      -- February 2026 enrollment (rate increase)\n-- ****************************************************************************\n\n       recurring_trial_revenue_entries (record_date, amount) AS (\n           VALUES\n               -- \u2193\u2193\u2193 ADD NEW RECURRING TRIAL REVENUE ENTRIES BELOW THIS LINE \u2193\u2193\u2193\n               (DATE '1900-01-01', 0.00)  -- Placeholder (no trial revenue yet)\n               -- \u2191\u2191\u2191 ADD NEW RECURRING TRIAL REVENUE ENTRIES ABOVE THIS LINE \u2191\u2191\u2191\n               -- Remember: separate multiple entries with commas, no comma after the last one\n               -- Delete the placeholder row above once you add real entries\n       ),\n\n       /* Aggregate one-time equipment revenue by month */\n       onetime_equipment_monthly AS (\n           SELECT TO_CHAR(record_date, 'YYYYMM') AS month,\n                  SUM(amount) AS onetime_equipment_revenue\n             FROM onetime_equipment_purchase_entries\n            WHERE record_date > DATE '1900-01-01'  -- Exclude placeholder\n            GROUP BY TO_CHAR(record_date, 'YYYYMM')\n       ),\n\n       /* Aggregate recurring trial revenue by month */\n       recurring_trial_monthly AS (\n           SELECT TO_CHAR(record_date, 'YYYYMM') AS month,\n                  SUM(amount) AS recurring_trial_revenue\n             FROM recurring_trial_revenue_entries\n            WHERE record_date > DATE '1900-01-01'  -- Exclude placeholder\n            GROUP BY TO_CHAR(record_date, 'YYYYMM')\n       ),\n\n-- ############################################################################\n-- INPATIENT VOLUME SECTION (existing logic - no changes needed)\n-- ############################################################################\n\n       base AS (\n           SELECT patient_stay.admission_number,\n                  a.stay_id,\n                  patient_stay.stay_duration AS duration,\n                  f.charges,\n                  f.collection,\n                  patient_stay.balance AS bal,\n                  c.treatment_start_date,\n                  CASE\n                  WHEN lp.last_collection_date >= DATE '2025-10-01' THEN lp.last_collection_date\n                  ELSE fp.first_collection_date\n                  END AS settle_date\n             FROM (SELECT DISTINCT stay_id\n                     FROM billing_ledger) AS a\n\n                  LEFT JOIN patient_stay\n                  ON patient_stay.uuid = a.stay_id\n\n                  LEFT JOIN (SELECT stay_id,\n                                    action_date AS treatment_start_date\n                               FROM treatment_ledger\n                              WHERE action = 'treatment') AS c\n                  ON c.stay_id = a.stay_id\n\n                  LEFT JOIN (SELECT stay_id,\n                                    MIN(action_date) AS first_collection_date\n                               FROM collection_ledger\n                              WHERE action = 'payment_received'\n                                AND action_amount > 1\n                              GROUP BY stay_id) AS fp\n                  ON fp.stay_id = a.stay_id\n\n                  LEFT JOIN (SELECT stay_id,\n                                    MAX(action_date) AS last_collection_date\n                               FROM collection_ledger\n                              WHERE action = 'payment_received'\n                                AND action_amount > 1\n                              GROUP BY stay_id) AS lp\n                  ON lp.stay_id = a.stay_id\n\n                  LEFT JOIN (SELECT stay_id,\n                                    SUM(CASE WHEN action IN ('procedure_fee_added',\n                                                             'penalty_fee_added',\n                                                             'adjustment_fee_added')\n                                             THEN amount ELSE 0 END) AS charges,\n                                    SUM(CASE WHEN action = 'payment_received' THEN amount\n                                             WHEN action = 'payment_reversed' THEN -1 * amount\n                                             ELSE 0 END) AS collection\n                               FROM billing_ledger\n                              GROUP BY stay_id) AS f\n                  ON patient_stay.uuid = f.stay_id\n\n            WHERE patient_stay.billing_status = 'fully_settled'\n       ),\n\n       enriched AS (\n           SELECT admission_number,\n                  stay_id,\n                  duration,\n                  charges,\n                  collection,\n                  bal,\n                  treatment_start_date,\n                  settle_date,\n                  TO_CHAR(treatment_start_date, 'YYYYMM') AS treatment_yearmon,\n                  TO_CHAR(settle_date, 'YYYYMM') AS settle_yearmon,\n                  EXTRACT(DAY FROM (settle_date - treatment_start_date)) AS actual_duration,\n                  (charges / NULLIF(bal, 0)) * 365\n                      / NULLIF(EXTRACT(DAY FROM (settle_date - treatment_start_date)), 0) AS apr\n             FROM base\n       ),\n\n       /* Aggregate inpatient volume by month */\n       inpatient_monthly AS (\n           SELECT settle_yearmon AS month,\n                  SUM(charges) AS inpatient_volume\n             FROM enriched\n            GROUP BY settle_yearmon\n       ),\n\n-- ############################################################################\n-- COMBINE ALL VOLUME STREAMS\n-- ############################################################################\n\n       /* Get all unique months across all volume types */\n       all_months AS (\n           SELECT month\n             FROM inpatient_monthly\n\n            UNION\n\n           SELECT month\n             FROM onetime_equipment_monthly\n\n            UNION\n\n           SELECT month\n             FROM recurring_trial_monthly\n       )\n\n-- ============================================================================\n-- FINAL OUTPUT: Stacked Bar Chart Data\n-- ============================================================================\nSELECT am.month AS "Month",\n       COALESCE(im.inpatient_volume, 0) AS "Inpatient_Volume",\n       COALESCE(rtm.recurring_trial_revenue, 0) AS "Recurring_Trial_Revenue",\n       COALESCE(oem.onetime_equipment_revenue, 0) AS "Onetime_Equipment_Revenue"\n  FROM all_months AS am\n       LEFT JOIN inpatient_monthly AS im\n       ON am.month = im.month\n\n       LEFT JOIN recurring_trial_monthly AS rtm\n       ON am.month = rtm.month\n\n       LEFT JOIN onetime_equipment_monthly AS oem\n       ON am.month = oem.month\n ORDER BY am.month;`
  );
});
