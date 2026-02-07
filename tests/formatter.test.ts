import { describe, it, expect } from 'bun:test';
import { formatSQL } from '../src/format';
import { formatStatements } from '../src/formatter';
import type * as AST from '../src/ast';

function assertFormat(name: string, input: string, expected: string) {
  it(name, () => {
    const result = formatSQL(input).trimEnd();
    const exp = expected.trimEnd();
    expect(result).toBe(exp);
  });
}

describe('Category 1: Basic SELECT Queries', () => {
  const tests: [string, string, string][] = [
    ['1.1 — Simple single-table SELECT',
      `select file_hash from file_system where file_name = '.vimrc';`,
      `SELECT file_hash\n  FROM file_system\n WHERE file_name = '.vimrc';`],
    ['1.2 — Multiple columns, single table',
      `select a.title, a.release_date, a.recording_date from albums as a where a.title = 'Charcoal Lane' or a.title = 'The New Danger';`,
      `SELECT a.title, a.release_date, a.recording_date\n  FROM albums AS a\n WHERE a.title = 'Charcoal Lane'\n    OR a.title = 'The New Danger';`],
    ['1.3 — Column list wrapping with logical grouping',
      `select a.title, a.release_date, a.recording_date, a.production_date from albums as a where a.title = 'Charcoal Lane' or a.title = 'The New Danger';`,
      `SELECT a.title,\n       a.release_date, a.recording_date, a.production_date\n  FROM albums AS a\n WHERE a.title = 'Charcoal Lane'\n    OR a.title = 'The New Danger';`],
    ['1.4 — Simple SELECT with alias',
      `select first_name as fn from staff;`,
      `SELECT first_name AS fn\n  FROM staff;`],
    ['1.5 — Aggregate with alias',
      `select sum(s.monitor_tally) as monitor_total from staff as s;`,
      `SELECT SUM(s.monitor_tally) AS monitor_total\n  FROM staff AS s;`],
    ['1.6 — WHERE with AND',
      `select model_num from phones as p where p.release_date > '2014-09-30' and p.manufacturer = 'Apple';`,
      `SELECT model_num\n  FROM phones AS p\n WHERE p.release_date > '2014-09-30'\n   AND p.manufacturer = 'Apple';`],
    ['1.7 — SELECT with no WHERE clause',
      `select first_name from staff;`,
      `SELECT first_name\n  FROM staff;`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
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

  assertFormat('3.5 — IN subquery with UNION',
    `select * from t where id in (select id from a union select id from b);`,
    `SELECT *
  FROM t
 WHERE id IN
       (SELECT id
          FROM a

         UNION

        SELECT id
          FROM b);`
  );

  assertFormat('3.6 — Subquery in FROM with CTE',
    `select * from (with x as (select 1 as a) select a from x) q;`,
    `SELECT *
  FROM (  WITH x AS (
                   SELECT 1 AS a
               )
        SELECT a
          FROM x) AS q;`
  );

  assertFormat('3.7 — IN subquery with CTE',
    `select 1 where 1 in (with x as (select 1 as id) select id from x);`,
    `SELECT 1
 WHERE 1 IN
       (  WITH x AS (
                   SELECT 1 AS id
               )
        SELECT id
          FROM x);`
  );
});

describe('Category 4: CASE Expressions', () => {
  const tests: [string, string, string][] = [
    ['4.1 — Simple CASE',
      `select case postcode when 'BN1' then 'Brighton' when 'EH1' then 'Edinburgh' end as city from office_locations where country = 'United Kingdom' and opening_time between 8 and 9 and postcode in ('EH1', 'BN1', 'NN1', 'KW1');`,
      `SELECT CASE postcode\n       WHEN 'BN1' THEN 'Brighton'\n       WHEN 'EH1' THEN 'Edinburgh'\n       END AS city\n  FROM office_locations\n WHERE country = 'United Kingdom'\n   AND opening_time BETWEEN 8 AND 9\n   AND postcode IN ('EH1', 'BN1', 'NN1', 'KW1');`],
    ['4.2 — Searched CASE with ELSE',
      `select employee_name, case when salary > 100000 then 'Senior' when salary > 50000 then 'Mid' else 'Junior' end as level from employees;`,
      `SELECT employee_name,\n       CASE\n       WHEN salary > 100000 THEN 'Senior'\n       WHEN salary > 50000 THEN 'Mid'\n       ELSE 'Junior'\n       END AS level\n  FROM employees;`],
    ['4.3 — Nested CASE',
      `select product_name, case category when 'Electronics' then case when price > 1000 then 'Premium' else 'Standard' end when 'Books' then 'Literature' else 'Other' end as classification from products;`,
      `SELECT product_name,\n       CASE category\n       WHEN 'Electronics' THEN CASE\n                               WHEN price > 1000 THEN 'Premium'\n                               ELSE 'Standard'\n                               END\n       WHEN 'Books' THEN 'Literature'\n       ELSE 'Other'\n       END AS classification\n  FROM products;`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
});

describe('Category 5: INSERT / UPDATE / DELETE', () => {
  const tests: [string, string, string][] = [
    ['5.1 — Simple INSERT',
      `insert into albums (title, release_date, recording_date) values ('Charcoal Lane', '1990-01-01 01:01:01.00000', '1990-01-01 01:01:01.00000'), ('The New Danger', '2008-01-01 01:01:01.00000', '1990-01-01 01:01:01.00000');`,
      `INSERT INTO albums (title, release_date, recording_date)\nVALUES ('Charcoal Lane', '1990-01-01 01:01:01.00000', '1990-01-01 01:01:01.00000'),\n       ('The New Danger', '2008-01-01 01:01:01.00000', '1990-01-01 01:01:01.00000');`],
    ['5.2 — UPDATE with multiple SET',
      `update file_system set file_modified_date = '1980-02-22 13:19:01.00000', file_size = 209732 where file_name = '.vimrc';`,
      `UPDATE file_system\n   SET file_modified_date = '1980-02-22 13:19:01.00000',\n       file_size = 209732\n WHERE file_name = '.vimrc';`],
    ['5.3 — Simple DELETE',
      `delete from albums where title = 'The New Danger';`,
      `DELETE\n  FROM albums\n WHERE title = 'The New Danger';`],
    ['5.4 — INSERT with SELECT',
      `insert into archive_albums (title, release_date) select title, release_date from albums where release_date < '2000-01-01';`,
      `INSERT INTO archive_albums (title, release_date)\nSELECT title, release_date\n  FROM albums\n WHERE release_date < '2000-01-01';`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
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
  const tests: [string, string, string][] = [
    ['11.1 — GROUP BY with HAVING',
      `select department, count(*) as employee_count, avg(salary) as avg_salary from employees group by department having count(*) > 5 and avg(salary) > 60000 order by avg_salary desc;`,
      `SELECT department,\n       COUNT(*) AS employee_count,\n       AVG(salary) AS avg_salary\n  FROM employees\n GROUP BY department\nHAVING COUNT(*) > 5\n   AND AVG(salary) > 60000\n ORDER BY avg_salary DESC;`],
    ['11.2 — GROUP BY multiple columns',
      `select region, department, sum(revenue) as total_revenue from sales group by region, department order by region, total_revenue desc;`,
      `SELECT region, department, SUM(revenue) AS total_revenue\n  FROM sales\n GROUP BY region, department\n ORDER BY region, total_revenue DESC;`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
});

describe('Category 12: Edge Cases and Stress Tests', () => {
  const tests: [string, string, string][] = [
    ['12.1 — Already formatted (idempotency)',
      `SELECT file_hash\n  FROM file_system\n WHERE file_name = '.vimrc';`,
      `SELECT file_hash\n  FROM file_system\n WHERE file_name = '.vimrc';`],
    ['12.3 — Multiple statements',
      `select 1; select 2;`,
      `SELECT 1;\n\nSELECT 2;`],
    ['12.4 — Empty/whitespace input',
      `   `,
      ``],
    ['12.6 — Mixed case keywords in input',
      `Select First_Name As FN From Staff Where Department = 'Sales' Order By First_Name Asc;`,
      `SELECT first_name AS fn\n  FROM staff\n WHERE department = 'Sales'\n ORDER BY first_name ASC;`],
    ['12.7 — String literals containing keywords',
      `select message from logs where message like '%SELECT FROM%' and level = 'ERROR';`,
      `SELECT message\n  FROM logs\n WHERE message LIKE '%SELECT FROM%'\n   AND level = 'ERROR';`],
    ['12.8 — Quoted identifiers',
      `select "Order ID", "Customer Name" from "Order Table" where "Order Date" > '2024-01-01';`,
      `SELECT "Order ID", "Customer Name"\n  FROM "Order Table"\n WHERE "Order Date" > '2024-01-01';`],
    ['12.8b — Escaped quoted identifiers',
      `select "a""b", "schema""x"."weird""col" from "T""A";`,
      `SELECT "a""b", "schema""x"."weird""col"\n  FROM "T""A";`],
    ['12.9 — DISTINCT keyword',
      `select distinct department, city from employees where country = 'US' order by department;`,
      `SELECT DISTINCT department, city\n  FROM employees\n WHERE country = 'US'\n ORDER BY department;`],
    ['12.10 — SELECT with LIMIT and OFFSET',
      `select employee_name, salary from employees order by salary desc limit 10 offset 20;`,
      `SELECT employee_name, salary\n  FROM employees\n ORDER BY salary DESC\n LIMIT 10\nOFFSET 20;`],
    ['12.11 — COALESCE and NULLIF',
      `select coalesce(preferred_name, first_name) as display_name, nullif(middle_name, '') as middle_name from staff;`,
      `SELECT COALESCE(preferred_name, first_name) AS display_name,\n       NULLIF(middle_name, '') AS middle_name\n  FROM staff;`],
    ['12.12 — CAST expression',
      `select cast(order_date as date) as order_day, cast(amount as decimal(10, 2)) as formatted_amount from orders;`,
      `SELECT CAST(order_date AS DATE) AS order_day,\n       CAST(amount AS DECIMAL(10, 2)) AS formatted_amount\n  FROM orders;`],
    ['12.13 — Arithmetic expressions',
      `select product_name, price * quantity as line_total, (price * quantity) - discount as net_total from order_items where (price * quantity) > 100;`,
      `SELECT product_name,\n       price * quantity AS line_total,\n       (price * quantity) - discount AS net_total\n  FROM order_items\n WHERE (price * quantity) > 100;`],
    ['12.14 — IS NULL / IS NOT NULL',
      `select employee_name from employees where manager_id is null or termination_date is not null;`,
      `SELECT employee_name\n  FROM employees\n WHERE manager_id IS NULL\n    OR termination_date IS NOT NULL;`],
    ['12.14b — IS NOT TRUE / IS NOT FALSE',
      `select user_id from feature_flags where is_enabled is not true or is_enabled is not false;`,
      `SELECT user_id\n  FROM feature_flags\n WHERE is_enabled IS NOT TRUE\n    OR is_enabled IS NOT FALSE;`],
    ['12.15 — BETWEEN in WHERE',
      `select order_id, order_date from orders where order_date between '2024-01-01' and '2024-12-31' and total_amount between 100 and 5000;`,
      `SELECT order_id, order_date\n  FROM orders\n WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31'\n   AND total_amount BETWEEN 100 AND 5000;`],
    ['12.16 — NOT IN',
      `select product_name from products where category_id not in (select category_id from discontinued_categories);`,
      `SELECT product_name\n  FROM products\n WHERE category_id NOT IN\n       (SELECT category_id\n          FROM discontinued_categories);`],
    ['12.17 — SELECT * (star)',
      `select * from employees where department = 'Sales';`,
      `SELECT *\n  FROM employees\n WHERE department = 'Sales';`],
    ['12.18 — Multiple aggregate functions',
      `select department, count(*) as cnt, min(salary) as min_sal, max(salary) as max_sal, avg(salary) as avg_sal, sum(salary) as total_sal from employees group by department;`,
      `SELECT department,\n       COUNT(*) AS cnt,\n       MIN(salary) AS min_sal,\n       MAX(salary) AS max_sal,\n       AVG(salary) AS avg_sal,\n       SUM(salary) AS total_sal\n  FROM employees\n GROUP BY department;`],
    ['12.19 — LIKE with wildcards',
      `select first_name, last_name from customers where last_name like 'Mc%' and first_name not like '_a%';`,
      `SELECT first_name, last_name\n  FROM customers\n WHERE last_name LIKE 'Mc%'\n   AND first_name NOT LIKE '_a%';`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);

  // Tests with complex multi-line expected output kept as individual calls
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
  const tests: [string, string, string][] = [
    ['13.1 — Leading commas',
      `SELECT manufacturer\n       , model\n       , engine_size\n  FROM motorbikes;`,
      `SELECT manufacturer, model, engine_size\n  FROM motorbikes;`],
    ['13.2 — Lowercase keywords',
      `select e.name from employees as e where e.active = true and e.department = 'Sales';`,
      `SELECT e.name\n  FROM employees AS e\n WHERE e.active = TRUE\n   AND e.department = 'Sales';`],
    ['13.3 — No river alignment',
      `SELECT file_hash\nFROM file_system\nWHERE file_name = '.vimrc';`,
      `SELECT file_hash\n  FROM file_system\n WHERE file_name = '.vimrc';`],
    ['13.4 — Tabs instead of spaces',
      `SELECT    file_hash\n    FROM    file_system\n    WHERE    file_name = '.vimrc';`,
      `SELECT file_hash\n  FROM file_system\n WHERE file_name = '.vimrc';`],
    ['13.5 — No spaces around operators',
      `select price*quantity as total,price+tax as with_tax from items where price>100 and quantity>=5;`,
      `SELECT price * quantity AS total, price + tax AS with_tax\n  FROM items\n WHERE price > 100\n   AND quantity >= 5;`],
    ['13.6 — Missing semicolon',
      `select name from staff`,
      `SELECT name\n  FROM staff;`],
    ['13.7 — Everything on one line with terrible spacing',
      `SELECT   a.id ,a.name,    a.email   FROM  accounts   a    WHERE     a.active=1     AND a.created>'2024-01-01'    ORDER BY    a.name`,
      `SELECT a.id, a.name, a.email\n  FROM accounts AS a\n WHERE a.active = 1\n   AND a.created > '2024-01-01'\n ORDER BY a.name;`],
    ['13.8 — Mixed indentation chaos',
      `  select\n    e.name,\n      e.salary\n        from employees e\n    where\n  e.salary > 50000\n      order by e.salary desc;`,
      `SELECT e.name, e.salary\n  FROM employees AS e\n WHERE e.salary > 50000\n ORDER BY e.salary DESC;`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
});

describe('Category 14: ALTER TABLE and DROP', () => {
  const tests: [string, string, string][] = [
    ['14.1 — ALTER TABLE ADD COLUMN',
      `alter table staff add column email varchar(255) not null default '';`,
      `ALTER TABLE staff\n        ADD COLUMN email VARCHAR(255) NOT NULL DEFAULT '';`],
    ['14.2 — DROP TABLE',
      `drop table if exists temporary_data;`,
      `DROP TABLE IF EXISTS temporary_data;`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
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
    `-- ============================================================================\n-- STACKED BAR CHART: Monthly Patient Volume by Category\n-- ============================================================================\n-- This query produces three volume columns for a stacked bar chart:\n--   1. Inpatient Volume     - from fully settled admissions (existing logic)\n--   2. Recurring Trials     - monthly clinical trial enrollment income\n--   3. One-time Equipment   - incidental/non-recurring equipment purchases\n--\n-- OUTPUT COLUMNS:\n--   Month                    (YYYYMM format)\n--   Inpatient_Volume         (sum of procedure/penalty/adjustment fees)\n--   Recurring_Trial_Revenue\n--   Onetime_Equipment_Revenue\n-- ============================================================================\n\n\n-- ############################################################################\n-- SYNTHETIC RESEARCH REVENUE SECTION\n-- ############################################################################\n-- Add new research revenue entries in the CTEs below.\n-- Each entry needs: record_date (DATE) and amount (NUMERIC)\n-- ############################################################################\n\n-- ****************************************************************************\n-- ONE-TIME EQUIPMENT PURCHASES\n-- ****************************************************************************\n-- Add new one-time equipment purchases here.\n-- Format: (DATE 'YYYY-MM-DD', amount)\n--\n-- Example entries:\n--   (DATE '2025-12-19', 50000),    -- December 2025 purchase\n--   (DATE '2026-01-15', 25000),    -- January 2026 purchase\n--   (DATE '2026-02-01', 10000)     -- February 2026 purchase (no trailing comma on last entry)\n-- ****************************************************************************\n\n  WITH onetime_equipment_purchase_entries (record_date, amount) AS (\n           VALUES\n               -- ↓↓↓ ADD NEW ONE-TIME EQUIPMENT PURCHASE ENTRIES BELOW THIS LINE ↓↓↓\n               (DATE '2025-12-19', 50000.00)  -- Initial equipment order - December 2025\n               -- ↑↑↑ ADD NEW ONE-TIME EQUIPMENT PURCHASE ENTRIES ABOVE THIS LINE ↑↑↑\n               -- Remember: separate multiple entries with commas, no comma after the last one\n       ),\n\n-- ****************************************************************************\n-- RECURRING TRIAL REVENUE\n-- ****************************************************************************\n-- Add new recurring/subscription trial revenue here.\n-- Format: (DATE 'YYYY-MM-DD', amount)\n--\n-- Example entries:\n--   (DATE '2025-12-01', 5000),     -- December 2025 enrollment\n--   (DATE '2026-01-01', 5000),     -- January 2026 enrollment\n--   (DATE '2026-02-01', 5500)      -- February 2026 enrollment (rate increase)\n-- ****************************************************************************\n\n       recurring_trial_revenue_entries (record_date, amount) AS (\n           VALUES\n               -- ↓↓↓ ADD NEW RECURRING TRIAL REVENUE ENTRIES BELOW THIS LINE ↓↓↓\n               (DATE '1900-01-01', 0.00)  -- Placeholder (no trial revenue yet)\n               -- ↑↑↑ ADD NEW RECURRING TRIAL REVENUE ENTRIES ABOVE THIS LINE ↑↑↑\n               -- Remember: separate multiple entries with commas, no comma after the last one\n               -- Delete the placeholder row above once you add real entries\n       ),\n\n-- Aggregate one-time equipment revenue by month\n\n       onetime_equipment_monthly AS (\n           SELECT TO_CHAR(record_date, 'YYYYMM') AS month,\n                  SUM(amount) AS onetime_equipment_revenue\n             FROM onetime_equipment_purchase_entries\n            WHERE record_date > DATE '1900-01-01'  -- Exclude placeholder\n            GROUP BY TO_CHAR(record_date, 'YYYYMM')\n       ),\n\n-- Aggregate recurring trial revenue by month\n\n       recurring_trial_monthly AS (\n           SELECT TO_CHAR(record_date, 'YYYYMM') AS month,\n                  SUM(amount) AS recurring_trial_revenue\n             FROM recurring_trial_revenue_entries\n            WHERE record_date > DATE '1900-01-01'  -- Exclude placeholder\n            GROUP BY TO_CHAR(record_date, 'YYYYMM')\n       ),\n\n-- ############################################################################\n-- INPATIENT VOLUME SECTION (existing logic - no changes needed)\n-- ############################################################################\n\n       base AS (\n           SELECT patient_stay.admission_number,\n                  a.stay_id,\n                  patient_stay.stay_duration AS duration,\n                  f.charges,\n                  f.collection,\n                  patient_stay.balance AS bal,\n                  c.treatment_start_date,\n                  CASE\n                  WHEN lp.last_collection_date >= DATE '2025-10-01' THEN lp.last_collection_date\n                  ELSE fp.first_collection_date\n                  END AS settle_date\n             FROM (SELECT DISTINCT stay_id\n                     FROM billing_ledger) AS a\n\n                  LEFT JOIN patient_stay\n                  ON patient_stay.uuid = a.stay_id\n\n                  LEFT JOIN (SELECT stay_id,\n                                    action_date AS treatment_start_date\n                               FROM treatment_ledger\n                              WHERE action = 'treatment') AS c\n                  ON c.stay_id = a.stay_id\n\n                  LEFT JOIN (SELECT stay_id,\n                                    MIN(action_date) AS first_collection_date\n                               FROM collection_ledger\n                              WHERE action = 'payment_received'\n                                AND action_amount > 1\n                              GROUP BY stay_id) AS fp\n                  ON fp.stay_id = a.stay_id\n\n                  LEFT JOIN (SELECT stay_id,\n                                    MAX(action_date) AS last_collection_date\n                               FROM collection_ledger\n                              WHERE action = 'payment_received'\n                                AND action_amount > 1\n                              GROUP BY stay_id) AS lp\n                  ON lp.stay_id = a.stay_id\n\n                  LEFT JOIN (SELECT stay_id,\n                                    SUM(CASE WHEN action IN ('procedure_fee_added', 'penalty_fee_added',\n                                                             'adjustment_fee_added')\n                                             THEN amount ELSE 0 END) AS charges,\n                                    SUM(CASE WHEN action = 'payment_received' THEN amount\n                                             WHEN action = 'payment_reversed' THEN -1 * amount\n                                             ELSE 0 END) AS collection\n                               FROM billing_ledger\n                              GROUP BY stay_id) AS f\n                  ON patient_stay.uuid = f.stay_id\n\n            WHERE patient_stay.billing_status = 'fully_settled'\n       ),\n\n       enriched AS (\n           SELECT admission_number,\n                  stay_id,\n                  duration,\n                  charges,\n                  collection,\n                  bal,\n                  treatment_start_date,\n                  settle_date,\n                  TO_CHAR(treatment_start_date, 'YYYYMM') AS treatment_yearmon,\n                  TO_CHAR(settle_date, 'YYYYMM') AS settle_yearmon,\n                  EXTRACT(DAY FROM (settle_date - treatment_start_date)) AS actual_duration,\n                  (charges / NULLIF(bal, 0)) * 365\n                      / NULLIF(EXTRACT(DAY FROM (settle_date - treatment_start_date)), 0) AS apr\n             FROM base\n       ),\n\n-- Aggregate inpatient volume by month\n\n       inpatient_monthly AS (\n           SELECT settle_yearmon AS month,\n                  SUM(charges) AS inpatient_volume\n             FROM enriched\n            GROUP BY settle_yearmon\n       ),\n\n-- ############################################################################\n-- COMBINE ALL VOLUME STREAMS\n-- ############################################################################\n\n-- Get all unique months across all volume types\n\n       all_months AS (\n           SELECT month\n             FROM inpatient_monthly\n\n            UNION\n\n           SELECT month\n             FROM onetime_equipment_monthly\n\n            UNION\n\n           SELECT month\n             FROM recurring_trial_monthly\n       )\n\n-- ============================================================================\n-- FINAL OUTPUT: Stacked Bar Chart Data\n-- ============================================================================\nSELECT am.month AS "Month",\n       COALESCE(im.inpatient_volume, 0) AS "Inpatient_Volume",\n       COALESCE(rtm.recurring_trial_revenue, 0) AS "Recurring_Trial_Revenue",\n       COALESCE(oem.onetime_equipment_revenue, 0) AS "Onetime_Equipment_Revenue"\n  FROM all_months AS am\n       LEFT JOIN inpatient_monthly AS im\n       ON am.month = im.month\n\n       LEFT JOIN recurring_trial_monthly AS rtm\n       ON am.month = rtm.month\n\n       LEFT JOIN onetime_equipment_monthly AS oem\n       ON am.month = oem.month\n ORDER BY am.month;`
  );
});

describe('Category 16: PostgreSQL Type Casting (::)', () => {
  const tests: [string, string, string][] = [
    ['16.1 — Simple cast with ::',
      `select '2024-01-15'::date as start_date, '100.50'::numeric(10, 2) as amount from transactions;`,
      `SELECT '2024-01-15'::DATE AS start_date,\n       '100.50'::NUMERIC(10, 2) AS amount\n  FROM transactions;`],
    ['16.2 — Cast in WHERE clause',
      `select order_id, total from orders where created_at::date = '2025-01-01' and total::integer > 100;`,
      `SELECT order_id, total\n  FROM orders\n WHERE created_at::DATE = '2025-01-01'\n   AND total::INTEGER > 100;`],
    ['16.3 — Chained casts and cast with array',
      `select id, payload::text::integer as parsed_id, tags::text[] as tag_array from raw_events where payload::text <> '';`,
      `SELECT id,\n       payload::TEXT::INTEGER AS parsed_id,\n       tags::TEXT[] AS tag_array\n  FROM raw_events\n WHERE payload::TEXT <> '';`],
    ['16.4 — Cast in aggregate and expression',
      `select department, sum(salary::numeric(12, 2)) as total_salary, avg(salary::numeric)::numeric(10, 2) as avg_salary from employees group by department;`,
      `SELECT department,\n       SUM(salary::NUMERIC(12, 2)) AS total_salary,\n       AVG(salary::NUMERIC)::NUMERIC(10, 2) AS avg_salary\n  FROM employees\n GROUP BY department;`],
    ['16.5 — Cast to INTERVAL',
      `select event_name, event_date, event_date + duration::interval as end_time, ('30 days')::interval as buffer from events where event_date + duration::interval > now();`,
      `SELECT event_name,\n       event_date,\n       event_date + duration::INTERVAL AS end_time,\n       ('30 days')::INTERVAL AS buffer\n  FROM events\n WHERE event_date + duration::INTERVAL > NOW();`],
    ['16.6 — Cast inside CASE',
      `select name, case when amount::numeric > 1000 then 'high'::varchar when amount::numeric > 100 then 'medium'::varchar else 'low'::varchar end as tier from invoices;`,
      `SELECT name,\n       CASE\n       WHEN amount::NUMERIC > 1000 THEN 'high'::VARCHAR\n       WHEN amount::NUMERIC > 100 THEN 'medium'::VARCHAR\n       ELSE 'low'::VARCHAR\n       END AS tier\n  FROM invoices;`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
});

describe('Category 17: JSON and JSONB Operators', () => {
  assertFormat('17.1 — Arrow operators (-> and ->>)',
    `select id, payload -> 'user' as user_obj, payload -> 'user' ->> 'name' as user_name, payload -> 'user' ->> 'email' as user_email from events where payload ->> 'type' = 'login';`,
    `SELECT id,
       payload -> 'user' AS user_obj,
       payload -> 'user' ->> 'name' AS user_name,
       payload -> 'user' ->> 'email' AS user_email
  FROM events
 WHERE payload ->> 'type' = 'login';`
  );

  assertFormat('17.2 — Path operators (#> and #>>)',
    `select id, data #> '{address,city}' as city_json, data #>> '{address,city}' as city_text, data #>> '{contacts,0,phone}' as first_phone from customers where data #>> '{address,country}' = 'US';`,
    `SELECT id,
       data #> '{address,city}' AS city_json,
       data #>> '{address,city}' AS city_text,
       data #>> '{contacts,0,phone}' AS first_phone
  FROM customers
 WHERE data #>> '{address,country}' = 'US';`
  );

  assertFormat('17.3 — JSONB containment (@>, <@)',
    `select id, name from products where metadata @> '{"featured": true}'::jsonb and attributes @> '{"color": "red"}'::jsonb;`,
    `SELECT id, name
  FROM products
 WHERE metadata @> '{"featured": true}'::JSONB
   AND attributes @> '{"color": "red"}'::JSONB;`
  );

  assertFormat('17.4 — JSONB existence (?, ?|, ?&)',
    `select id, name from products where tags ? 'sale' and metadata ?| array['featured', 'promoted'] and requirements ?& array['size', 'color'];`,
    `SELECT id, name
  FROM products
 WHERE tags ? 'sale'
   AND metadata ?| ARRAY['featured', 'promoted']
   AND requirements ?& ARRAY['size', 'color'];`
  );

  assertFormat('17.5 — JSONB concatenation and deletion',
    `update user_profiles set preferences = preferences || '{"theme": "dark"}'::jsonb, metadata = metadata - 'deprecated_key', tags = tags - 'old_tag' where user_id = 42;`,
    `UPDATE user_profiles
   SET preferences = preferences || '{"theme": "dark"}'::JSONB,
       metadata = metadata - 'deprecated_key',
       tags = tags - 'old_tag'
 WHERE user_id = 42;`
  );

  assertFormat('17.6 — jsonb_build_object and jsonb_agg',
    `select d.department_name, jsonb_build_object('count', count(*), 'avg_salary', avg(e.salary)::numeric(10, 2), 'employees', jsonb_agg(jsonb_build_object('name', e.name, 'title', e.title) order by e.name)) as department_summary from departments as d inner join employees as e on d.department_id = e.department_id group by d.department_name;`,
    `SELECT d.department_name,
       JSONB_BUILD_OBJECT(
           'count', COUNT(*),
           'avg_salary', AVG(e.salary)::NUMERIC(10, 2),
           'employees', JSONB_AGG(
               JSONB_BUILD_OBJECT('name', e.name, 'title', e.title)
               ORDER BY e.name
           )
       ) AS department_summary
  FROM departments AS d
       INNER JOIN employees AS e
       ON d.department_id = e.department_id
 GROUP BY d.department_name;`
  );

  assertFormat('17.7 — jsonb_each, jsonb_array_elements (set-returning in FROM)',
    `select e.id, kv.key as setting_name, kv.value as setting_value from events as e, jsonb_each(e.settings) as kv where kv.key like 'notification_%';`,
    `SELECT e.id,
       kv.key AS setting_name,
       kv.value AS setting_value
  FROM events AS e,
       JSONB_EACH(e.settings) AS kv
 WHERE kv.key LIKE 'notification_%';`
  );

  assertFormat('17.8 — JSON path expression (PostgreSQL 12+)',
    `select id, data @? '$.items[*] ? (@.price > 100)' as has_expensive, jsonb_path_query_array(data, '$.items[*].name') as item_names from orders where data @@ '$.total > 500';`,
    `SELECT id,
       data @? '$.items[*] ? (@.price > 100)' AS has_expensive,
       JSONB_PATH_QUERY_ARRAY(data, '$.items[*].name') AS item_names
  FROM orders
 WHERE data @@ '$.total > 500';`
  );
});

describe('Category 18: Array Operators and Syntax', () => {
  assertFormat('18.1 — ARRAY constructor and ANY/ALL',
    `select product_name, price from products where category_id = any(array[1, 2, 3]) and price > all(array[10.00, 20.00]);`,
    `SELECT product_name, price
  FROM products
 WHERE category_id = ANY(ARRAY[1, 2, 3])
   AND price > ALL(ARRAY[10.00, 20.00]);`
  );

  assertFormat('18.2 — Array overlap (&&), contains (@>), contained by (<@)',
    `select id, name from users where interests && array['music', 'art'] and skills @> array['python', 'sql'] and roles <@ array['admin', 'editor', 'viewer'];`,
    `SELECT id, name
  FROM users
 WHERE interests && ARRAY['music', 'art']
   AND skills @> ARRAY['python', 'sql']
   AND roles <@ ARRAY['admin', 'editor', 'viewer'];`
  );

  assertFormat('18.3 — Array subscript and slice',
    `select employee_name, phone_numbers[1] as primary_phone, phone_numbers[2:3] as alt_phones, array_length(phone_numbers, 1) as phone_count from employees where phone_numbers[1] is not null;`,
    `SELECT employee_name,
       phone_numbers[1] AS primary_phone,
       phone_numbers[2:3] AS alt_phones,
       ARRAY_LENGTH(phone_numbers, 1) AS phone_count
  FROM employees
 WHERE phone_numbers[1] IS NOT NULL;`
  );

  assertFormat('18.4 — ARRAY_AGG with ORDER BY',
    `select department, array_agg(employee_name order by hire_date) as employees_by_seniority, array_agg(distinct job_title order by job_title) as unique_titles from employees group by department;`,
    `SELECT department,
       ARRAY_AGG(employee_name ORDER BY hire_date) AS employees_by_seniority,
       ARRAY_AGG(DISTINCT job_title ORDER BY job_title) AS unique_titles
  FROM employees
 GROUP BY department;`
  );

  assertFormat('18.5 — UNNEST',
    `select u.id, u.name, t.tag from users as u, unnest(u.tags) as t(tag) where t.tag like 'vip_%';`,
    `SELECT u.id, u.name, t.tag
  FROM users AS u,
       UNNEST(u.tags) AS t(tag)
 WHERE t.tag LIKE 'vip_%';`
  );
});

describe('Category 19: String Operators and Pattern Matching', () => {
  assertFormat('19.1 — Concatenation operator (||)',
    `select first_name || ' ' || last_name as full_name, 'ID-' || id::text || '-' || region as composite_key from employees where first_name || ' ' || last_name like 'John%';`,
    `SELECT first_name || ' ' || last_name AS full_name,
       'ID-' || id::TEXT || '-' || region AS composite_key
  FROM employees
 WHERE first_name || ' ' || last_name LIKE 'John%';`
  );

  assertFormat('19.2 — ILIKE and SIMILAR TO',
    `select name, email from users where name ilike '%smith%' and email not ilike '%test%' and phone similar to '\\+1[0-9]{10}';`,
    `SELECT name, email
  FROM users
 WHERE name ILIKE '%smith%'
   AND email NOT ILIKE '%test%'
   AND phone SIMILAR TO '\\+1[0-9]{10}';`
  );

  assertFormat('19.3 — Regex operators (~, ~*, !~, !~*)',
    `select email, username from accounts where email ~ '^[a-z]+@example\\.com$' and username ~* '^admin' and notes !~ 'deprecated' and path !~* '^/test/';`,
    `SELECT email, username
  FROM accounts
 WHERE email ~ '^[a-z]+@example\\.com$'
   AND username ~* '^admin'
   AND notes !~ 'deprecated'
   AND path !~* '^/test/';`
  );

  assertFormat('19.4 — POSITION, OVERLAY, SUBSTRING, TRIM',
    `select position('@' in email) as at_pos, substring(phone from 1 for 3) as area_code, overlay(ssn placing '***' from 1 for 3) as masked_ssn, trim(both ' ' from name) as clean_name, trim(leading '0' from account_num) as trimmed_num from contacts;`,
    `SELECT POSITION('@' IN email) AS at_pos,
       SUBSTRING(phone FROM 1 FOR 3) AS area_code,
       OVERLAY(ssn PLACING '***' FROM 1 FOR 3) AS masked_ssn,
       TRIM(BOTH ' ' FROM name) AS clean_name,
       TRIM(LEADING '0' FROM account_num) AS trimmed_num
  FROM contacts;`
  );

  assertFormat('19.5 — TRIM shorthand forms',
    `select trim(name) as clean_name, trim(from name) as clean_name2, trim(leading from account_num) as clean_name3 from contacts;`,
    `SELECT TRIM(name) AS clean_name,
       TRIM(FROM name) AS clean_name2,
       TRIM(LEADING FROM account_num) AS clean_name3
  FROM contacts;`
  );
});

describe('Category 20: LATERAL Joins', () => {
  assertFormat('20.1 — LATERAL subquery',
    `select d.department_name, top_earner.name, top_earner.salary from departments as d left join lateral (select e.name, e.salary from employees as e where e.department_id = d.department_id order by e.salary desc limit 1) as top_earner on true;`,
    `SELECT d.department_name, top_earner.name, top_earner.salary
  FROM departments AS d
       LEFT JOIN LATERAL (SELECT e.name, e.salary
                            FROM employees AS e
                           WHERE e.department_id = d.department_id
                           ORDER BY e.salary DESC
                           LIMIT 1) AS top_earner
       ON TRUE;`
  );

  assertFormat('20.2 — LATERAL with set-returning function',
    `select u.user_name, g.month_start from users as u, lateral generate_series(u.created_at::date, current_date, '1 month'::interval) as g(month_start) where u.active = true;`,
    `SELECT u.user_name, g.month_start
  FROM users AS u,
       LATERAL GENERATE_SERIES(
           u.created_at::DATE, CURRENT_DATE, '1 month'::INTERVAL
       ) AS g(month_start)
 WHERE u.active = TRUE;`
  );
});

describe('Category 21: RETURNING Clause', () => {
  assertFormat('21.1 — INSERT ... RETURNING',
    `insert into audit_log (action, entity_id, created_at) values ('create', 42, now()) returning log_id, created_at;`,
    `   INSERT INTO audit_log (action, entity_id, created_at)
   VALUES ('create', 42, NOW())
RETURNING log_id, created_at;`
  );

  assertFormat('21.2 — UPDATE ... RETURNING',
    `update inventory set quantity = quantity - 1, updated_at = now() where product_id = 101 and quantity > 0 returning product_id, quantity as remaining;`,
    `   UPDATE inventory
      SET quantity = quantity - 1,
          updated_at = NOW()
    WHERE product_id = 101
      AND quantity > 0
RETURNING product_id, quantity AS remaining;`
  );

  assertFormat('21.3 — DELETE ... RETURNING',
    `delete from expired_sessions where expires_at < now() returning session_id, user_id;`,
    `   DELETE
     FROM expired_sessions
    WHERE expires_at < NOW()
RETURNING session_id, user_id;`
  );
});

describe('Category 22: ON CONFLICT (Upsert)', () => {
  assertFormat('22.1 — ON CONFLICT DO NOTHING',
    `insert into user_logins (user_id, login_date) values (42, current_date) on conflict (user_id, login_date) do nothing;`,
    `INSERT INTO user_logins (user_id, login_date)
VALUES (42, CURRENT_DATE)
    ON CONFLICT (user_id, login_date)
    DO NOTHING;`
  );

  assertFormat('22.2 — ON CONFLICT DO UPDATE (full upsert)',
    `insert into product_inventory (product_id, warehouse_id, quantity, updated_at) values (101, 5, 50, now()) on conflict (product_id, warehouse_id) do update set quantity = excluded.quantity, updated_at = excluded.updated_at where product_inventory.quantity <> excluded.quantity returning product_id, quantity;`,
    `   INSERT INTO product_inventory (product_id, warehouse_id, quantity, updated_at)
   VALUES (101, 5, 50, NOW())
       ON CONFLICT (product_id, warehouse_id)
       DO UPDATE
             SET quantity = excluded.quantity,
                 updated_at = excluded.updated_at
           WHERE product_inventory.quantity <> excluded.quantity
RETURNING product_id, quantity;`
  );

  assertFormat('22.3 — ON CONFLICT ON CONSTRAINT',
    `insert into subscriptions (user_id, plan_id, starts_at) values (7, 3, now()) on conflict on constraint subscriptions_user_id_key do update set plan_id = excluded.plan_id, starts_at = excluded.starts_at;`,
    `INSERT INTO subscriptions (user_id, plan_id, starts_at)
VALUES (7, 3, NOW())
    ON CONFLICT ON CONSTRAINT subscriptions_user_id_key
    DO UPDATE
          SET plan_id = excluded.plan_id,
              starts_at = excluded.starts_at;`
  );
});

describe('Category 23: GROUPING SETS, CUBE, ROLLUP', () => {
  assertFormat('23.1 — GROUPING SETS',
    `select region, department, sum(revenue) as total_revenue from sales group by grouping sets ((region, department), (region), (department), ());`,
    `SELECT region, department, SUM(revenue) AS total_revenue
  FROM sales
 GROUP BY GROUPING SETS (
              (region, department),
              (region),
              (department),
              ()
          );`
  );

  assertFormat('23.2 — ROLLUP',
    `select extract(year from order_date) as order_year, extract(quarter from order_date) as order_quarter, region, sum(amount) as total from orders group by rollup (extract(year from order_date), extract(quarter from order_date), region) order by order_year, order_quarter, region;`,
    `SELECT EXTRACT(YEAR FROM order_date) AS order_year,
       EXTRACT(QUARTER FROM order_date) AS order_quarter,
       region,
       SUM(amount) AS total
  FROM orders
 GROUP BY ROLLUP (
              EXTRACT(YEAR FROM order_date),
              EXTRACT(QUARTER FROM order_date),
              region
          )
 ORDER BY order_year, order_quarter, region;`
  );

  assertFormat('23.3 — CUBE with GROUPING function',
    `select region, product, sum(sales) as total, grouping(region) as is_region_total, grouping(product) as is_product_total from revenue group by cube (region, product) having sum(sales) > 10000 order by grouping(region), grouping(product), region, product;`,
    `SELECT region,
       product,
       SUM(sales) AS total,
       GROUPING(region) AS is_region_total,
       GROUPING(product) AS is_product_total
  FROM revenue
 GROUP BY CUBE (region, product)
HAVING SUM(sales) > 10000
 ORDER BY GROUPING(region), GROUPING(product), region, product;`
  );
});

describe('Category 24: Recursive CTEs', () => {
  assertFormat('24.1 — Recursive hierarchy traversal',
    `with recursive org_chart as (select employee_id, name, manager_id, 1 as depth from employees where manager_id is null union all select e.employee_id, e.name, e.manager_id, oc.depth + 1 from employees as e inner join org_chart as oc on e.manager_id = oc.employee_id) select employee_id, name, depth from org_chart order by depth, name;`,
    `  WITH RECURSIVE org_chart AS (
           SELECT employee_id, name, manager_id, 1 AS depth
             FROM employees
            WHERE manager_id IS NULL

            UNION ALL

           SELECT e.employee_id, e.name, e.manager_id, oc.depth + 1
             FROM employees AS e
                  INNER JOIN org_chart AS oc
                  ON e.manager_id = oc.employee_id
       )
SELECT employee_id, name, depth
  FROM org_chart
 ORDER BY depth, name;`
  );

  assertFormat('24.2 — Recursive path-building',
    `with recursive category_path as (select id, name, parent_id, name::text as path from categories where parent_id is null union all select c.id, c.name, c.parent_id, cp.path || ' > ' || c.name from categories as c inner join category_path as cp on c.parent_id = cp.id) select id, name, path from category_path order by path;`,
    `  WITH RECURSIVE category_path AS (
           SELECT id, name, parent_id, name::TEXT AS path
             FROM categories
            WHERE parent_id IS NULL

            UNION ALL

           SELECT c.id, c.name, c.parent_id,
                  cp.path || ' > ' || c.name
             FROM categories AS c
                  INNER JOIN category_path AS cp
                  ON c.parent_id = cp.id
       )
SELECT id, name, path
  FROM category_path
 ORDER BY path;`
  );
});

describe('Category 25: FILTER Clause on Aggregates', () => {
  assertFormat('25.1 — FILTER on COUNT/SUM',
    `select department, count(*) as total, count(*) filter (where status = 'active') as active_count, sum(salary) filter (where hire_date >= '2024-01-01') as new_hire_salary, avg(salary) filter (where title like '%Senior%') as senior_avg from employees group by department;`,
    `SELECT department,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE status = 'active') AS active_count,
       SUM(salary) FILTER (WHERE hire_date >= '2024-01-01') AS new_hire_salary,
       AVG(salary) FILTER (WHERE title LIKE '%Senior%') AS senior_avg
  FROM employees
 GROUP BY department;`
  );
});

describe('Category 26: Ordered-Set and Hypothetical Aggregates', () => {
  assertFormat('26.1 — PERCENTILE_CONT, PERCENTILE_DISC, MODE',
    `select department, percentile_cont(0.5) within group (order by salary) as median_salary, percentile_disc(0.9) within group (order by salary) as p90_salary, mode() within group (order by job_title) as most_common_title from employees group by department;`,
    `SELECT department,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary,
       PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY salary) AS p90_salary,
       MODE() WITHIN GROUP (ORDER BY job_title) AS most_common_title
  FROM employees
 GROUP BY department;`
  );
});

describe('Category 27: UPDATE with FROM and JOIN', () => {
  assertFormat('27.1 — UPDATE ... FROM',
    `update orders set status = 'shipped', shipped_date = s.ship_date from shipments as s where s.order_id = orders.order_id and s.carrier = 'FedEx';`,
    `UPDATE orders
   SET status = 'shipped',
       shipped_date = s.ship_date
  FROM shipments AS s
 WHERE s.order_id = orders.order_id
   AND s.carrier = 'FedEx';`
  );

  assertFormat('27.2 — UPDATE with subquery in SET',
    `update products set avg_rating = (select avg(rating)::numeric(3, 2) from reviews as r where r.product_id = products.product_id), review_count = (select count(*) from reviews as r where r.product_id = products.product_id) where exists (select 1 from reviews as r where r.product_id = products.product_id);`,
    `UPDATE products
   SET avg_rating = (SELECT AVG(rating)::NUMERIC(3, 2)
                       FROM reviews AS r
                      WHERE r.product_id = products.product_id),
       review_count = (SELECT COUNT(*)
                         FROM reviews AS r
                        WHERE r.product_id = products.product_id)
 WHERE EXISTS
       (SELECT 1
          FROM reviews AS r
         WHERE r.product_id = products.product_id);`
  );
});

describe('Category 28: NATURAL JOIN and JOIN ... USING', () => {
  assertFormat('28.1 — NATURAL JOIN',
    `select order_id, customer_name, product_name from orders natural join customers natural join products;`,
    `SELECT order_id, customer_name, product_name
  FROM orders
       NATURAL JOIN customers
       NATURAL JOIN products;`
  );

  assertFormat('28.2 — JOIN ... USING',
    `select o.order_id, c.name, p.product_name from orders as o inner join customers as c using (customer_id) inner join order_items as oi using (order_id) inner join products as p using (product_id) where o.order_date > '2024-01-01';`,
    `SELECT o.order_id, c.name, p.product_name
  FROM orders AS o
       INNER JOIN customers AS c
       USING (customer_id)

       INNER JOIN order_items AS oi
       USING (order_id)

       INNER JOIN products AS p
       USING (product_id)
 WHERE o.order_date > '2024-01-01';`
  );
});

describe('Category 29: GENERATE_SERIES and Table-Generating Functions', () => {
  assertFormat('29.1 — generate_series for date spine',
    `select d.date::date as calendar_date, coalesce(o.order_count, 0) as order_count from generate_series('2024-01-01'::date, '2024-12-31'::date, '1 day'::interval) as d(date) left join (select order_date::date as order_date, count(*) as order_count from orders group by order_date::date) as o on d.date::date = o.order_date;`,
    `SELECT d.date::DATE AS calendar_date,
       COALESCE(o.order_count, 0) AS order_count
  FROM GENERATE_SERIES(
           '2024-01-01'::DATE,
           '2024-12-31'::DATE,
           '1 day'::INTERVAL
       ) AS d(date)
       LEFT JOIN (SELECT order_date::DATE AS order_date,
                         COUNT(*) AS order_count
                    FROM orders
                   GROUP BY order_date::DATE) AS o
       ON d.date::DATE = o.order_date;`
  );
});

describe('Category 30: INTERVAL Arithmetic', () => {
  assertFormat('30.1 — Interval expressions',
    `select user_id, last_login, now() - last_login as time_since_login, created_at + interval '30 days' as trial_end, created_at + (term_months || ' months')::interval as contract_end from users where last_login < now() - interval '90 days' and created_at > now() - interval '1 year';`,
    `SELECT user_id,
       last_login,
       NOW() - last_login AS time_since_login,
       created_at + INTERVAL '30 days' AS trial_end,
       created_at + (term_months || ' months')::INTERVAL AS contract_end
  FROM users
 WHERE last_login < NOW() - INTERVAL '90 days'
   AND created_at > NOW() - INTERVAL '1 year';`
  );
});

describe('Category 31: EXTRACT, DATE_PART, DATE_TRUNC, AGE', () => {
  assertFormat('31.1 — Date/time extraction functions',
    `select order_id, extract(year from order_date) as order_year, extract(dow from order_date) as day_of_week, date_part('quarter', order_date) as quarter, date_trunc('month', order_date) as month_start, age(now(), order_date) as order_age from orders where date_trunc('year', order_date) = date_trunc('year', now());`,
    `SELECT order_id,
       EXTRACT(YEAR FROM order_date) AS order_year,
       EXTRACT(DOW FROM order_date) AS day_of_week,
       DATE_PART('quarter', order_date) AS quarter,
       DATE_TRUNC('month', order_date) AS month_start,
       AGE(NOW(), order_date) AS order_age
  FROM orders
 WHERE DATE_TRUNC('year', order_date) = DATE_TRUNC('year', NOW());`
  );
});

describe('Category 32: IS DISTINCT FROM', () => {
  assertFormat('32.1 — IS DISTINCT FROM / IS NOT DISTINCT FROM',
    `select a.id, a.value, b.value from table_a as a inner join table_b as b on a.id = b.id where a.status is distinct from b.status and a.category is not distinct from b.category;`,
    `SELECT a.id, a.value, b.value
  FROM table_a AS a
       INNER JOIN table_b AS b
       ON a.id = b.id
 WHERE a.status IS DISTINCT FROM b.status
   AND a.category IS NOT DISTINCT FROM b.category;`
  );
});

describe('Category 33: GREATEST, LEAST', () => {
  assertFormat('33.1 — GREATEST and LEAST',
    `select product_name, greatest(online_price, store_price, wholesale_price) as max_price, least(online_price, store_price, wholesale_price) as min_price, greatest(online_price, store_price) - least(online_price, store_price) as price_spread from products where greatest(online_price, store_price) > 100;`,
    `SELECT product_name,
       GREATEST(online_price, store_price, wholesale_price) AS max_price,
       LEAST(online_price, store_price, wholesale_price) AS min_price,
       GREATEST(online_price, store_price) - LEAST(online_price, store_price) AS price_spread
  FROM products
 WHERE GREATEST(online_price, store_price) > 100;`
  );
});

describe('Category 34: STRING_AGG', () => {
  assertFormat('34.1 — STRING_AGG with ORDER BY and SEPARATOR',
    `select department, string_agg(employee_name, ', ' order by employee_name) as employee_list, string_agg(distinct job_title, '; ' order by job_title) as titles from employees group by department having count(*) > 3;`,
    `SELECT department,
       STRING_AGG(employee_name, ', ' ORDER BY employee_name) AS employee_list,
       STRING_AGG(DISTINCT job_title, '; ' ORDER BY job_title) AS titles
  FROM employees
 GROUP BY department
HAVING COUNT(*) > 3;`
  );
});

describe('Category 35: VALUES as Standalone Query', () => {
  assertFormat('35.1 — Standalone VALUES',
    `values (1, 'alpha', true), (2, 'beta', false), (3, 'gamma', true);`,
    `VALUES (1, 'alpha', TRUE),
       (2, 'beta', FALSE),
       (3, 'gamma', TRUE);`
  );
});

describe('Category 36: CREATE INDEX, CREATE VIEW', () => {
  assertFormat('36.1 — CREATE INDEX',
    `create index concurrently if not exists idx_orders_customer_date on orders using btree (customer_id, order_date desc) where status <> 'cancelled';`,
    `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_customer_date
   ON orders
USING BTREE (customer_id, order_date DESC)
WHERE status <> 'cancelled';`
  );

  assertFormat('36.2 — CREATE INDEX with expression',
    `create unique index idx_users_lower_email on users (lower(email)) where deleted_at is null;`,
    `CREATE UNIQUE INDEX idx_users_lower_email
   ON users (LOWER(email))
WHERE deleted_at IS NULL;`
  );

  assertFormat('36.3 — CREATE VIEW',
    `create or replace view active_employee_summary as select d.department_name, count(*) as headcount, avg(e.salary)::numeric(10, 2) as avg_salary from employees as e inner join departments as d on e.department_id = d.department_id where e.termination_date is null group by d.department_name;`,
    `CREATE OR REPLACE VIEW active_employee_summary AS
SELECT d.department_name,
       COUNT(*) AS headcount,
       AVG(e.salary)::NUMERIC(10, 2) AS avg_salary
  FROM employees AS e
       INNER JOIN departments AS d
       ON e.department_id = d.department_id
 WHERE e.termination_date IS NULL
 GROUP BY d.department_name;`
  );

  assertFormat('36.4 — CREATE MATERIALIZED VIEW',
    `create materialized view if not exists monthly_revenue as select date_trunc('month', order_date)::date as month, sum(amount) as total, count(*) as order_count from orders where status = 'completed' group by date_trunc('month', order_date)::date with data;`,
    `CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_revenue AS
SELECT DATE_TRUNC('month', order_date)::DATE AS month,
       SUM(amount) AS total,
       COUNT(*) AS order_count
  FROM orders
 WHERE status = 'completed'
 GROUP BY DATE_TRUNC('month', order_date)::DATE
  WITH DATA;`
  );
});

describe('Category 37: GRANT, REVOKE, TRUNCATE', () => {
  const tests: [string, string, string][] = [
    ['37.1 — GRANT',
      `grant select, insert, update on all tables in schema public to app_readwrite;`,
      `GRANT SELECT, INSERT, UPDATE\n   ON ALL TABLES IN SCHEMA public\n   TO app_readwrite;`],
    ['37.2 — TRUNCATE',
      `truncate table staging_events, staging_users restart identity cascade;`,
      `TRUNCATE TABLE staging_events, staging_users\nRESTART IDENTITY CASCADE;`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
});

describe('Category 38: Sequences', () => {
  const tests: [string, string, string][] = [
    ['38.1 — NEXTVAL, CURRVAL, SETVAL',
      `select nextval('invoice_id_seq') as next_id;`,
      `SELECT NEXTVAL('invoice_id_seq') AS next_id;`],
    ['38.2 — INSERT with sequence',
      `insert into invoices (invoice_id, customer_id, amount) values (nextval('invoice_id_seq'), 42, 500.00) returning invoice_id;`,
      `   INSERT INTO invoices (invoice_id, customer_id, amount)\n   VALUES (NEXTVAL('invoice_id_seq'), 42, 500.00)\nRETURNING invoice_id;`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
});

describe('Category 39: Advanced Window Functions', () => {
  assertFormat('39.1 — LAG, LEAD with defaults',
    `select order_date, revenue, lag(revenue, 1, 0) over (order by order_date) as prev_revenue, lead(revenue, 1, 0) over (order by order_date) as next_revenue, revenue - lag(revenue, 1, 0) over (order by order_date) as revenue_change from daily_revenue;`,
    `SELECT order_date,
       revenue,
       LAG(revenue, 1, 0) OVER (ORDER BY order_date) AS prev_revenue,
       LEAD(revenue, 1, 0) OVER (ORDER BY order_date) AS next_revenue,
       revenue - LAG(revenue, 1, 0) OVER (ORDER BY order_date) AS revenue_change
  FROM daily_revenue;`
  );

  assertFormat('39.2 — NTILE, CUME_DIST, PERCENT_RANK',
    `select employee_name, salary, ntile(4) over (order by salary) as quartile, cume_dist() over (order by salary) as cumulative_dist, percent_rank() over (order by salary) as pct_rank from employees where department = 'Engineering';`,
    `SELECT employee_name,
       salary,
       NTILE(4) OVER (ORDER BY salary) AS quartile,
       CUME_DIST() OVER (ORDER BY salary) AS cumulative_dist,
       PERCENT_RANK() OVER (ORDER BY salary) AS pct_rank
  FROM employees
 WHERE department = 'Engineering';`
  );

  assertFormat('39.3 — WINDOW clause (named windows)',
    `select employee_name, department, salary, avg(salary) over dept_window as dept_avg, rank() over salary_window as salary_rank from employees window dept_window as (partition by department), salary_window as (order by salary desc);`,
    `SELECT employee_name,
       department,
       salary,
       AVG(salary) OVER dept_window AS dept_avg,
       RANK() OVER salary_window AS salary_rank
  FROM employees
WINDOW dept_window AS (PARTITION BY department),
       salary_window AS (ORDER BY salary DESC);`
  );

  assertFormat('39.4 — Window frame with EXCLUDE',
    `select measurement_date, value, avg(value) over (order by measurement_date rows between 3 preceding and 3 following exclude current row) as smoothed from sensor_data;`,
    `SELECT measurement_date,
       value,
       AVG(value) OVER (ORDER BY measurement_date
                         ROWS BETWEEN 3 PRECEDING
                                  AND 3 FOLLOWING
                         EXCLUDE CURRENT ROW) AS smoothed
  FROM sensor_data;`
  );

  assertFormat('39.5 — RANGE frame with interval',
    `select event_date, event_count, sum(event_count) over (order by event_date range between interval '7 days' preceding and current row) as rolling_7d from daily_events;`,
    `SELECT event_date,
       event_count,
       SUM(event_count) OVER (ORDER BY event_date
                               RANGE BETWEEN INTERVAL '7 days' PRECEDING
                                         AND CURRENT ROW) AS rolling_7d
  FROM daily_events;`
  );
});

describe('Category 40: MERGE Statement', () => {
  assertFormat('40.1 — MERGE with all clauses',
    `merge into inventory as t using incoming_stock as s on t.product_id = s.product_id and t.warehouse_id = s.warehouse_id when matched and s.quantity = 0 then delete when matched then update set quantity = t.quantity + s.quantity, updated_at = now() when not matched then insert (product_id, warehouse_id, quantity, updated_at) values (s.product_id, s.warehouse_id, s.quantity, now());`,
    ` MERGE INTO inventory AS t
 USING incoming_stock AS s
    ON t.product_id = s.product_id
   AND t.warehouse_id = s.warehouse_id
  WHEN MATCHED AND s.quantity = 0 THEN
       DELETE
  WHEN MATCHED THEN
       UPDATE
          SET quantity = t.quantity + s.quantity,
              updated_at = NOW()
  WHEN NOT MATCHED THEN
       INSERT (product_id, warehouse_id, quantity, updated_at)
       VALUES (s.product_id, s.warehouse_id, s.quantity, NOW());`
  );

  assertFormat('40.2 — MERGE river alignment — keywords right-align to river',
    `merge into target_table as t using source_table as s on t.id = s.id when matched then update set name = s.name when not matched then insert (id, name) values (s.id, s.name);`,
    ` MERGE INTO target_table AS t
 USING source_table AS s
    ON t.id = s.id
  WHEN MATCHED THEN
       UPDATE
          SET name = s.name
  WHEN NOT MATCHED THEN
       INSERT (id, name)
       VALUES (s.id, s.name);`
  );

  assertFormat('40.3 — MERGE with delete-only (no VALUES, smaller river)',
    `merge into target as t using source as s on t.id = s.id when matched then delete;`,
    `MERGE INTO target AS t
USING source AS s
   ON t.id = s.id
 WHEN MATCHED THEN
      DELETE;`
  );
});

describe('Category 40b: CREATE INDEX River Alignment', () => {
  assertFormat('40b.1 — CREATE INDEX with USING and WHERE — keywords right-align to river',
    `create index idx_test on my_table using gin (data) where active = true;`,
    `CREATE INDEX idx_test
   ON my_table
USING GIN (data)
WHERE active = TRUE;`
  );

  assertFormat('40b.2 — CREATE INDEX without USING — ON and WHERE aligned',
    `create index idx_simple on my_table (col1, col2) where status = 'active';`,
    `CREATE INDEX idx_simple
   ON my_table (col1, col2)
WHERE status = 'active';`
  );

  assertFormat('40b.3 — CREATE INDEX without WHERE — minimal river',
    `create unique index idx_email on users (email);`,
    `CREATE UNIQUE INDEX idx_email
ON users (email);`
  );
});

describe('Category 41: Complex Parenthesized Boolean Logic', () => {
  assertFormat('41.1 — Nested AND/OR with parentheses',
    `select * from orders where (status = 'pending' and (priority = 'high' or (priority = 'medium' and created_at < now() - interval '7 days'))) or (status = 'review' and assigned_to is not null);`,
    `SELECT *
  FROM orders
 WHERE (status = 'pending'
        AND (priority = 'high'
             OR (priority = 'medium'
                 AND created_at < NOW() - INTERVAL '7 days')))
    OR (status = 'review'
        AND assigned_to IS NOT NULL);`
  );
});

describe('Category 42: Multiple CTEs with MATERIALIZED Hints', () => {
  assertFormat('42.1 — MATERIALIZED / NOT MATERIALIZED CTEs',
    `with active_users as materialized (select user_id, email from users where status = 'active'), recent_orders as not materialized (select order_id, user_id, amount from orders where order_date > now() - interval '30 days') select au.email, count(ro.order_id) as order_count, sum(ro.amount) as total_spent from active_users as au left join recent_orders as ro on au.user_id = ro.user_id group by au.email having sum(ro.amount) > 100;`,
    `  WITH active_users AS MATERIALIZED (
           SELECT user_id, email
             FROM users
            WHERE status = 'active'
       ),
       recent_orders AS NOT MATERIALIZED (
           SELECT order_id, user_id, amount
             FROM orders
            WHERE order_date > NOW() - INTERVAL '30 days'
       )
SELECT au.email,
       COUNT(ro.order_id) AS order_count,
       SUM(ro.amount) AS total_spent
  FROM active_users AS au
       LEFT JOIN recent_orders AS ro
       ON au.user_id = ro.user_id
 GROUP BY au.email
HAVING SUM(ro.amount) > 100;`
  );
});

describe('Category 43: Self-Joins', () => {
  assertFormat('43.1 — Self-join for hierarchy',
    `select e.name as employee, m.name as manager from employees as e left join employees as m on e.manager_id = m.employee_id where e.department = 'Engineering';`,
    `SELECT e.name AS employee, m.name AS manager
  FROM employees AS e
       LEFT JOIN employees AS m
       ON e.manager_id = m.employee_id
 WHERE e.department = 'Engineering';`
  );
});

describe('Category 44: Correlated Subqueries', () => {
  assertFormat('44.1 — Correlated UPDATE subquery',
    `select e.employee_name, e.salary, (select avg(e2.salary) from employees as e2 where e2.department_id = e.department_id) as dept_avg, e.salary - (select avg(e2.salary) from employees as e2 where e2.department_id = e.department_id) as diff_from_avg from employees as e where e.salary > (select avg(e2.salary) * 1.5 from employees as e2 where e2.department_id = e.department_id) order by diff_from_avg desc;`,
    `SELECT e.employee_name,
       e.salary,
       (SELECT AVG(e2.salary)
          FROM employees AS e2
         WHERE e2.department_id = e.department_id) AS dept_avg,
       e.salary - (SELECT AVG(e2.salary)
                      FROM employees AS e2
                     WHERE e2.department_id = e.department_id) AS diff_from_avg
  FROM employees AS e
 WHERE e.salary >
       (SELECT AVG(e2.salary) * 1.5
          FROM employees AS e2
         WHERE e2.department_id = e.department_id)
 ORDER BY diff_from_avg DESC;`
  );
});

describe('Category 45: FETCH FIRST', () => {
  const tests: [string, string, string][] = [
    ['45.1 — FETCH FIRST N ROWS',
      `select employee_name, salary from employees order by salary desc offset 10 rows fetch first 5 rows only;`,
      `SELECT employee_name, salary\n  FROM employees\n ORDER BY salary DESC\nOFFSET 10 ROWS\n FETCH FIRST 5 ROWS ONLY;`],
    ['45.2 — FETCH with ties',
      `select employee_name, salary from employees order by salary desc fetch first 10 rows with ties;`,
      `SELECT employee_name, salary\n  FROM employees\n ORDER BY salary DESC\n FETCH FIRST 10 ROWS WITH TIES;`],
  ];
  for (const [name, input, expected] of tests) assertFormat(name, input, expected);
});

describe('Category 46: TABLESAMPLE', () => {
  assertFormat('46.1 — TABLESAMPLE BERNOULLI',
    `select * from large_events tablesample bernoulli(10) repeatable(42) where event_type = 'click';`,
    `SELECT *
  FROM large_events TABLESAMPLE BERNOULLI(10) REPEATABLE(42)
 WHERE event_type = 'click';`
  );
});

describe('Category 47: Complex Expressions and Operator Precedence', () => {
  assertFormat('47.1 — Mixed arithmetic with function calls',
    `select product_name, (unit_price * (1 - discount / 100.0))::numeric(10, 2) * quantity as line_total, round(((unit_price * quantity) - coalesce(credit, 0)) * (1 + tax_rate / 100.0), 2) as total_with_tax from order_lines where (unit_price * quantity) > 0 and coalesce(discount, 0) between 0 and 50;`,
    `SELECT product_name,
       (unit_price * (1 - discount / 100.0))::NUMERIC(10, 2) * quantity AS line_total,
       ROUND(((unit_price * quantity) - COALESCE(credit, 0)) * (1 + tax_rate / 100.0), 2) AS total_with_tax
  FROM order_lines
 WHERE (unit_price * quantity) > 0
   AND COALESCE(discount, 0) BETWEEN 0 AND 50;`
  );

  assertFormat('47.2 — Bitwise operators',
    `select id, permissions, permissions & 4 as has_read, permissions | 2 as with_write, permissions # 255 as inverted, ~permissions as bitwise_not, permissions << 1 as shifted from user_permissions where permissions & 4 = 4;`,
    `SELECT id,
       permissions,
       permissions & 4 AS has_read,
       permissions | 2 AS with_write,
       permissions # 255 AS inverted,
       ~permissions AS bitwise_not,
       permissions << 1 AS shifted
  FROM user_permissions
 WHERE permissions & 4 = 4;`
  );
});

describe('Category 48: Multiple Statements in Sequence', () => {
  assertFormat('48.1 — DDL + DML sequence',
    `create table if not exists temp_results (id serial primary key, label varchar(50) not null, value numeric(10, 2) default 0.00 not null); insert into temp_results (label, value) select category, sum(amount) from transactions group by category; update temp_results set label = upper(label) where value > 1000; select * from temp_results order by value desc;`,
    `CREATE TABLE IF NOT EXISTS temp_results (
    id    SERIAL       PRIMARY KEY,
    label VARCHAR(50)  NOT NULL,
    value NUMERIC(10, 2) DEFAULT 0.00 NOT NULL
);

INSERT INTO temp_results (label, value)
SELECT category, SUM(amount)
  FROM transactions
 GROUP BY category;

UPDATE temp_results
   SET label = UPPER(label)
 WHERE value > 1000;

SELECT *
  FROM temp_results
 ORDER BY value DESC;`
  );
});

describe('Category 49: Stress Tests — Operator Ambiguity', () => {
  assertFormat('49.1 — Operators that look like keywords or other operators',
    `select j.data -> 'key' ->> 'sub' as text_val, j.data::text as json_text, j.data @> '{"a":1}'::jsonb as contains_a, j.tags && array['x'] as has_x, j.perms & 7 as low_bits, j.name || ':' || j.id::text as composite, j.score between 1 and 10 as in_range from jsonb_table as j where j.data ? 'key' and j.data ->> 'type' in ('a', 'b') and j.active is not distinct from true;`,
    `SELECT j.data -> 'key' ->> 'sub' AS text_val,
       j.data::TEXT AS json_text,
       j.data @> '{"a":1}'::JSONB AS contains_a,
       j.tags && ARRAY['x'] AS has_x,
       j.perms & 7 AS low_bits,
       j.name || ':' || j.id::TEXT AS composite,
       j.score BETWEEN 1 AND 10 AS in_range
  FROM jsonb_table AS j
 WHERE j.data ? 'key'
   AND j.data ->> 'type' IN ('a', 'b')
   AND j.active IS NOT DISTINCT FROM TRUE;`
  );

  assertFormat('49.2 — Cast ambiguity: :: vs comparison chains',
    `select a::integer + b::integer as sum_ints, (a || b)::varchar(100) as concat_cast, array[1,2,3]::integer[] as int_array, row(1, 'a')::my_type as typed_row, null::bigint as null_bigint from expressions where a::numeric > b::numeric and c::text <> '' and d::boolean is true;`,
    `SELECT a::INTEGER + b::INTEGER AS sum_ints,
       (a || b)::VARCHAR(100) AS concat_cast,
       ARRAY[1, 2, 3]::INTEGER[] AS int_array,
       ROW(1, 'a')::my_type AS typed_row,
       NULL::BIGINT AS null_bigint
  FROM expressions
 WHERE a::NUMERIC > b::NUMERIC
   AND c::TEXT <> ''
   AND d::BOOLEAN IS TRUE;`
  );
});

describe('Category 50: Full Kitchen-Sink Query', () => {
  assertFormat('50.1 — Maximum complexity: CTE + recursive + window + JSON + cast + LATERAL + FILTER + CASE',
    `with recursive category_tree as (select id, name, parent_id, 1 as depth, array[id] as path from categories where parent_id is null union all select c.id, c.name, c.parent_id, ct.depth + 1, ct.path || c.id from categories as c inner join category_tree as ct on c.parent_id = ct.id where ct.depth < 10), category_stats as materialized (select ct.id, ct.name, ct.depth, ct.path, count(*) filter (where p.status = 'active') as active_products, count(*) filter (where p.status = 'discontinued') as discontinued_products, avg(p.price::numeric)::numeric(10, 2) as avg_price, jsonb_build_object('min', min(p.price), 'max', max(p.price), 'median', percentile_cont(0.5) within group (order by p.price)) as price_stats from category_tree as ct left join products as p on p.category_id = ct.id group by ct.id, ct.name, ct.depth, ct.path) select cs.name, cs.depth, cs.active_products, cs.avg_price, cs.price_stats ->> 'median' as median_price, case when cs.active_products > 100 then 'large' when cs.active_products > 10 then 'medium' else 'small' end as size_tier, row_number() over (partition by cs.depth order by cs.active_products desc) as rank_in_depth, recent.latest_product, recent.latest_date from category_stats as cs left join lateral (select p.name as latest_product, p.created_at::date as latest_date from products as p where p.category_id = cs.id and p.status = 'active' order by p.created_at desc limit 1) as recent on true where cs.active_products > 0 and cs.price_stats ->> 'max' is not null order by cs.depth, cs.active_products desc;`,
    `  WITH RECURSIVE category_tree AS (
           SELECT id, name, parent_id, 1 AS depth, ARRAY[id] AS path
             FROM categories
            WHERE parent_id IS NULL

            UNION ALL

           SELECT c.id, c.name, c.parent_id,
                  ct.depth + 1,
                  ct.path || c.id
             FROM categories AS c
                  INNER JOIN category_tree AS ct
                  ON c.parent_id = ct.id
            WHERE ct.depth < 10
       ),
       category_stats AS MATERIALIZED (
           SELECT ct.id,
                  ct.name,
                  ct.depth,
                  ct.path,
                  COUNT(*) FILTER (WHERE p.status = 'active') AS active_products,
                  COUNT(*) FILTER (WHERE p.status = 'discontinued') AS discontinued_products,
                  AVG(p.price::NUMERIC)::NUMERIC(10, 2) AS avg_price,
                  JSONB_BUILD_OBJECT(
                      'min', MIN(p.price),
                      'max', MAX(p.price),
                      'median', PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.price)
                  ) AS price_stats
             FROM category_tree AS ct
                  LEFT JOIN products AS p
                  ON p.category_id = ct.id
            GROUP BY ct.id, ct.name, ct.depth, ct.path
       )
SELECT cs.name,
       cs.depth,
       cs.active_products,
       cs.avg_price,
       cs.price_stats ->> 'median' AS median_price,
       CASE
       WHEN cs.active_products > 100 THEN 'large'
       WHEN cs.active_products > 10 THEN 'medium'
       ELSE 'small'
       END AS size_tier,
       ROW_NUMBER() OVER (PARTITION BY cs.depth
                              ORDER BY cs.active_products DESC) AS rank_in_depth,
       recent.latest_product,
       recent.latest_date
  FROM category_stats AS cs
       LEFT JOIN LATERAL (SELECT p.name AS latest_product,
                                 p.created_at::DATE AS latest_date
                            FROM products AS p
                           WHERE p.category_id = cs.id
                             AND p.status = 'active'
                           ORDER BY p.created_at DESC
                           LIMIT 1) AS recent
       ON TRUE
 WHERE cs.active_products > 0
   AND cs.price_stats ->> 'max' IS NOT NULL
 ORDER BY cs.depth, cs.active_products DESC;`
  );
});

describe('INTERVAL expressions', () => {
  it('formats INTERVAL with arithmetic', () => {
    const out = formatSQL("SELECT INTERVAL '1 day' * 5 FROM t;");
    expect(out).toContain("INTERVAL '1 day'");
    expect(out).toContain('* 5');
  });

  it('formats INTERVAL with TO clause', () => {
    const out = formatSQL("SELECT INTERVAL '1' HOUR TO SECOND FROM t;");
    expect(out).toContain('INTERVAL');
    expect(out).toContain('HOUR TO SECOND');
  });

  it('formats INTERVAL in date arithmetic', () => {
    const out = formatSQL("SELECT NOW() + INTERVAL '3 hours' FROM t;");
    expect(out).toContain("INTERVAL '3 hours'");
  });
});

describe('Window frame with quoted string containing AND', () => {
  it('handles RANGE BETWEEN INTERVAL containing AND in quotes', () => {
    const sql = `SELECT value, SUM(value) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1 AND 2' PRECEDING AND CURRENT ROW) AS rolling FROM sensor_data;`;
    const formatted = formatSQL(sql);
    expect(formatted).toContain("INTERVAL '1 AND 2' PRECEDING");
    expect(formatted).toContain('AND CURRENT ROW');
  });

  it('handles ROWS BETWEEN with quoted text containing AND', () => {
    const sql = `SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) FROM t;`;
    const formatted = formatSQL(sql);
    expect(formatted).toContain('ROWS BETWEEN');
    expect(formatted).toContain('PRECEDING');
    expect(formatted).toContain('CURRENT ROW');
  });
});

describe('Formatter depth limit', () => {
  it('does not crash on 200+ level deeply nested expression', () => {
    // Build a deeply nested paren expression beyond MAX_FORMATTER_DEPTH=200
    // We keep it within parser max depth by using AND chains (not nested parens)
    const conditions: string[] = [];
    for (let i = 0; i < 250; i++) {
      conditions.push('x = 1');
    }
    const sql = 'SELECT 1 FROM t WHERE ' + conditions.join(' AND ') + ';';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
    expect(result).toContain('WHERE');
  });

  it('falls back gracefully at max formatter depth', () => {
    // Build nested CASE expressions that stress the formatter depth
    const depth = 80;
    let expr = '1';
    for (let i = 0; i < depth; i++) {
      expr = `CASE WHEN a = ${i} THEN ${expr} ELSE 0 END`;
    }
    const sql = `SELECT ${expr} FROM t;`;
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
    expect(result).toContain('CASE');
  });

  it('emits fallback marker instead of throwing MaxDepthError', () => {
    const makeSelect = (expr: AST.Expression): AST.SelectStatement => ({
      type: 'select',
      distinct: false,
      columns: [{ expr }],
      joins: [],
      leadingComments: [],
    });

    let expr: AST.Expression = { type: 'literal', value: '1', literalType: 'number' };
    for (let i = 0; i < 220; i++) {
      expr = {
        type: 'subquery',
        query: makeSelect(expr),
      };
    }

    const out = formatStatements([makeSelect(expr)]);
    expect(out).toContain('/* depth exceeded */');
  });
});

describe('Comment preservation regressions', () => {
  it('preserves trailing JOIN comments without splitting WHERE into raw SQL', () => {
    const sql = 'SELECT id FROM a JOIN b ON a.id = b.id -- join comment\\nWHERE a.id > 0;';
    const out = formatSQL(sql);
    expect(out).toContain('-- join comment');
    expect(out).toContain('WHERE a.id > 0;');
    expect(out).not.toContain('\n;\n');
  });

  it('preserves trailing ORDER BY comments without emitting extra statements', () => {
    const sql = 'SELECT id FROM t ORDER BY id, -- sort 1\\ncreated_at DESC -- sort 2\\n;';
    const out = formatSQL(sql);
    expect(out).toContain('-- sort 1');
    expect(out).toContain('-- sort 2');
    expect(out).not.toContain('\n;\n');
  });

  it('preserves standalone trailing comments between statements', () => {
    const sql = 'SELECT 1; -- trailing note';
    const out = formatSQL(sql);
    expect(out).toContain('SELECT 1;');
    expect(out).toContain('-- trailing note');
  });
});

describe('CJK width-aware layout', () => {
  it('wraps wide-character SELECT lists using display-width heuristics', () => {
    const sql = 'SELECT 顾客编号 AS 顾客编号列, 订单编号 AS 订单编号列, 商品编号 AS 商品编号列, 发货编号 AS 发货编号列 FROM 销售记录;';
    const out = formatSQL(sql);
    expect(out).toContain('SELECT 顾客编号 AS 顾客编号列,');
    expect(out).toContain('\n       订单编号 AS 订单编号列,');
    expect(out).toContain('\n  FROM 销售记录;');
  });
});

describe('Complex CASE with nested subqueries', () => {
  it('formats CASE with subquery in WHEN condition', () => {
    const sql = `SELECT CASE WHEN x > (SELECT AVG(y) FROM s) THEN 'above' ELSE 'below' END AS status FROM t;`;
    const out = formatSQL(sql);
    expect(out).toContain('CASE');
    expect(out).toContain('SELECT AVG(y)');
    expect(out).toContain('END AS status');
  });

  it('formats CASE with subquery in THEN result', () => {
    const sql = `SELECT CASE WHEN active = true THEN (SELECT COUNT(*) FROM orders WHERE user_id = u.id) ELSE 0 END AS order_count FROM users AS u;`;
    const out = formatSQL(sql);
    expect(out).toContain('CASE');
    expect(out).toContain('SELECT COUNT(*)');
    expect(out).toContain('END AS order_count');
  });

  it('formats nested CASE inside CASE', () => {
    const sql = `SELECT CASE category WHEN 'A' THEN CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END WHEN 'B' THEN 'category_b' ELSE 'other' END AS label FROM products;`;
    const out = formatSQL(sql);
    expect(out).toContain('CASE category');
    expect(out).toContain("WHEN 'A'");
    expect(out).toContain("WHEN 'B'");
    expect(out).toContain('END AS label');
  });
});

describe('CTE with column list', () => {
  assertFormat('CTE with column list',
    `WITH cte (id, name) AS (SELECT 1, 'Alice') SELECT * FROM cte;`,
    `  WITH cte (id, name) AS (
           SELECT 1, 'Alice'
       )
SELECT *
  FROM cte;`
  );

  assertFormat('CTE with column list and materialized',
    `WITH cte (revenue_date, amount) AS MATERIALIZED (SELECT order_date, sum(total) FROM orders GROUP BY order_date) SELECT * FROM cte;`,
    `  WITH cte (revenue_date, amount) AS MATERIALIZED (
           SELECT order_date, SUM(total)
             FROM orders
            GROUP BY order_date
       )
SELECT *
  FROM cte;`
  );
});

describe('IN list wrapping', () => {
  assertFormat('Long IN list wraps at 80 columns',
    `SELECT * FROM items WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);`,
    `SELECT *
  FROM items
 WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
              19, 20);`
  );

  assertFormat('Short IN list stays inline',
    `SELECT * FROM items WHERE id IN (1, 2, 3);`,
    `SELECT *
  FROM items
 WHERE id IN (1, 2, 3);`
  );

  assertFormat('Long string IN list wraps',
    `SELECT * FROM users WHERE status IN ('active', 'pending', 'suspended', 'deactivated', 'banned', 'review');`,
    `SELECT *
  FROM users
 WHERE status IN ('active', 'pending', 'suspended', 'deactivated', 'banned',
                  'review');`
  );
});

describe('OVER clause wrapping', () => {
  assertFormat('Long OVER with PARTITION BY wraps',
    `SELECT ROW_NUMBER() OVER (PARTITION BY department, region, division, category ORDER BY salary DESC) FROM employees;`,
    `SELECT ROW_NUMBER() OVER (PARTITION BY department, region, division, category
                              ORDER BY salary DESC)
  FROM employees;`
  );
});

describe('Array constructor wrapping', () => {
  assertFormat('Long ARRAY constructor wraps',
    `SELECT ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] AS big_array FROM t;`,
    `SELECT ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
             20] AS big_array
  FROM t;`
  );

  assertFormat('Short ARRAY stays inline',
    `SELECT ARRAY[1, 2, 3] AS arr FROM t;`,
    `SELECT ARRAY[1, 2, 3] AS arr
  FROM t;`
  );
});

describe('formatJoinOn recursion fix', () => {
  assertFormat('Deeply nested AND in JOIN ON chains properly',
    `SELECT * FROM a JOIN b ON a.id = b.id AND a.x = b.x AND a.y = b.y AND a.z = b.z;`,
    `SELECT *
  FROM a
  JOIN b
    ON a.id = b.id
       AND a.x = b.x
       AND a.y = b.y
       AND a.z = b.z;`
  );
});

describe('formatExpr depth tracking', () => {
  it('handles deeply nested binary expressions without stack overflow', () => {
    // Build a deeply nested binary expression
    let expr = 'a = 1';
    for (let i = 0; i < 180; i++) {
      expr = `(${expr}) + 1`;
    }
    const sql = `SELECT ${expr} FROM t;`;
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
  });
});

describe('InExpr discriminated union', () => {
  it('handles IN with value list correctly', () => {
    const sql = `SELECT * FROM t WHERE x IN (1, 2, 3);`;
    const out = formatSQL(sql);
    expect(out).toContain('IN (1, 2, 3)');
  });

  it('handles IN with subquery correctly', () => {
    const sql = `SELECT * FROM t WHERE x IN (SELECT id FROM s);`;
    const out = formatSQL(sql);
    expect(out).toContain('IN');
    expect(out).toContain('SELECT id');
  });

  it('handles NOT IN with value list', () => {
    const sql = `SELECT * FROM t WHERE x NOT IN (1, 2, 3);`;
    const out = formatSQL(sql);
    expect(out).toContain('NOT IN (1, 2, 3)');
  });
});

describe('mixed new features end-to-end', () => {
  it('formats CTE with column list + INTERVAL + GROUPS window + IN list in one query', () => {
    const sql = `WITH revenue (dt, amount) AS (SELECT order_date, SUM(total) FROM orders GROUP BY order_date) SELECT dt, amount, SUM(amount) OVER (ORDER BY dt GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling, dt + INTERVAL '30' DAY AS deadline FROM revenue WHERE amount IN (100, 200, 300, 400, 500);`;
    const out = formatSQL(sql);
    expect(out).toContain('WITH revenue (dt, amount)');
    expect(out).toContain('GROUPS BETWEEN');
    expect(out).toContain("INTERVAL '30' DAY");
    expect(out).toContain('IN (');
  });

  it('formats empty statements (;;)', () => {
    const result = formatSQL(';;');
    expect(result).toBe('');
  });

  it('formats Unicode quoted identifiers', () => {
    const sql = `SELECT "日本語" FROM "テーブル";`;
    const out = formatSQL(sql);
    expect(out).toContain('"日本語"');
    expect(out).toContain('"テーブル"');
  });

  it('formats comments between tokens', () => {
    const sql = `SELECT a, /* pick columns */ b FROM t;`;
    const out = formatSQL(sql);
    expect(out).toContain('SELECT');
    expect(out).toContain('FROM');
  });

  it('formats mixed quoted/unquoted identifiers', () => {
    const sql = `SELECT MyCol, "MyCol" FROM t;`;
    const out = formatSQL(sql);
    // Unquoted identifiers should be lowercased
    expect(out).toContain('mycol');
    // Quoted identifiers should preserve case
    expect(out).toContain('"MyCol"');
  });

  it('formats dollar-quoted strings', () => {
    const sql = `SELECT $$hello world$$;`;
    const out = formatSQL(sql);
    expect(out).toContain('$$hello world$$');
  });
});

describe('edge cases for identifier lengths', () => {
  it('handles identifier near MAX_IDENTIFIER_LENGTH (9999 chars)', () => {
    const longId = 'a'.repeat(9999);
    const sql = `SELECT ${longId} FROM t;`;
    const out = formatSQL(sql);
    expect(out).toContain(longId);
  });
});

describe('production-readiness formatting regressions', () => {
  it('formats COLLATE expressions without alias confusion', () => {
    const out = formatSQL('SELECT name COLLATE "C" FROM users;');
    expect(out).toContain('SELECT name COLLATE "C"');
    expect(out).toContain('\n  FROM users;');
  });

  it('formats DISTINCT ON with correct column alignment', () => {
    const sql = 'SELECT DISTINCT ON (customer_id) customer_id, order_id, created_at, total_amount FROM orders ORDER BY customer_id, created_at DESC;';
    const out = formatSQL(sql);
    expect(out).toContain('SELECT DISTINCT ON (customer_id) customer_id,');
    expect(out).toContain('\n       order_id, created_at, total_amount');
  });

  it('formats EXPLAIN statements and nested query body', () => {
    const sql = 'EXPLAIN ANALYZE SELECT * FROM t WHERE id = 1;';
    const out = formatSQL(sql);
    expect(out).toContain('EXPLAIN (ANALYZE)');
    expect(out).toContain('\nSELECT *');
    expect(out).toContain('\n WHERE id = 1;');
  });

  it('formats recursive CTE SEARCH/CYCLE clauses', () => {
    const sql = 'WITH RECURSIVE t(n) AS (SELECT 1) SEARCH DEPTH FIRST BY n SET ord CYCLE n SET cyc USING path SELECT * FROM t;';
    const out = formatSQL(sql);
    expect(out).toContain('SEARCH DEPTH FIRST BY n SET ord');
    expect(out).toContain('CYCLE n SET cyc USING path');
  });

  it('wraps long JOIN ON predicates at logical boundaries', () => {
    const sql = 'SELECT * FROM orders o JOIN shipments s ON o.super_long_customer_identifier = s.super_long_customer_identifier AND o.super_long_order_identifier = s.super_long_order_identifier AND o.super_long_tracking_identifier = s.super_long_tracking_identifier;';
    const out = formatSQL(sql);
    expect(out).toContain('\n       AND o.super_long_order_identifier = s.super_long_order_identifier');
    expect(out).toContain('\n       AND o.super_long_tracking_identifier = s.super_long_tracking_identifier');
  });

  it('wraps long BETWEEN expressions at AND boundary', () => {
    const sql = 'SELECT * FROM t WHERE extraordinarily_long_column_identifier BETWEEN extraordinarily_long_lower_bound_value AND extraordinarily_long_upper_bound_value;';
    const out = formatSQL(sql);
    expect(out).toContain('BETWEEN extraordinarily_long_lower_bound_value');
    expect(out).toContain('\n                                                  AND extraordinarily_long_upper_bound_value');
  });

  it('wraps IN (SELECT ...) subqueries in predicate context', () => {
    const sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM order_items WHERE status = 'very_long_status_value' AND category = 'extremely_long_category_name');";
    const out = formatSQL(sql);
    expect(out).toContain('WHERE id IN');
    expect(out).toContain('\n       (SELECT user_id');
  });

  it('preserves line-comment boundaries in recovery-mode raw output', () => {
    const sql = 'SELECT CASE WHEN x = 1 -- note\nTHEN y ELSE z END FROM t;';
    const out = formatSQL(sql);
    expect(out.trimEnd()).toBe(sql);
    expect(out).toContain('-- note\nTHEN');
  });
});
