--Проект №2
DELETE FROM mart.d_city;

DELETE FROM mart.d_customer ;

DELETE FROM mart.d_item ;


-- Создание столбца status, который содержит статус заказа: shipped - отправлен, refunded - возвращен
ALTER TABLE staging.user_order_log ADD COLUMN status varchar(15);


-- Создание столбца status, который содержит статус заказа: shipped - отправлен, refunded - возвращен
ALTER TABLE mart.f_sales  ADD COLUMN status varchar(15);

--Проверка данных в таблице mart.f_sales
SELECT * FROM mart.f_sales
ORDER BY date_id DESC;


 

--Создание таблицы mart.f_customer_retention (второй этап проекта)
DROP TABLE IF EXISTS mart.f_customer_retention; 

CREATE TABLE IF NOT EXISTS mart.f_customer_retention ( 

id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, --идентификатор записи 
new_customers_count INT NOT NULL,  --кол-во новых клиентов (тех, которые сделали только один заказ за рассматриваемый промежуток времени). 
returning_customers_count INT NOT NULL,--кол-во вернувшихся клиентов (тех, которые сделали только несколько заказов за рассматриваемый промежуток времени). 
refunded_customer_count INT NOT NULL, --кол-во клиентов, оформивших возврат за рассматриваемый промежуток времени. 
period_name VARCHAR(6) DEFAULT 'weekly',--weekly. 
period_id INT NOT NULL, --идентификатор периода (номер недели или номер месяца). 
item_id INT NOT NULL, --идентификатор категории товара. 
new_customers_revenue INT NOT NULL, --доход с новых клиентов. 
returning_customers_revenue INT NOT NULL, --доход с вернувшихся клиентов. 
customers_refunded INT NOT NULL, --количество возвратов клиентов.
CONSTRAINT f_customer_retention_pk PRIMARY KEY (id) -- первичный ключ таблицы 
); 

--Проверка, создалась таблица или нет
SELECT * 
FROM mart.f_customer_retention;

/*Скрипт заполнения таблицы mart.f_customer_retention. Скрипт выполняется в Airflow 
Скрипты добавил в папку migrations, чтобы вам было их видно */

DELETE
FROM mart.f_customer_retention
WHERE period_id=(SELECT week_of_year FROM mart.d_calendar WHERE date_actual = '{{ds}}'::DATE);

WITH customers AS
         (SELECT *
          FROM mart.f_sales
                   JOIN mart.d_calendar ON f_sales.date_id = d_calendar.date_id
          WHERE week_of_year = DATE_PART('week', '{{ds}}'::DATE)),
     new_customers AS
         (SELECT customer_id
          FROM customers
          WHERE status = 'shipped'
          GROUP BY customer_id
          HAVING count(*) = 1),
     returning_customers AS
         (SELECT customer_id
          FROM customers
          WHERE status = 'shipped'
          GROUP BY customer_id
          HAVING count(*) > 1),
     refunded_customers AS
         (SELECT customer_id
          FROM customers
          WHERE status = 'refunded'
          GROUP BY customer_id)
INSERT INTO mart.f_customer_retention (new_customers_count, new_customers_revenue, returning_customers_count, 
returning_customers_revenue, refunded_customer_count, customers_refunded, period_id, period_name, item_id)
SELECT COALESCE(new_customers.customers, 0) AS new_customers_count,
       COALESCE(new_customers.revenue, 0) AS new_customers_revenue,
       COALESCE(returning_customers.customers, 0) AS returning_customers_count,
       COALESCE(returning_customers.revenue, 0) AS returning_customers_revenue,
       COALESCE(refunded_customers.customers, 0) AS refunded_customers_count,
       COALESCE(refunded_customers.refunded, 0) AS customers_refunded,
       COALESCE(new_customers.week_of_year,
                returning_customers.week_of_year,
                refunded_customers.week_of_year) AS period_id,
       'week'AS period_name,
       COALESCE(new_customers.item_id,
                returning_customers.item_id,
                refunded_customers.item_id) AS item_id  
FROM (SELECT week_of_year,
             item_id,
             sum(payment_amount) AS revenue,
             sum(quantity)       AS items,
             count(*)            AS customers
      FROM customers
      WHERE status = 'shipped'
        AND customer_id in (SELECT customer_id FROM new_customers)
      GROUP BY week_of_year, item_id) new_customers
         FULL JOIN
     (SELECT week_of_year,
             item_id,
             sum(payment_amount) AS revenue,
             sum(quantity)       AS items,
             count(*)            AS customers
      FROM customers
      WHERE status = 'shipped'
        AND customer_id IN (SELECT customer_id FROM returning_customers)
      GROUP BY week_of_year, item_id) returning_customers
     ON new_customers.week_of_year = returning_customers.week_of_year
         AND new_customers.item_id = returning_customers.item_id
         FULL JOIN
     (SELECT week_of_year,
             item_id,
             sum(payment_amount) AS refunded,
             sum(quantity)       AS items,
             count(*)            AS customers
      FROM customers
      WHERE status = 'refunded'
        AND customer_id in (SELECT customer_id FROM refunded_customers)
        GROUP BY week_of_year, item_id) AS refunded_customers
     ON new_customers.week_of_year = refunded_customers.week_of_year
         AND new_customers.item_id = refunded_customers.item_id;

--Проверка, отработал скрипт в Airflow или нет          
SELECT * 
FROM mart.f_customer_retention
ORDER BY item_id;


