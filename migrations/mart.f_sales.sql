--Загрузка данных в таблицу mart.f_sales
DELETE 
FROM 
mart.f_sales AS fs
WHERE fs.date_id = (SELECT date_id FROM mart.d_calendar WHERE date_actual = '{{ds}}'::DATE);

insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select dc.date_id, item_id, customer_id, city_id, quantity, 
CASE 
        WHEN status = 'shipped' THEN payment_amount*1 
        WHEN status = 'refunded' THEN -1*payment_amount   
	   END 	AS payment_amount,
	   status	
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}' and dc.date_id is not null;

