SELECT sum(order_amount) as Total_Sales, sales_month
from dbname.order_sales
group by sales_month;