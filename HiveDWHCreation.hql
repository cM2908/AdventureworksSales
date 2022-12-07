DROP TABLE products;

CREATE EXTERNAL TABLE products(
	`product_id` INT,
	`product_number` STRING,
	`product_name` STRING,
	`model_name` STRING,
	`price` DOUBLE,
	`category` STRING,
	`subcategory` STRING,
	`modified_date` TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/adventureworks/product/';

DROP TABLE creditcards;

CREATE EXTERNAL TABLE creditcards (
	`creditcard_id` INT,
	`card_type` STRING,
	`card_number` STRING,
	`expiry_month` INT,
	`expiry_year` INT,
	`modified_date` TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/adventureworks/creditcard/';

DROP TABLE specialoffers;

CREATE EXTERNAL TABLE specialoffers (
	`specialoffer_id` INT,
	`description` STRING,
	`discount_pct` DOUBLE,
	`type` STRING,
	`category` STRING,
	`offer_start_date` TIMESTAMP,
	`offer_end_date` TIMESTAMP,
	`min_quantity` INT,
	`max_quantity` INT,
	`modified_date` TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/adventureworks/specialoffer/';

DROP TABLE dates;

CREATE EXTERNAL TABLE dates (
	`date_key` INT,
	`full_date` DATE,
	`date_name` STRING,
	`date_name_us` STRING,
	`date_name_eu` STRING,
	`day_of_week` INT,
	`day_name_of_week` STRING,
	`day_of_month` INT,
	`day_of_year` INT,
	`weekday_weekend` STRING,
	`week_of_year` INT,
	`month_name` STRING,
	`month_of_year` INT,
	`is_last_day_of_month` STRING,
	`calendar_quarter` INT,
	`calendar_year` INT,
	`calendar_year_month` STRING,
	`calendar_year_qtr` STRING,
	`fiscal_month_of_year` INT,
	`fiscal_quarter` INT,
	`fiscal_year` INT,
	`fiscal_year_month` STRING,
	`fiscal_year_qtr` STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/adventureworks/date/';

DROP TABLE sales;

CREATE EXTERNAL TABLE sales (
	`salesorder_detail_id` INT,
	`salesorder_id` INT,
	`customer_id` INT,
	`product_id` INT,
	`store_id` INT,
	`date_id` INT,
	`specialoffer_id` INT,
	`creditcard_id` INT,
	`salesorder_number` STRING,
	`quantity` INT,
	`unit_price` DOUBLE,
	`unit_price_discount` DOUBLE,
	`line_total` DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/adventureworks/sales/';

DROP TABLE salesorders;

CREATE EXTERNAL TABLE salesorders (
	`salesorder_id` INT,
	`customer_id` INT,
	`store_id` INT,
	`date_id` INT,
	`creditcard_id` INT,
	`salesorder_number` STRING,
	`sub_total` DOUBLE,
	`tax_amt` DOUBLE,
	`freight` DOUBLE,
	`total_due` DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/adventureworks/sales_order/';

DROP TABLE stores;

CREATE EXTERNAL TABLE stores (
	`store_id` INT,
	`store_name` STRING,
	`annual_sales` DOUBLE,
	`annual_revenue` DOUBLE,
	`bank_name` STRING,
	`business_type` STRING,
	`year_opened` INT,
	`specialty` STRING,
	`square_feet` DOUBLE,
	`brands` STRING,
	`internet` STRING,
	`number_employees` INT,
	`territory_name` STRING,
	`country_region_code` STRING,
	`territory_group` STRING,
	`modified_date` TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/adventureworks/store_with_demographics/';

DROP TABLE customers;

CREATE EXTERNAL TABLE customers(
	`customer_id` INT,
	`account_number` STRING,
	`customer_type` STRING,
	`territory_name` STRING,
	`country_region_code` STRING,
	`territory_group` STRING,
	`total_purchase_ytd` DOUBLE, 
	`date_first_purchase` TIMESTAMP, 
	`birthdate` TIMESTAMP,
	`marital_status` STRING, 
	`yearly_income` STRING, 
	`gender` STRING, 
	`total_children` INT,
	`number_children_at_home` INT, 
	`education` STRING, 
	`occupation` STRING, 
	`home_owner_flag` STRING, 
	`number_of_cars_owned` INT,
	`commute_distance` STRING,
	`modified_date` TIMESTAMP,
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/adventureworks/customer_with_demographics/';
