DROP VIEW v_customer;

CREATE VIEW v_customer AS
SELECT cus.customer_id, cus.account_number, cus.customer_type, ind.demographics,
	ter.name AS territory_name,ter.country_region_code,ter.group AS territory_group,
	(CASE WHEN cus.modified_date > ind.modified_date THEN cus.modified_date ELSE ind.modified_date END) AS modified_date
FROM customer cus
JOIN individual ind ON ind.customer_id = cus.customer_id
JOIN salesterritory ter ON cus.territory_id = ter.territory_id;



DROP VIEW v_product;

CREATE VIEW v_product AS
SELECT p.product_id,p.product_number,REPLACE(p.product_name,',','-') AS product_name,p.model_name,p.price,pc.category_name,psc.subcategory_name,p.modified_date
FROM products p
JOIN product_subcategory psc ON p.subcategory_id = psc.subcategory_id
JOIN product_category pc ON psc.category_id = pc.category_id;



DROP VIEW v_store;

CREATE VIEW v_store AS
SELECT st.store_id,st.name AS store_name,st.demographics,
	ter.name AS territory_name,ter.country_region_code,ter.group AS territory_group,
	st.modified_date
FROM store st
JOIN salesterritory ter ON st.territory_id = ter.territory_id;



DROP VIEW v_sales;

CREATE VIEW v_sales AS
SELECT sod.salesorder_detail_id,
	sod.salesorder_id,
	soh.customer_id,
	sod.product_id,
	soh.store_id,
	d.date_key AS date_id,
	sod.specialoffer_id,
	soh.creditcard_id,
	soh.salesorder_number,
	sod.order_qty AS quantity,
	sod.unit_price,
	sod.unit_price_discount,
	sod.line_total
FROM salesorderdetail sod
JOIN salesorderheader soh ON sod.salesorder_id = soh.salesorder_id
JOIN dim_date d ON DATE_FORMAT(soh.order_date,'%Y-%m-%d') = d.full_date;



DROP VIEW v_sales_order;

CREATE VIEW v_sales_order AS
SELECT soh.salesorder_id,
	soh.customer_id,
	soh.store_id,
	d.date_key AS date_id,
	soh.creditcard_id,
	soh.salesorder_number,
	soh.sub_total,
	soh.tax_amt,
	soh.freight,
	soh.total_due
FROM salesorderheader soh
JOIN dim_date d ON DATE_FORMAT(soh.order_date,'%Y-%m-%d') = d.full_date;
