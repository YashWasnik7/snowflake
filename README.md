# Pacific Retail Data Transformations

## Overview
This project demonstrates a comprehensive ETL pipeline using **Snowflake** to transform raw data into a structured and validated format for downstream analytics. The pipeline follows a multi-layer architecture with **Bronze**, **Silver**, **Gold** layers for better data governance and business insights.

The project includes:
1. **Data Loading**: Loading CSV files (`customers`, `orders`, `products`) into the **Bronze** layer.
2. **Data Transformation**: Applying business rules to clean and validate the data in the **Silver** layer.
3. **Business Logic**: Ensuring data quality and enriching the dataset for business use cases.

## Data Sources
### Raw Data
- `customers.csv`
- `orders.csv`
- `products.csv`

Raw layer creation:

![image](https://github.com/user-attachments/assets/394292c0-f211-4317-8875-9a79bf0ccb8d)

Silver layer table creation:

![image](https://github.com/user-attachments/assets/4d0eac12-822b-4815-93db-06a369b61f58)


## Transformations Applied
### 1. Customers Table
#### Business Logic:
- **Email Validation**: Ensure `email` is not null; default invalid emails to `'invalid@example.com'`.
- **Customer Type Standardization**: Normalize to `Regular`, `Premium`, or `Unknown`.
- **Age Verification**: Ensure `age` is between 18 and 120.
- **Gender Standardization**: Classify gender as `Male`, `Female`, or `Other`.
- **Total Purchases Validation**: Ensure `total_purchases` is numeric; default invalid values to `0`.

#### SQL Query:
```sql
CREATE OR REPLACE TABLE PACIFIC_RETAIL_DB.SILVER.CUSTOMER AS
SELECT 
    customer_id, 
    CUSTOMER_TYPE,
    email, 
    CASE 
        WHEN email IS NULL THEN 'invalid@example.com'  -- Ensure email is not null
        ELSE email 
    END AS validated_email,
    CASE 
        WHEN LOWER(customer_type) IN ('regular', 'premium') THEN INITCAP(customer_type)
        ELSE 'Unknown'  -- Normalize customer types to "Regular", "Premium", or "Unknown"
    END AS standardized_customer_type,
    CASE 
        WHEN age BETWEEN 18 AND 120 THEN age  -- Ensure the age is between 18 and 120
        ELSE NULL
    END AS verified_age,
    CASE 
        WHEN LOWER(gender) IN ('male', 'female') THEN INITCAP(gender)
        ELSE 'Other'  -- Classify gender as "Male", "Female", or "Other"
    END AS standardized_gender,
    CASE 
        WHEN TRY_CAST(total_purchases AS NUMBER) IS NOT NULL THEN total_purchases  -- Validate total purchases is numeric
        ELSE 0
    END AS validated_total_purchases
FROM 
    PACIFIC_RETAIL_DB.BRONZE.RAW_CUSTOMER;
```

### Data before & after transformations

![image](https://github.com/user-attachments/assets/f7420be1-2b17-48bf-a6be-827e2334c07c)

### Aggregated Query for Gold Layer
The query aggregates data to analyze daily sales, providing metrics such as total quantity sold, total sales, average price per unit, and average transaction value. Below is the query for the view:

#### `VW_DAILY_SALES_ANALYSIS`
```sql
CREATE OR REPLACE VIEW VW_DAILY_SALES_ANALYSIS AS
SELECT 
    o.transaction_date,
    p.product_id,
    p.name AS product_name,
    p.category AS product_category,
    c.customer_id,
    c.customer_type,
    SUM(o.quantity) AS total_quantity,
    SUM(o.total_amount) AS total_sales,
    COUNT(DISTINCT o.transaction_id) AS num_transactions,
    SUM(o.total_amount) / NULLIF(SUM(o.quantity), 0) AS avg_price_per_unit,
    SUM(o.total_amount) / NULLIF(COUNT(DISTINCT o.transaction_id), 0) AS avg_transaction_value
FROM PACIFIC_RETAIL_DB.SILVER.ORDERS o
JOIN PACIFIC_RETAIL_DB.SILVER.PRODUCT p ON o.product_id = p.product_id
JOIN PACIFIC_RETAIL_DB.SILVER.CUSTOMER c ON o.customer_id = c.customer_id
GROUP BY 
    o.transaction_date,
    p.product_id,
    p.name,
    p.category,
    c.customer_id,
    c.customer_type;
```

### Example Output from `VW_DAILY_SALES_ANALYSIS`

TRANSACTION_DATE	PRODUCT_ID	PRODUCT_NAME	PRODUCT_CATEGORY	CUSTOMER_ID	CUSTOMER_TYPE	TOTAL_QUANTITY	TOTAL_SALES	NUM_TRANSACTIONS	AVG_PRICE_PER_UNIT	AVG_TRANSACTION_VALUE
7/27/20	425	Product 425	Garden	802	Regular	1	363.4	1	363.4	363.4
8/10/22	280	Product 280	Clothing	858	Regular	6	758.18	1	126.3633333	758.18
5/22/20	694	Product 694	Toys	658	Regular	9	748.66	1	83.18444444	748.66
	930	Product 930	Beauty	516	Regular	4	933.78	1	233.445	933.78
6/24/22	104	Product 104	Home	368	VIP	10	137.28	1	13.728	137.28

![image](https://github.com/user-attachments/assets/3790b9ac-65e7-4c27-98f7-d76078d67cae)





