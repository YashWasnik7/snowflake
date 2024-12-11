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

## Analysis
### 1. Daily Sales Analysis
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

### Output from `VW_DAILY_SALES_ANALYSIS`

![image](https://github.com/user-attachments/assets/3790b9ac-65e7-4c27-98f7-d76078d67cae)

### 2. Customer behavior
### 'VW_CUSTOMER_PRODUCT_AFFINITY'
```  sql
CREATE OR REPLACE VIEW VW_CUSTOMER_PRODUCT_AFFINITY AS
SELECT 
    c.customer_id,
    c.customer_type,
    p.product_id,
    p.name AS product_name,
    p.category AS product_category,
    DATE_TRUNC('MONTH', o.transaction_date) AS purchase_month,
    COUNT(DISTINCT o.transaction_id) AS purchase_count,
    SUM(o.quantity) AS total_quantity,
    SUM(o.total_amount) AS total_spent,
    AVG(o.total_amount) AS avg_purchase_amount,
    DATEDIFF('DAY', MIN(o.transaction_date), MAX(o.transaction_date)) AS days_between_first_last_purchase
FROM SILVER.CUSTOMER c
JOIN SILVER.ORDERS o ON c.customer_id = o.customer_id
JOIN SILVER.PRODUCT p ON o.product_id = p.product_id
GROUP BY 
    c.customer_id,
    c.customer_type,
    p.product_id,
    p.name,
    p.category,
    DATE_TRUNC('MONTH', o.transaction_date);
```

### Output from `VW_CUSTOMER_PRODUCT_AFFINITY`
![image](https://github.com/user-attachments/assets/c3b54467-2a95-43cb-9218-73c0adc7cc23)

