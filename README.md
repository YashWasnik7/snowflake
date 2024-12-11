# Pacific Retail Data Transformation Project
# Snowflake Data Warehousing & Engineering project 
## Project Overview
This project demonstrates a comprehensive ETL pipeline using **Snowflake** to transform raw data into a structured and validated format for downstream analytics. The pipeline follows a multi-layer architecture with **Bronze**, **Silver**, and **Gold** layers for better data governance and business insights.

The project includes:
1. **Data Loading**: Loading CSV files (`customers`, `orders`, `products`) into the **Bronze** layer.
2. **Data Transformation**: Applying business rules to clean and validate the data in the **Silver** layer.
3. **Business Logic**: Ensuring data quality and enriching the dataset for business use cases.

Raw layer creation:

![image](https://github.com/user-attachments/assets/394292c0-f211-4317-8875-9a79bf0ccb8d)

Silver layer table creation:

![image](https://github.com/user-attachments/assets/4d0eac12-822b-4815-93db-06a369b61f58)

## DATA QUALITY CHECKS while loading silver.customer table from bronze.customer :

![image](https://github.com/user-attachments/assets/3d8fdc8a-b79d-4cfe-8654-b64ff4f230f3)

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
CREATE OR REPLACE TABLE silver.customers AS
SELECT 
    customer_id,
    CASE WHEN email IS NOT NULL THEN email ELSE 'invalid@example.com' END AS validated_email,
    CASE 
        WHEN LOWER(customer_type) IN ('regular', 'premium') THEN INITCAP(customer_type)
        ELSE 'Unknown'
    END AS standardized_customer_type,
    CASE 
        WHEN age BETWEEN 18 AND 120 THEN age ELSE NULL
    END AS verified_age,
    CASE 
        WHEN LOWER(gender) IN ('male', 'female') THEN INITCAP(gender)
        ELSE 'Other'
    END AS standardized_gender,
    CASE 
        WHEN TRY_CAST(total_purchases AS NUMBER) IS NOT NULL THEN total_purchases
        ELSE 0
    END AS validated_total_purchases
FROM 
    bronze.raw_customers;

#### SQL Query:
```sql
CREATE OR REPLACE TABLE silver.customers AS
SELECT 
    customer_id,
    CASE WHEN email IS NOT NULL THEN email ELSE 'invalid@example.com' END AS validated_email,
    CASE 
        WHEN LOWER(customer_type) IN ('regular', 'premium') THEN INITCAP(customer_type)
        ELSE 'Unknown'
    END AS standardized_customer_type,
    CASE 
        WHEN age BETWEEN 18 AND 120 THEN age ELSE NULL
    END AS verified_age,
    CASE 
        WHEN LOWER(gender) IN ('male', 'female') THEN INITCAP(gender)
        ELSE 'Other'
    END AS standardized_gender,
    CASE 
        WHEN TRY_CAST(total_purchases AS NUMBER) IS NOT NULL THEN total_purchases
        ELSE 0
    END AS validated_total_purchases
FROM 
    bronze.raw_customers;

#### Data before & after transformations

![image](https://github.com/user-attachments/assets/f7420be1-2b17-48bf-a6be-827e2334c07c)
