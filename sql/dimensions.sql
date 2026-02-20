-- Dimension: Date
CREATE TABLE IF NOT EXISTS `commercepulse.dim_date` (
  date_key     DATE        NOT NULL,
  day_of_week  STRING,
  week_number  INT64,
  month        INT64,
  quarter      INT64,
  year         INT64,
  is_weekend   BOOL
);

-- Dimension: Customer
CREATE TABLE IF NOT EXISTS `commercepulse.dim_customer` (
  customer_id   STRING NOT NULL,
  customer_name STRING,
  email         STRING,
  country       STRING,
  created_at    TIMESTAMP
);

-- Dimension: Product
CREATE TABLE IF NOT EXISTS `commercepulse.dim_product` (
  product_id   STRING NOT NULL,
  product_name STRING,
  category     STRING,
  vendor_id    STRING,
  unit_price   FLOAT64
);