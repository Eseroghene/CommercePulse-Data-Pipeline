-- Fact: Orders (current state — upsert by order_id)
CREATE TABLE IF NOT EXISTS `commercepulse.fact_orders` (
  order_id        STRING    NOT NULL,
  customer_id     STRING,
  vendor          STRING,
  order_amount    FLOAT64,
  order_status    STRING,
  created_at      TIMESTAMP,
  updated_at      TIMESTAMP,
  event_id        STRING
);

-- Fact: Payments (append-only)
CREATE TABLE IF NOT EXISTS `commercepulse.fact_payments` (
  payment_id      STRING    NOT NULL,
  order_id        STRING,
  vendor          STRING,
  payment_amount  FLOAT64,
  payment_status  STRING,
  payment_method  STRING,
  payment_date    TIMESTAMP,
  event_id        STRING
);

-- Fact: Refunds (append-only)
CREATE TABLE IF NOT EXISTS `commercepulse.fact_refunds` (
  refund_id      STRING    NOT NULL,
  order_id       STRING,
  payment_id     STRING,
  vendor         STRING,
  refund_amount  FLOAT64,
  refund_reason  STRING,
  refund_type    STRING,
  refund_date    TIMESTAMP,
  event_id       STRING
);

-- Fact: Daily Aggregates
CREATE TABLE IF NOT EXISTS `commercepulse.fact_order_daily` (
  order_date           DATE,
  vendor               STRING,
  gross_revenue        FLOAT64,
  total_refunds        FLOAT64,
  net_revenue          FLOAT64,
  order_count          INT64,
  paid_count           INT64,
  payment_success_rate FLOAT64,
  refund_rate          FLOAT64
);