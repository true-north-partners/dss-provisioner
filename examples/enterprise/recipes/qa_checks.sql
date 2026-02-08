SELECT
  CURRENT_TIMESTAMP AS checked_at,
  COUNT(*) AS total_customers,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customer_id,
  SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS missing_email
FROM customers_curated
