SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS missing_email,
  SUM(CASE WHEN full_name IS NULL THEN 1 ELSE 0 END) AS missing_full_name
FROM customers_curated
