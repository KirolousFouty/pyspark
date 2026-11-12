# Intermediate challenges

1. Deduplicate orders by `order_id`, retaining the latest `ingested_at` using a window.
2. Join products to order items and calculate category revenue; verify each product key is unique before joining.
3. Find the three highest-value orders per customer with `row_number`.
4. Recreate one solution using a temporary SQL view and a CTE.
5. Debug: a left join produces more rows than the input. Profile duplicate right-side keys and fix the data contract.
