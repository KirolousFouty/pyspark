# Beginner challenges

Work without looking at `solutions/`.

1. Read `data/raw/orders.csv`, cast `amount`, and keep only positive values.
2. Derive a `high_value` boolean for amounts at least 100 using `when`/`otherwise`.
3. Count orders by status and explain why the result has fewer rows.
4. Debug: why does `orders.filter(F.col("amount") > 50)` produce an unreliable comparison?

Expected check: your positive-value result has no null `amount_num` and no value less than or equal to zero.
