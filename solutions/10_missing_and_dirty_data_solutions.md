# Solutions: Missing and Dirty Data

## Exercise 1
```python
orders.withColumn('amount_num', F.col('amount').cast('double')).filter(F.col('status') == 'completed').select('customer_id', 'amount_num').printSchema()
```

## Exercise 2
```python
typed_orders.groupBy('customer_id').agg(F.sum('amount_num').alias('revenue')).orderBy(F.desc('revenue')).show()
```
`groupBy` is wide because records with the same key may begin in different partitions.

## Exercise 3
```python
typed_orders.join(customers, 'customer_id', 'left').count()
```
Compare this count to `typed_orders.count()`; any increase indicates duplicate customer keys.

## Debugging
Cast first: `orders.withColumn('amount_num', F.col('amount').cast('double')).filter(F.col('amount_num') > 10)`. Null after a cast represents unparseable input; quarantine or reject it according to a documented rule.

## Concept answer
`show` and `count` are actions. `filter` and `withColumn` are narrow in typical cases; `groupBy` can shuffle.
