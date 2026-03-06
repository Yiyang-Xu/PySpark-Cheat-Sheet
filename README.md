# PySpark Cheat Sheet

A comprehensive, production-ready reference guide for the most commonly used patterns and functions in PySpark SQL.

## 📑 Table of Contents

- [1. Quickstart & Basics](https://www.google.com/search?q=%231-quickstart--basics)
- [2. Column & Row Operations](https://www.google.com/search?q=%232-column--row-operations)
- [3. Logic Control & Conditionals](https://www.google.com/search?q=%233-logic-control--conditionals)
- [4. Joins & Unions](https://www.google.com/search?q=%234-joins--unions)
- [5. Aggregation & Window Functions](https://www.google.com/search?q=%235-aggregation--window-functions)
- [6. Data Transformations](https://www.google.com/search?q=%236-data-transformations)
  - [String Operations](https://www.google.com/search?q=%23string-operations)
  - [Number Operations](https://www.google.com/search?q=%23number-operations)
  - [Date & Timestamp](https://www.google.com/search?q=%23date--timestamp)
  - [Array & Struct](https://www.google.com/search?q=%23array--struct)
- [7. Advanced Operations & Optimization](https://www.google.com/search?q=%237-advanced-operations--optimization)

## 1. Quickstart & Basics

### Setup

```
brew install apache-spark && pip install pyspark
```

### Initialize Session & Read

```
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('/path/to/your/input/file')
```

### Inspection Tools

```
df.show()           # Show preview
df.printSchema()    # Get schema
df.count()          # Get row count
df.columns          # Get list of columns
df.dtypes           # Get column types

# Local Conversion (WARNING: In-memory)
pandas_df = df.toPandas()
rows = df.collect()
```

## 2. Column & Row Operations

### Selection & Filtering

```
# Filter conditions
df = df.filter(df.age > 25)
df = df.filter((df.age > 25) & (df.is_adult == 'Y'))
df = df.filter(F.col('name').isin(['Alice', 'Bob']))

# Sorting
df = df.orderBy(F.col("department").asc(), F.col("salary").desc())
```

### Column Manipulation

```
# Add/Rename/Drop
df = df.withColumn('status', F.lit('PASS'))
df = df.withColumnRenamed('dob', 'date_of_birth')
df = df.drop('mod_dt', 'mod_username')

# Selection with Alias
df = df.select('name', F.col('dob').alias('date_of_birth'))
```

### Deduplication

```python
df = df.distinct()
df = df.dropDuplicates(['name', 'height'])
```

## 3. Logic Control & Conditionals

### Case When (If-Then-Else)

```python
df = df.withColumn('full_name', F.when(
    (df.fname.isNotNull() & df.lname.isNotNull()), F.concat(df.fname, df.lname)
).otherwise(F.lit('N/A')))
```

### Handling Nulls

```python
# Fill NA
df = df.fillna({'first_name': 'Tom', 'age': 0})

# Coalesce (Pick first non-null)
df = df.withColumn('last_name', F.coalesce(df.last_name, df.surname, F.lit('N/A')))

# Replace values
df = df.replace({"": None}, subset=["name"]) # Empty string to Null
```

## 4. Joins & Unions

```python
# Join on single/multiple keys
df = df.join(other_table, 'person_id', 'left')
df = df.join(other_table, ['first_name', 'last_name'], 'left')

# Join on different column names
df = df.join(other_table, df.id == other_table.person_id, 'left')

# Union (Append rows)
df_all = df1.union(df2)
```

## 5. Aggregation & Window Functions

### GroupBy Aggregations

```python
df.groupBy('gender').agg(
    F.max('age').alias('max_age'),
    F.collect_set('name').alias('unique_names')
)
```

### Window Functions (Ranking & Moving Metrics)

```python
from pyspark.sql import Window as W

# Get latest row per group
window = W.partitionBy("user_id").orderBy(F.desc("date"))
df = df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")
```

## 6. Data Transformations

### String Operations

- **Filters**: `contains()`, `startswith()`, `endswith()`, `like()`, `rlike()` (Regex).

- **Functions**:

  ```python
  F.trim(col), F.lpad(col, 4, '0'), F.concat_ws('-', 'fname', 'lname')
  F.regexp_replace(id, 'pattern', 'replacement')
  F.regexp_extract(id, 'pattern', 0)
  ```

### Number Operations

```python
F.round('price', 2), F.floor('price'), F.abs('price'), F.pow('x', 'y')
F.greatest('col_a', 'col_b') # Max of multiple columns
```

### Date & Timestamp

```python
F.to_date('col', 'yyyy-MM-dd'), F.current_date()
F.datediff('end', 'start'), F.add_months('date', 1)
F.year('date'), F.month('date'), F.dayofmonth('date')
```

### Array & Struct

```python
# Array
df.withColumn('first_elem', F.col("my_array").getItem(0))
df.select(F.explode('my_array')) # Row per element

# Struct
df.withColumn('my_struct', F.struct('col_a', 'col_b'))
df.select(F.col('my_struct').getField('col_a'))
```

## 7. Advanced Operations & Optimization

### Partitioning & Caching

```python
df = df.repartition(10) # Shuffle
df = df.coalesce(1)     # Minimal shuffle (decrease only)
df.cache()              # Persist in memory
```

### UDFs (User Defined Functions)

```python
times_two_udf = F.udf(lambda x: x * 2, T.IntegerType())
df = df.withColumn('age_doubled', times_two_udf(df.age))
```

## Contributing

If you can't find what you're looking for, check out the [PySpark Official Documentation](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html) and submit a PR!
