# PySpark Cheatsheet (Pandas User Edition)

## I. Setup & I/O

### 1. Quickstart & Installation

Install on macOS:

```bash
brew install apache-spark && pip install pyspark
```

### 2. Session & First DataFrame

```python
from pyspark.sql import SparkSession

# Standard session initialization
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# I/O options: 
df = spark.read.csv('/path/to/your/input/file', header=True, inferSchema=True)
```

### 3. Writing Data

```python
# Write output to disk
df.write.csv('/path/to/your/output/file')
df.write.mode("overwrite").parquet('/path/to/parquet') # Common production format
```

## II. DataFrame Inspection (Basics)

### 1. Previewing Data

```python
# Show a preview (Default 20 rows)
df.show()

# Show preview of first / last n rows
df.head(5)
df.tail(5)
```

### 2. Metadata & Stats

```python
# Get columns
df.columns

# Get columns + column types
df.dtypes

# Get schema
df.schema
df.printSchema() # Human readable tree format

# Get row count
df.count()

# Get column count
len(df.columns)
```

### 3. Data Collection (In-Memory)

```python
# Get results (WARNING: in-memory) as list of PySpark Rows
collected_rows = df.collect()

# Get results (WARNING: in-memory) as list of Python dicts
dicts = [row.asDict(recursive=True) for row in df.collect()]

# Convert (WARNING: in-memory) to Pandas DataFrame
pandas_df = df.toPandas()
```

## III. Data Transformation Patterns

### 1. Core Imports

```python
# Easily reference these as F.my_function() and T.my_type() below
from pyspark.sql import functions as F, types as T
```

### 2. Filtering & Sorting

```python
# Filter on equals condition
df = df.filter(F.col('is_adult') == 'Y')

# Filter on >, <, >=, <= condition
df = df.filter(F.col('age') > 25)

# Multiple conditions require parentheses around each condition
df = df.filter((F.col('age') > 25) & (F.col('is_adult') == 'Y'))

# Compare against a list of allowed values
df = df.filter(F.col('first_name').isin([3, 4, 7]))

# Sort results
df = df.orderBy(F.col('age').asc())
df = df.orderBy(F.col('age').desc())

# Sort by 'department' (ASC) and then 'salary' (DESC)
df_sorted = df.orderBy(
    F.col("department").asc(), 
    F.col("salary").desc()
)

# Limit actual DataFrame to n rows (non-deterministic)
df = df.limit(5)
```

### 3. Joins

```python
# Left join in another dataset
df = df.join(person_lookup_table, 'person_id', 'left')

# Match on different columns in left & right datasets
df = df.join(other_table, df.id == other_table.person_id, 'left')

# Match on multiple columns
df = df.join(other_table, ['first_name', 'last_name'], 'left')

# Optimized Join: Broadcast small tables to avoid Shuffling
df = df.join(F.broadcast(small_table), 'id', 'left')
```

### 4. Column Operations

```python
# Add a new static column
df = df.withColumn('status', F.lit('PASS'))

# Construct a new dynamic column (Equivalent to np.where)
df = df.withColumn('full_name', F.when(
    (df.fname.isNotNull() & df.lname.isNotNull()), F.concat(df.fname, df.lname)
).otherwise(F.lit('N/A')))

# Pick which columns to keep, optionally rename some
df = df.select(
    'name',
    'age',
    F.col('dob').alias('date_of_birth'),
)

# Remove columns
df = df.drop('mod_dt', 'mod_username')

# Rename a column
df = df.withColumnRenamed('dob', 'date_of_birth')

# Keep all the columns which also occur in another dataset
df = df.select(*(F.col(c) for c in df2.columns))

# Batch Rename/Clean Columns
for col in df.columns:
    df = df.withColumnRenamed(col, col.lower().replace(' ', '_').replace('-', '_'))
```

### 5. Casting, Nulls & Duplicates

```python
# Cast a column to a different type
df = df.withColumn('price', df.price.cast(T.DoubleType()))

# Replace all nulls with a specific value
df = df.fillna({
    'first_name': 'Tom',
    'age': 0,
})

# Take the first value that is not null
df = df.withColumn('last_name', F.coalesce(df.last_name, df.surname, F.lit('N/A')))

# Drop duplicate rows in a dataset (distinct)
df = df.dropDuplicates() 
df = df.distinct()

# Drop duplicate rows, but consider only specific columns
df = df.dropDuplicates(['name', 'height'])

# Replace empty strings with null (leave out subset keyword arg to replace in all columns)
df = df.replace({"": None}, subset=["name"])

# Convert Python/PySpark/NumPy NaN operator to null
df = df.replace(float("nan"), None)
```

## IV. Data Types Specific Operations

### 1. String Operations

#### String Filters

```python
# Contains, StartsWith, EndsWith
df = df.filter(df.name.contains('o'))
df = df.filter(df.name.startswith('Al'))
df = df.filter(df.name.endswith('ice'))

# Null Checks
df = df.filter(df.is_adult.isNull())
df = df.filter(df.first_name.isNotNull())

# SQL Like & Regex
df = df.filter(df.name.like('Al%'))
df = df.filter(df.name.rlike('[A-Z]*ice$'))

# Is In List
df = df.filter(df.name.isin('Bob', 'Mike'))
```

#### String Functions

```python
# Substring, Trim, Pad
df = df.withColumn('short_id', df.id.substr(0, 10))
df = df.withColumn('name', F.trim(df.name))
df = df.withColumn('id', F.lpad('id', 4, '0'))
df = df.withColumn('id', F.ltrim('id'))

# Concatenate
df = df.withColumn('full_name', F.concat('fname', F.lit(' '), 'lname'))
df = df.withColumn('full_name', F.concat_ws('-', 'fname', 'lname'))

# Regex Operations
df = df.withColumn('id', F.regexp_replace('id', '0F1(.*)', '1F1-$1'))
df = df.withColumn('id', F.regexp_extract('id', '[0-9]*', 0))
```

### 2. Number Operations

```python
# Round, Floor, Ceil, Abs
df = df.withColumn('price', F.round('price', 0))
df = df.withColumn('price', F.floor('price'))
df = df.withColumn('price', F.ceil('price'))
df = df.withColumn('price', F.abs('price'))

# Math Functions
df = df.withColumn('exponential_growth', F.pow('x', 'y'))
df = df.withColumn('least', F.least('subtotal', 'total'))
df = df.withColumn('greatest', F.greatest('subtotal', 'total'))
```

### 3. Date & Timestamp Operations

```python
# Current Date & Conversion
df = df.withColumn('current_date', F.current_date())
df = df.withColumn('date_of_birth', F.to_date('date_of_birth', 'yyyy-MM-dd'))
df = df.withColumn('time_of_birth', F.to_timestamp('time_of_birth', 'yyyy-MM-dd HH:mm:ss'))

# Extraction (Year, Month, etc.)
# F.year, F.month, F.dayofmonth, F.hour, F.minute, F.second
df = df.filter(F.year('date_of_birth') == F.lit('2017'))

# Date Arithmetic
df = df.withColumn('three_days_after', F.date_add('date_of_birth', 3))
df = df.withColumn('three_days_before', F.date_sub('date_of_birth', 3))
df = df.withColumn('next_month', F.add_months('date_of_birth', 1))

# Date Diff
df = df.withColumn('days_between', F.datediff('end', 'start'))
df = df.withColumn('months_between', F.months_between('start', 'end'))

# Filtering by Date Range
df = df.filter(
    (F.col('date_of_birth') >= F.lit('2017-05-10')) &
    (F.col('date_of_birth') <= F.lit('2018-07-21'))
)
```

### 4. Array & Struct Operations

#### Arrays

```python
# Create, Size, GetItem
df = df.withColumn('full_name', F.array('fname', 'lname'))
df = df.withColumn('empty_array_column', F.array([]))
df = df.withColumn('first_element', F.col("my_array").getItem(0))
df = df.withColumn('array_length', F.size('my_array'))

# Advanced Array
df = df.withColumn('flattened', F.flatten('my_array'))
df = df.withColumn('unique_elements', F.array_distinct('my_array'))
df = df.withColumn('elem_ids', F.transform(F.col('my_array'), lambda x: x.getField('id')))

# Explode (Array to Rows)
df = df.select(F.explode('my_array'))
```

#### Structs

```python
# Create Struct
df = df.withColumn('my_struct', F.struct(F.col('col_a'), F.col('col_b')))

# Get Field
df = df.withColumn('col_a', F.col('my_struct').getField('col_a'))
```

## V. Advanced & Analytics

### 1. Aggregation Operations

```python
# Basic Aggs: F.count, F.sum, F.mean, F.max, F.min
df = df.groupBy('gender').agg(F.max('age').alias('max_age_by_gender'))

# Collect Aggs
df = df.groupBy('age').agg(F.collect_set('name').alias('person_names'))
df = df.groupBy('age').agg(F.collect_list('name').alias('person_names'))
```

### 2. Window Functions

```python
from pyspark.sql import Window as W

# Deduplication using Window
window = W.partitionBy("first_name", "last_name").orderBy(F.desc("date"))
df = df.withColumn("row_number", F.row_number().over(window))
df = df.filter(F.col("row_number") == 1).drop("row_number")
```

### 3. Performance & Optimization

```python
# Repartition – df.repartition(num_output_partitions)
df = df.repartition(1)

# Cache data in memory
df.cache()
df.unpersist() # Release memory
```

### 4. UDFs (User Defined Functions)

```python
# Multiply each row's age column by two
times_two_udf = F.udf(lambda x: x * 2 if x is not None else None, T.IntegerType())
df = df.withColumn('age', times_two_udf(df.age))

# Randomly choose a value to use as a row's name
import random
random_name_udf = F.udf(lambda: random.choice(['Bob', 'Tom', 'Amy', 'Jenna']), T.StringType())
df = df.withColumn('name', random_name_udf())
```

## VI. Useful Custom Utils

```python
from pyspark.sql import DataFrame

def flatten(df: DataFrame, delimiter="_") -> DataFrame:
    '''
    Flatten nested struct columns in `df` by one level separated by `delimiter`.
    '''
    flat_cols = [name for name, type in df.dtypes if not type.startswith("struct")]
    nested_cols = [name for name, type in df.dtypes if type.startswith("struct")]

    flat_df = df.select(
        flat_cols
        + [F.col(nc + "." + c).alias(nc + delimiter + c) for nc in nested_cols for c in df.select(nc + ".*").columns]
    )
    return flat_df


def lookup_and_replace(df1: DataFrame, df2: DataFrame, df1_key: str, df2_key: str, df2_value: str) -> DataFrame:
    '''
    Replace every value in `df1`'s `df1_key` column with the corresponding value
    `df2_value` from `df2` where `df1_key` matches `df2_key`.
    '''
    return (
        df1
        .join(df2[[df2_key, df2_value]], df1[df1_key] == df2[df2_key], 'left')
        .withColumn(df1_key, F.coalesce(F.col(df2_value), F.col(df1_key)))
        .drop(df2_key)
        .drop(df2_value)
    )
```
