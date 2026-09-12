"""
=============================================================================
   PYTHON CODING INTERVIEW QUESTIONS — BASIC TO DATA ENGINEER (5 YRS EXP)
=============================================================================
   Prepared for : Parth Nigam | Liberty Mutual
   Date         : September 12, 2026
   Covers       : Basics → OOP → DS&Algo → File I/O → SQL/DB → PySpark
                  → ETL Pipelines → Performance → Real-World DE Problems
=============================================================================
"""

# ===========================================================================
# SECTION 1 — PYTHON BASICS (0–6 Months)
# ===========================================================================

# Q1. Reverse a string without using slicing
def reverse_string(s):
    result = ""
    for ch in s:
        result = ch + result
    return result
# reverse_string("hello") → "olleh"

# Q2. Check if a string is a palindrome
def is_palindrome(s):
    s = s.lower().replace(" ", "")
    return s == s[::-1]
# is_palindrome("racecar") → True

# Q3. Find the factorial of a number (recursive & iterative)
def factorial_recursive(n):
    return 1 if n <= 1 else n * factorial_recursive(n - 1)

def factorial_iterative(n):
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result

# Q4. FizzBuzz
def fizzbuzz(n):
    for i in range(1, n + 1):
        if i % 15 == 0:   print("FizzBuzz")
        elif i % 3 == 0:  print("Fizz")
        elif i % 5 == 0:  print("Buzz")
        else:             print(i)

# Q5. Count vowels in a string
def count_vowels(s):
    return sum(1 for ch in s.lower() if ch in "aeiou")

# Q6. Find duplicate elements in a list
def find_duplicates(lst):
    seen = set()
    duplicates = set()
    for x in lst:
        if x in seen:
            duplicates.add(x)
        seen.add(x)
    return list(duplicates)
# find_duplicates([1, 2, 3, 2, 4, 3]) → [2, 3]

# Q7. Fibonacci sequence up to n terms
def fibonacci(n):
    a, b = 0, 1
    result = []
    for _ in range(n):
        result.append(a)
        a, b = b, a + b
    return result
# fibonacci(7) → [0, 1, 1, 2, 3, 5, 8]

# Q8. Check if a number is prime
def is_prime(n):
    if n < 2: return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

# Q9. Find second largest element in a list
def second_largest(lst):
    unique = list(set(lst))
    unique.sort()
    return unique[-2] if len(unique) >= 2 else None

# Q10. Merge two sorted lists
def merge_sorted(l1, l2):
    result = []
    i = j = 0
    while i < len(l1) and j < len(l2):
        if l1[i] <= l2[j]:
            result.append(l1[i]); i += 1
        else:
            result.append(l2[j]); j += 1
    return result + l1[i:] + l2[j:]


# ===========================================================================
# SECTION 2 — DATA STRUCTURES & ALGORITHMS (6 Months–1.5 Years)
# ===========================================================================

# Q11. Two Sum Problem
def two_sum(nums, target):
    seen = {}
    for i, num in enumerate(nums):
        diff = target - num
        if diff in seen:
            return [seen[diff], i]
        seen[num] = i
# two_sum([2, 7, 11, 15], 9) → [0, 1]

# Q12. Anagram Check
def are_anagrams(s1, s2):
    from collections import Counter
    return Counter(s1.lower()) == Counter(s2.lower())

# Q13. Flatten a nested list
def flatten(nested):
    result = []
    for item in nested:
        if isinstance(item, list):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result
# flatten([1, [2, [3, 4]], 5]) → [1, 2, 3, 4, 5]

# Q14. Stack implementation
class Stack:
    def __init__(self):
        self.items = []
    def push(self, item): self.items.append(item)
    def pop(self): return self.items.pop() if self.items else None
    def peek(self): return self.items[-1] if self.items else None
    def is_empty(self): return len(self.items) == 0

# Q15. Queue using two stacks
class QueueUsingStacks:
    def __init__(self):
        self.s1, self.s2 = [], []
    def enqueue(self, item): self.s1.append(item)
    def dequeue(self):
        if not self.s2:
            while self.s1:
                self.s2.append(self.s1.pop())
        return self.s2.pop() if self.s2 else None

# Q16. Binary Search
def binary_search(arr, target):
    lo, hi = 0, len(arr) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if arr[mid] == target: return mid
        elif arr[mid] < target: lo = mid + 1
        else: hi = mid - 1
    return -1

# Q17. Linked List — basic structure + reversal
class Node:
    def __init__(self, val):
        self.val = val
        self.next = None

def reverse_linked_list(head):
    prev, curr = None, head
    while curr:
        nxt = curr.next
        curr.next = prev
        prev = curr
        curr = nxt
    return prev

# Q18. Detect cycle in linked list (Floyd's algorithm)
def has_cycle(head):
    slow = fast = head
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
        if slow == fast:
            return True
    return False

# Q19. Sliding window — max sum of k elements
def max_sum_subarray(arr, k):
    window_sum = sum(arr[:k])
    max_sum = window_sum
    for i in range(k, len(arr)):
        window_sum += arr[i] - arr[i - k]
        max_sum = max(max_sum, window_sum)
    return max_sum

# Q20. Valid parentheses
def is_valid_parens(s):
    stack = []
    mapping = {')': '(', '}': '{', ']': '['}
    for ch in s:
        if ch in mapping:
            top = stack.pop() if stack else '#'
            if mapping[ch] != top:
                return False
        else:
            stack.append(ch)
    return not stack
# is_valid_parens("({[]})") → True


# ===========================================================================
# SECTION 3 — INTERMEDIATE PYTHON (1–2 Years)
# ===========================================================================

# Q21. List comprehensions — squares of even numbers
def even_squares(n):
    return [x**2 for x in range(n) if x % 2 == 0]

# Q22. Lambda, map, filter, reduce
from functools import reduce

numbers = [1, 2, 3, 4, 5]
squared     = list(map(lambda x: x**2, numbers))
evens       = list(filter(lambda x: x % 2 == 0, numbers))
total       = reduce(lambda a, b: a + b, numbers)

# Q23. Generator — infinite counter
def infinite_counter(start=0):
    while True:
        yield start
        start += 1

# Q24. Decorator — execution timer
import time
def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        print(f"{func.__name__} took {time.time() - start:.4f}s")
        return result
    return wrapper

@timer
def slow_function():
    time.sleep(0.5)
    return "done"

# Q25. Context manager — custom file handler
class ManagedFile:
    def __init__(self, path, mode='r'):
        self.path = path
        self.mode = mode
    def __enter__(self):
        self.file = open(self.path, self.mode)
        return self.file
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

# Usage: with ManagedFile("data.txt") as f: data = f.read()

# Q26. Dictionary operations — word frequency count
def word_frequency(text):
    from collections import Counter
    words = text.lower().split()
    return Counter(words)

# Q27. Grouping with defaultdict
from collections import defaultdict

def group_by_first_letter(words):
    groups = defaultdict(list)
    for word in words:
        groups[word[0]].append(word)
    return dict(groups)

# Q28. Unpacking & *args / **kwargs
def summarize(*args, **kwargs):
    print("Positional:", args)
    print("Keyword:", kwargs)
# summarize(1, 2, 3, name="Parth", role="DE")

# Q29. Exception handling — custom exception
class DataValidationError(Exception):
    pass

def validate_age(age):
    if not isinstance(age, int) or age < 0:
        raise DataValidationError(f"Invalid age: {age}")
    return age

# Q30. File reading & writing (CSV)
import csv

def read_csv(filepath):
    with open(filepath, newline='') as f:
        return list(csv.DictReader(f))

def write_csv(filepath, data, fieldnames):
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


# ===========================================================================
# SECTION 4 — OOP (Object-Oriented Programming) (1–2 Years)
# ===========================================================================

# Q31. Class, inheritance, polymorphism
class Shape:
    def area(self): raise NotImplementedError

class Circle(Shape):
    def __init__(self, r): self.r = r
    def area(self): return 3.14159 * self.r ** 2

class Rectangle(Shape):
    def __init__(self, w, h): self.w = w; self.h = h
    def area(self): return self.w * self.h

# Q32. Abstract base class
from abc import ABC, abstractmethod

class ETLJob(ABC):
    @abstractmethod
    def extract(self): pass
    @abstractmethod
    def transform(self): pass
    @abstractmethod
    def load(self): pass

    def run(self):
        data = self.extract()
        data = self.transform(data)
        self.load(data)

# Q33. Singleton pattern (useful for DB connections)
class Singleton:
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

# Q34. Property decorator — encapsulation
class Employee:
    def __init__(self, name, salary):
        self._name = name
        self._salary = salary

    @property
    def salary(self): return self._salary

    @salary.setter
    def salary(self, value):
        if value < 0: raise ValueError("Salary cannot be negative")
        self._salary = value

# Q35. Dataclass
from dataclasses import dataclass, field
from typing import List

@dataclass
class Pipeline:
    name: str
    steps: List[str] = field(default_factory=list)
    is_active: bool = True

    def add_step(self, step): self.steps.append(step)


# ===========================================================================
# SECTION 5 — PANDAS & DATA MANIPULATION (2–3 Years)
# ===========================================================================

# NOTE: These questions require: pip install pandas

# Q36. Load CSV and basic EDA
"""
import pandas as pd

df = pd.read_csv("data.csv")
print(df.head())
print(df.info())
print(df.describe())
print(df.isnull().sum())
"""

# Q37. Handle missing values
"""
df['col'].fillna(df['col'].mean(), inplace=True)    # fill with mean
df.dropna(subset=['important_col'], inplace=True)    # drop rows
df['col'].fillna(method='ffill', inplace=True)       # forward fill
"""

# Q38. GroupBy & Aggregation
"""
result = df.groupby('department').agg(
    avg_salary=('salary', 'mean'),
    total_emp=('emp_id', 'count'),
    max_salary=('salary', 'max')
).reset_index()
"""

# Q39. Merge / Join DataFrames
"""
merged = pd.merge(df1, df2, on='id', how='left')
"""

# Q40. Apply custom function to column
"""
def categorize_salary(sal):
    if sal < 50000: return 'Low'
    elif sal < 100000: return 'Mid'
    else: return 'High'

df['salary_band'] = df['salary'].apply(categorize_salary)
"""

# Q41. Pivot Table
"""
pivot = df.pivot_table(values='sales', index='region',
                       columns='year', aggfunc='sum', fill_value=0)
"""

# Q42. Window functions with pandas
"""
df['rolling_avg'] = df['revenue'].rolling(window=3).mean()
df['cumsum'] = df['revenue'].cumsum()
df['rank'] = df['revenue'].rank(ascending=False)
"""

# Q43. Deduplicate records
"""
df.drop_duplicates(subset=['user_id', 'event_date'], keep='last', inplace=True)
"""

# Q44. Date parsing & extraction
"""
df['date'] = pd.to_datetime(df['date'])
df['year']  = df['date'].dt.year
df['month'] = df['date'].dt.month
df['dow']   = df['date'].dt.day_name()
"""

# Q45. Explode nested lists in a column
"""
df['tags'] = df['tags'].str.split(',')
df_exploded = df.explode('tags')
"""


# ===========================================================================
# SECTION 6 — SQL + PYTHON DB CONNECTIVITY (2–3 Years)
# ===========================================================================

# Q46. Connect to PostgreSQL using psycopg2
"""
import psycopg2

conn = psycopg2.connect(
    host="localhost", dbname="mydb",
    user="admin", password="secret"
)
cur = conn.cursor()
cur.execute("SELECT * FROM employees WHERE salary > %s", (50000,))
rows = cur.fetchall()
conn.close()
"""

# Q47. SQLAlchemy ORM — define table + insert
"""
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id   = Column(Integer, primary_key=True)
    name = Column(String)
    age  = Column(Integer)

engine = create_engine("sqlite:///test.db")
Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(User(name="Parth", age=30))
    session.commit()
"""

# Q48. Bulk insert with pandas + SQLAlchemy
"""
df.to_sql('table_name', con=engine, if_exists='append', index=False, chunksize=1000)
"""

# Q49. Common SQL Interview questions (write as Python f-strings for reference)
SQL_QUESTIONS = {
    "Second Highest Salary":
        "SELECT MAX(salary) FROM employees WHERE salary < (SELECT MAX(salary) FROM employees);",

    "Running Total":
        "SELECT id, amount, SUM(amount) OVER (ORDER BY id) AS running_total FROM transactions;",

    "Duplicate Records":
        "SELECT email, COUNT(*) FROM users GROUP BY email HAVING COUNT(*) > 1;",

    "Latest Record per Group":
        """SELECT * FROM orders o
           WHERE event_date = (SELECT MAX(event_date) FROM orders WHERE user_id = o.user_id);""",

    "Rank within Partition":
        "SELECT *, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk FROM employees;",
}


# ===========================================================================
# SECTION 7 — FILE I/O, JSON & API HANDLING (2–3 Years)
# ===========================================================================

# Q50. Read / Write JSON
import json

def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)

def save_json(data, filepath):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

# Q51. Parse nested JSON and flatten
def flatten_json(d, parent_key='', sep='_'):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items
# flatten_json({"a": {"b": 1, "c": 2}, "d": 3}) → {'a_b':1, 'a_c':2, 'd':3}

# Q52. REST API call with retry logic
import urllib.request
import urllib.error

def fetch_with_retry(url, retries=3):
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                return json.loads(response.read().decode())
        except urllib.error.URLError as e:
            print(f"Attempt {attempt+1} failed: {e}")
    raise Exception(f"All {retries} retries failed for {url}")

# Q53. Read large file in chunks (memory efficient)
def process_large_file(filepath, chunk_size=1000):
    """
    import pandas as pd
    for chunk in pd.read_csv(filepath, chunksize=chunk_size):
        process(chunk)  # your logic
    """
    pass

# Q54. Gzip file handling
import gzip

def read_gzip(filepath):
    with gzip.open(filepath, 'rt', encoding='utf-8') as f:
        return f.read()


# ===========================================================================
# SECTION 8 — PYSPARK (3–5 Years — Core DE Skill)
# ===========================================================================

# NOTE: These require: pip install pyspark

# Q55. Create SparkSession and read CSV
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataEngineer") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)
"""

# Q56. PySpark — Filter, Select, GroupBy
"""
from pyspark.sql import functions as F

result = df \
    .filter(F.col("salary") > 50000) \
    .select("name", "department", "salary") \
    .groupBy("department") \
    .agg(F.avg("salary").alias("avg_salary"),
         F.count("*").alias("headcount"))
"""

# Q57. PySpark — Window Functions
"""
from pyspark.sql.window import Window

window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())

df = df.withColumn("rank",       F.rank().over(window_spec)) \
       .withColumn("dense_rank", F.dense_rank().over(window_spec)) \
       .withColumn("row_number", F.row_number().over(window_spec))
"""

# Q58. PySpark — Handle Nulls
"""
df.fillna({"salary": 0, "name": "Unknown"})
df.dropna(subset=["emp_id"])
df.filter(F.col("email").isNotNull())
"""

# Q59. PySpark — UDF (User Defined Function)
"""
from pyspark.sql.types import StringType

@F.udf(StringType())
def salary_band(sal):
    if sal is None: return "Unknown"
    if sal < 50000: return "Low"
    elif sal < 100000: return "Mid"
    return "High"

df = df.withColumn("band", salary_band(F.col("salary")))
"""

# Q60. PySpark — Join types
"""
# Inner, left, right, full, semi, anti
joined = df1.join(df2, df1.id == df2.id, how='left')

# Broadcast join (for small lookup tables)
from pyspark.sql.functions import broadcast
joined = df_large.join(broadcast(df_small), "id")
"""

# Q61. PySpark — Read/Write Parquet & Partitioning
"""
# Write
df.write.mode("overwrite") \
  .partitionBy("year", "month") \
  .parquet("s3://bucket/output/")

# Read with predicate pushdown
df = spark.read.parquet("s3://bucket/output/") \
    .filter(F.col("year") == 2024)
"""

# Q62. PySpark — Explode nested array column
"""
from pyspark.sql.functions import explode, col

df = df.withColumn("tag", explode(col("tags")))
"""

# Q63. PySpark — Repartition vs Coalesce
"""
# Repartition → full shuffle, increases or decreases partitions
df = df.repartition(100, "department")

# Coalesce → no shuffle, only decreases partitions (cheaper)
df = df.coalesce(10)
"""

# Q64. PySpark — Schema definition (avoid inferSchema in production)
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("id",         IntegerType(), True),
    StructField("name",       StringType(),  True),
    StructField("salary",     DoubleType(),  True),
    StructField("department", StringType(),  True),
])

df = spark.read.csv("data.csv", header=True, schema=schema)
"""

# Q65. PySpark — Slowly Changing Dimension (SCD Type 2 logic)
"""
# Detect new/changed records and apply SCD2 via merge/upsert
# Typically done with Delta Lake:

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/delta/employees")
delta_table.alias("target").merge(
    source=df_updates.alias("source"),
    condition="target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
"""


# ===========================================================================
# SECTION 9 — ETL PIPELINE DESIGN (3–5 Years)
# ===========================================================================

# Q66. ETL Pipeline class with logging & error handling
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, name):
        self.name = name

    def extract(self, source_path):
        logger.info(f"[{self.name}] Extracting from {source_path}")
        try:
            import pandas as pd
            return pd.read_csv(source_path)
        except Exception as e:
            logger.error(f"Extract failed: {e}")
            raise

    def transform(self, df):
        logger.info(f"[{self.name}] Transforming data")
        df.dropna(inplace=True)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        return df

    def load(self, df, target_path):
        logger.info(f"[{self.name}] Loading to {target_path}")
        df.to_csv(target_path, index=False)

    def run(self, src, tgt):
        df = self.extract(src)
        df = self.transform(df)
        self.load(df, tgt)
        logger.info(f"[{self.name}] Pipeline completed successfully")

# Q67. Incremental load using watermark
"""
import pandas as pd
from datetime import datetime

def incremental_load(source_path, watermark_path):
    # Read last watermark
    try:
        with open(watermark_path) as f:
            last_load = datetime.fromisoformat(f.read().strip())
    except FileNotFoundError:
        last_load = datetime.min

    df = pd.read_csv(source_path, parse_dates=['updated_at'])
    new_data = df[df['updated_at'] > last_load]

    # Process new_data ...

    # Update watermark
    with open(watermark_path, 'w') as f:
        f.write(datetime.now().isoformat())
    return new_data
"""

# Q68. Retry decorator with exponential backoff
import time
import functools

def retry(max_retries=3, delay=1, backoff=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            wait = delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        raise
                    logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                    wait *= backoff
        return wrapper
    return decorator

@retry(max_retries=3, delay=2, backoff=2)
def fetch_data_from_api(url):
    # API call here
    pass

# Q69. Config-driven pipeline using YAML
"""
import yaml

def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)

# config.yaml structure:
# source:
#   type: csv
#   path: /data/input.csv
# target:
#   type: postgres
#   table: employees
"""

# Q70. Data quality checks
def run_quality_checks(df, checks):
    """
    checks = {
        'no_nulls': ['id', 'name'],
        'unique':   ['id'],
        'positive': ['salary'],
    }
    """
    import pandas as pd
    errors = []
    for col in checks.get('no_nulls', []):
        nulls = df[col].isnull().sum()
        if nulls > 0:
            errors.append(f"NULL check failed: '{col}' has {nulls} nulls")

    for col in checks.get('unique', []):
        dups = df[col].duplicated().sum()
        if dups > 0:
            errors.append(f"Uniqueness check failed: '{col}' has {dups} duplicates")

    for col in checks.get('positive', []):
        negatives = (df[col] < 0).sum()
        if negatives > 0:
            errors.append(f"Positive check failed: '{col}' has {negatives} negative values")

    return errors


# ===========================================================================
# SECTION 10 — PERFORMANCE & OPTIMIZATION (4–5 Years)
# ===========================================================================

# Q71. Profile code execution
import cProfile

def profile_me():
    return sum(range(1_000_000))

# cProfile.run("profile_me()")

# Q72. Memory-efficient iteration with generators
def read_large_csv_generator(filepath):
    with open(filepath) as f:
        header = f.readline().strip().split(',')
        for line in f:
            yield dict(zip(header, line.strip().split(',')))

# Q73. Multiprocessing for CPU-bound tasks
from multiprocessing import Pool

def process_chunk(chunk):
    return [x ** 2 for x in chunk]

def parallel_process(data, workers=4):
    chunk_size = len(data) // workers
    chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
    with Pool(workers) as pool:
        results = pool.map(process_chunk, chunks)
    return [item for sublist in results for item in sublist]

# Q74. Async I/O for concurrent API calls
import asyncio

async def fetch(session, url):
    # async HTTP call (use aiohttp in practice)
    await asyncio.sleep(0.1)
    return {"url": url, "status": 200}

async def fetch_all(urls):
    tasks = [fetch(None, url) for url in urls]
    return await asyncio.gather(*tasks)

# asyncio.run(fetch_all(["http://api1.com", "http://api2.com"]))

# Q75. LRU Cache for expensive function calls
from functools import lru_cache

@lru_cache(maxsize=128)
def expensive_computation(n):
    time.sleep(0.1)  # simulate expensive work
    return n * n

# Q76. Efficient large dictionary lookup
def build_lookup(records, key_field):
    """Build O(1) lookup dict from list of dicts"""
    return {r[key_field]: r for r in records}

# Q77. Chunked batch processing
def process_in_batches(items, batch_size=500):
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        yield batch  # process each batch

# Q78. String formatting performance
# BAD:  result = "" ; for s in lst: result += s
# GOOD: result = "".join(lst)

# Q79. Set operations for large-scale deduplication
def find_new_records(existing_ids, incoming_ids):
    return list(set(incoming_ids) - set(existing_ids))  # O(n)

# Q80. Serialize / Deserialize with pickle (for caching)
import pickle

def cache_object(obj, path):
    with open(path, 'wb') as f:
        pickle.dump(obj, f)

def load_cached_object(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


# ===========================================================================
# SECTION 11 — REAL-WORLD DATA ENGINEERING CHALLENGES (4–5 Years)
# ===========================================================================

# Q81. Deduplicate streaming events (using sliding window)
from collections import deque

class EventDeduplicator:
    def __init__(self, window_seconds=3600):
        self.window = window_seconds
        self.seen = {}

    def is_duplicate(self, event_id, timestamp):
        now = timestamp
        # Evict old entries
        self.seen = {k: v for k, v in self.seen.items() if now - v < self.window}
        if event_id in self.seen:
            return True
        self.seen[event_id] = now
        return False

# Q82. Schema evolution — add new columns gracefully
"""
def safe_get(row, col, default=None):
    return row.get(col, default)

# In PySpark:
if 'new_column' not in df.columns:
    df = df.withColumn('new_column', F.lit(None).cast('string'))
"""

# Q83. Partition pruning — reading only relevant partitions
"""
# Store data as:  s3://bucket/events/year=2024/month=09/day=12/
# Read only:
df = spark.read.parquet("s3://bucket/events/") \
    .filter((F.col("year") == 2024) & (F.col("month") == 9))
"""

# Q84. Handle timezone conversion at scale
from datetime import datetime, timezone
import pytz

def convert_to_utc(dt_str, source_tz_str):
    source_tz = pytz.timezone(source_tz_str)
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt_localized = source_tz.localize(dt)
    return dt_localized.astimezone(pytz.utc)

# convert_to_utc("2026-09-12 13:37:49", "Asia/Calcutta")

# Q85. Idempotent pipeline design
"""
# Key principle: running a pipeline multiple times produces the same result
# Implementation:
#   1. Write to staging table first
#   2. TRUNCATE + INSERT (not just INSERT)
#   3. Use upsert/MERGE for target tables
#   4. Checkpoint states to resume from failure
"""

# Q86. Dead letter queue for bad records
def process_with_dlq(records, processor):
    successful = []
    failed = []
    for record in records:
        try:
            successful.append(processor(record))
        except Exception as e:
            failed.append({"record": record, "error": str(e)})
    return successful, failed

# Q87. Data lineage tracking (simple version)
class LineageTracker:
    def __init__(self):
        self.lineage = []

    def record(self, step, source, target, record_count):
        self.lineage.append({
            "step": step,
            "source": source,
            "target": target,
            "records": record_count,
            "timestamp": time.time()
        })

    def report(self):
        for entry in self.lineage:
            print(f"[{entry['step']}] {entry['source']} → {entry['target']} | {entry['records']} records")

# Q88. Config-based dynamic SQL generation
def build_insert_query(table, columns, values_placeholder=None):
    cols = ", ".join(columns)
    if not values_placeholder:
        values_placeholder = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {table} ({cols}) VALUES ({values_placeholder})"

# Q89. Rolling window aggregation without PySpark (pure Python)
def rolling_average(data, window):
    result = []
    q = deque(maxlen=window)
    for val in data:
        q.append(val)
        result.append(sum(q) / len(q))
    return result
# rolling_average([1,2,3,4,5,6], 3) → [1.0, 1.5, 2.0, 3.0, 4.0, 5.0]

# Q90. Unit test for a transform function
import unittest

def clean_phone(phone):
    import re
    return re.sub(r'\D', '', str(phone))

class TestCleanPhone(unittest.TestCase):
    def test_strips_hyphens(self):      self.assertEqual(clean_phone("123-456-7890"), "1234567890")
    def test_strips_parens(self):       self.assertEqual(clean_phone("(123) 456 7890"), "1234567890")
    def test_already_clean(self):       self.assertEqual(clean_phone("9876543210"), "9876543210")
    def test_empty(self):               self.assertEqual(clean_phone(""), "")

# if __name__ == "__main__": unittest.main()


# ===========================================================================
# SECTION 12 — TRICKY / COMMONLY ASKED PYTHON INTERVIEW QUESTIONS
# ===========================================================================

# Q91. Mutable default argument pitfall
# BAD:
def append_bad(item, lst=[]):
    lst.append(item)
    return lst

# GOOD:
def append_good(item, lst=None):
    if lst is None:
        lst = []
    lst.append(item)
    return lst

# Q92. *args vs **kwargs
def demo(*args, **kwargs):
    print(type(args))    # <class 'tuple'>
    print(type(kwargs))  # <class 'dict'>

# Q93. is vs ==
a = [1, 2, 3]
b = a        # same object
c = [1, 2, 3]  # different object, same value
# a is b → True  |  a is c → False  |  a == c → True

# Q94. Deep copy vs Shallow copy
import copy
original = [[1, 2], [3, 4]]
shallow  = copy.copy(original)    # inner lists still shared
deep     = copy.deepcopy(original) # fully independent

# Q95. List vs Tuple vs Set vs Dict — when to use
"""
List  → ordered, mutable, duplicates allowed         → [1, 2, 3]
Tuple → ordered, immutable, duplicates allowed       → (1, 2, 3)
Set   → unordered, mutable, NO duplicates            → {1, 2, 3}
Dict  → key-value pairs, ordered (Py3.7+), mutable   → {"a": 1}
"""

# Q96. Walrus operator (Python 3.8+)
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
if (n := len(data)) > 5:
    print(f"List is long ({n} elements)")

# Q97. zip, enumerate, any, all
names   = ["Alice", "Bob", "Charlie"]
scores  = [85, 92, 78]
paired  = list(zip(names, scores))       # [('Alice',85), ...]
indexed = list(enumerate(names, start=1)) # [(1,'Alice'), ...]
all_pass = all(s >= 50 for s in scores)  # True
any_fail = any(s < 80 for s in scores)  # True

# Q98. String methods for data cleaning
raw = "  Hello, World!  "
raw.strip()               # remove whitespace
raw.lower()               # lowercase
raw.replace(",", "")      # remove commas
raw.split()               # split on whitespace
"|".join(["a", "b", "c"]) # → "a|b|c"

# Q99. Comprehension types
squares     = [x**2 for x in range(10)]                      # list
even_sq_set = {x**2 for x in range(10) if x % 2 == 0}       # set
sq_dict     = {x: x**2 for x in range(5)}                    # dict
gen_expr    = (x**2 for x in range(10))                      # generator

# Q100. Threading vs Multiprocessing vs AsyncIO — when to use
"""
Threading       → I/O-bound tasks (file, network) — GIL still applies
Multiprocessing → CPU-bound tasks (heavy compute) — bypasses GIL
AsyncIO         → High-concurrency I/O (many simultaneous connections)

DE Rule of thumb:
  - Reading many files / APIs → AsyncIO or Threading
  - Heavy transformations     → Multiprocessing or PySpark
  - Distributed at scale      → PySpark / Dask
"""


# ===========================================================================
# QUICK REFERENCE: TOPICS COVERED
# ===========================================================================
"""
 SECTION  | TOPIC                          | QUESTIONS
 ---------|--------------------------------|----------
    1     | Python Basics                  | Q1  – Q10
    2     | Data Structures & Algorithms   | Q11 – Q20
    3     | Intermediate Python            | Q21 – Q30
    4     | OOP                            | Q31 – Q35
    5     | Pandas & Data Manipulation     | Q36 – Q45
    6     | SQL + DB Connectivity          | Q46 – Q49
    7     | File I/O, JSON & APIs          | Q50 – Q54
    8     | PySpark                        | Q55 – Q65
    9     | ETL Pipeline Design            | Q66 – Q70
   10     | Performance & Optimization     | Q71 – Q80
   11     | Real-World DE Challenges       | Q81 – Q90
   12     | Tricky Interview Questions     | Q91 – Q100
"""
