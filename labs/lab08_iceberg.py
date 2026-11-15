"""Lab 08: Iceberg exercises reconstructed from Python command history.

Original outputs were not saved.
Run with an existing Spark session configured for the lab catalog and S3.
Prerequisite: lab.silver.orders and lab.silver.users must exist.
Each execution appends new synthetic rows.
"""

from pyspark.sql import functions as F


def current_snapshot(table):
    return spark.sql(f"""
        SELECT snapshot_id
        FROM {table}.history
        ORDER BY made_current_at DESC
        LIMIT 1
    """).first()["snapshot_id"]


def inspect_table(table):
    spark.sql(f"DESCRIBE {table}").show()
    spark.sql(f"""
        SELECT snapshot_id, operation, committed_at
        FROM {table}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)
    spark.sql(f"""
        SELECT file_path, record_count
        FROM {table}.files
    """).show(truncate=False)


def compare_versions(table, snapshot):
    spark.sql(f"""
        SELECT 'before append' AS version, COUNT(*) AS rows
        FROM {table} VERSION AS OF {snapshot}
        UNION ALL
        SELECT 'current', COUNT(*)
        FROM {table}
    """).show()


# Orders: inspect, append and time travel.
orders_table = "lab.silver.orders"
inspect_table(orders_table)

orders_before = current_snapshot(orders_table)
orders_count = spark.table(orders_table).count()

new_orders = (
    spark.table(orders_table)
    .limit(3)
    .withColumn("order_id", F.expr("uuid()"))
)

new_orders.writeTo(orders_table).append()

assert spark.table(orders_table).count() == orders_count + 3
compare_versions(orders_table, orders_before)

# Orders: schema evolution.
if "source" not in spark.table(orders_table).columns:
    spark.sql(f"ALTER TABLE {orders_table} ADD COLUMN source STRING")

new_order = (
    spark.table(orders_table)
    .limit(1)
    .withColumn("order_id", F.expr("uuid()"))
    .withColumn("ordered_at", F.current_timestamp())
    .withColumn("quantity", F.lit(1).cast("int"))
    .withColumn("source", F.lit("spark"))
)

new_order.writeTo(orders_table).append()

assert spark.table(orders_table).count() == orders_count + 4

spark.sql(f"""
    SELECT source, COUNT(*) AS orders
    FROM {orders_table}
    GROUP BY source
""").show()

inspect_table(orders_table)

# Compare Iceberg with the previous lab's Parquet output.
warehouse = spark.conf.get("spark.sql.catalog.lab.warehouse").rstrip("/")
bucket_root = warehouse.rsplit("/", 1)[0]

spark.read.parquet(
    f"{bucket_root}/silver/spark/orders.parquet"
).createOrReplaceTempView("orders_parquet")

spark.sql("""
    SELECT 'Parquet' AS storage, COUNT(*) AS orders
    FROM orders_parquet
    UNION ALL
    SELECT 'Iceberg', COUNT(*)
    FROM lab.silver.orders
""").show()

# Users: reuse the table created in Lab 07.
users_table = "lab.silver.users"
inspect_table(users_table)

users_before = current_snapshot(users_table)
users_count = spark.table(users_table).count()

new_users = (
    spark.table(users_table)
    .limit(3)
    .withColumn("user_id", F.expr("uuid()"))
)

new_users.writeTo(users_table).append()

assert spark.table(users_table).count() == users_count + 3
compare_versions(users_table, users_before)

# Users: schema evolution.
if "country" not in spark.table(users_table).columns:
    spark.sql(f"ALTER TABLE {users_table} ADD COLUMN country STRING")

new_user = (
    spark.table(users_table)
    .limit(1)
    .withColumn("user_id", F.expr("uuid()"))
    .withColumn("country", F.lit("US"))
)

new_user.writeTo(users_table).append()

assert spark.table(users_table).count() == users_count + 4

spark.sql(f"""
    SELECT country, COUNT(*) AS users
    FROM {users_table}
    GROUP BY country
""").show()

inspect_table(users_table)
