from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ==========================================
# 1. Create Spark Session
# ==========================================

spark = SparkSession.builder \
    .appName("Employee Incentive Project") \
    .master("local[*]") \
    .getOrCreate()


# ==========================================
# 2. Read Employee Sales Data
# ==========================================

sales_path = "file:///home/vishnupriya/employee_incentive/data/sales.csv"

df = spark.read.csv(
    sales_path,
    header=True,
    inferSchema=True
)

print("\nEmployee Sales Data:")
df.show()


# ==========================================
# 3. Calculate Total Sales for 3 Months
# ==========================================

df = df.withColumn(
    "total_sales_3months",
    col("month1") + col("month2") + col("month3")
)

print("Total Sales:")
df.select(
    "empId",
    "empName",
    "total_sales_3months"
).show()


# ==========================================
# 4. Read Incentive Configuration
# ==========================================

config_path = "file:///home/vishnupriya/employee_incentive/data/incentive_config.csv"

config_df = spark.read.csv(
    config_path,
    header=True,
    inferSchema=True
)

config_row = config_df.first()

incentive_config = {
    "minimum_sales": config_row["minimum_sales"],
    "incentive_amount": config_row["incentive_amount"]
}


# ==========================================
# 5. Create Broadcast Variable
# ==========================================

broadcast_config = spark.sparkContext.broadcast(
    incentive_config
)


# ==========================================
# 6. Calculate Employee Incentive
# ==========================================

def calculate_incentive(row):

    config = broadcast_config.value

    if row["total_sales_3months"] >= config["minimum_sales"]:
        status = "Eligible"
        incentive = config["incentive_amount"]
    else:
        status = "Not Eligible"
        incentive = 0

    return (
        row["empId"],
        row["empName"],
        row["total_sales_3months"],
        status,
        incentive
    )


# ==========================================
# 7. Apply Function Using RDD
# ==========================================

result_rdd = df.rdd.map(calculate_incentive)


# ==========================================
# 8. Create Final DataFrame
# ==========================================

result_df = spark.createDataFrame(
    result_rdd,
    [
        "empId",
        "empName",
        "total_sales_3months",
        "eligible_status",
        "incentive"
    ]
)

print("Final Incentive Result:")
result_df.show()


# ==========================================
# 9. Separate Eligible Employees
# ==========================================

eligible_df = result_df.filter(
    col("total_sales_3months")
    >= broadcast_config.value["minimum_sales"]
)

print("Eligible Employees:")
eligible_df.show()


# ==========================================
# 10. Separate Non-Eligible Employees
# ==========================================

not_eligible_df = result_df.filter(
    col("total_sales_3months")
    < broadcast_config.value["minimum_sales"]
)

print("Not Eligible Employees:")
not_eligible_df.show()


# ==========================================
# 11. Save Eligible Employees
# ==========================================

eligible_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "file:///home/vishnupriya/employee_incentive/output/eligible"
    )


# ==========================================
# 12. Save Non-Eligible Employees
# ==========================================

not_eligible_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "file:///home/vishnupriya/employee_incentive/output/not_eligible"
    )


# ==========================================
# 13. Generate Summary
# ==========================================

eligible_count = eligible_df.count()

not_eligible_count = not_eligible_df.count()

total_incentive = eligible_df \
    .agg({"incentive": "sum"}) \
    .collect()[0][0]


print("\n===================================")
print("       INCENTIVE SUMMARY")
print("===================================")
print("Eligible employees     :", eligible_count)
print("Not eligible employees :", not_eligible_count)
print("Total incentive paid   : ₹", total_incentive)
print("===================================")


# ==========================================
# 14. Stop Spark
# ==========================================

spark.stop()
