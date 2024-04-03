from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import StructType, IntegerType, StructField, StringType

spark_session = SparkSession.builder.appName('assignment 1').getOrCreate()

purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])
purchase_data = [(1, "iphone13"),
                 (1, "dell i5 core"),
                 (2, "iphone13"),
                 (2, "dell i5 core"),
                 (3, "iphone13"),
                 (3, "dell i5 core"),
                 (1, "dell i3 core"),
                 (1, "hp i5 core"),
                 (1, "iphone14"),
                 (3, "iphone14"),
                 (4, "iphone13")]

product_schema = StructType([
    StructField("product_model", StringType(), True)
])
product_data = [("iphone13",), ("dell i5 core",), ("dell i3 core",), ("hp i5 core",), ("iphone14",)]

purchase_data_df = spark_session.createDataFrame(purchase_data, purchase_schema)
product_data_df = spark_session.createDataFrame(product_data, product_schema)

print("Purchase data")
purchase_data_df.show()

print("Product data")
product_data_df.show()

print("2. Find the customers who have bought only iphone13 ")
only_iphone13 = purchase_data_df.filter(purchase_data_df["product_model"] == "iphone13")
only_iphone13.show()

print("3. Find customers who upgraded from product iphone13 to product iphone14")
only_iphone14 = purchase_data_df.filter(col("product_model") == "iphone14")
upgraded_iphone14 = only_iphone13.join(only_iphone14, "customer", "inner")
upgraded_iphone14.show()

print("4.Find customers who have bought all models in the new Product Data")
customer_models_count = purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("model_count"))
all_models = customer_models_count.filter(customer_models_count.model_count == product_data_df.count()).select("customer")
all_models.show()