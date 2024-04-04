from pyspark.sql import SparkSession
from pyspark.sql.functions import explode , posexplode , explode_outer , posexplode_outer , current_date , year, month,day
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType,LongType

spark = SparkSession.builder.appName('nested_json_file').getOrCreate()

# Define the schema for the main JSON structure
json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("properties", StructType([
        StructField("name", StringType(), True),
        StructField("storeSize", StringType(), True)
    ]), True),
    StructField("employees", ArrayType(
       StructType([
           StructField("empId", LongType(), True),
           StructField("empName",StringType(),True)
       ])
    ), True)
])


json_path = "../resource/nested_json_file.json"
def read_json(path,schema):
    json_df = spark.read.json(path , multiLine=True , schema=schema)
    return json_df

print("1. Read JSON file provided in the attachment using the dynamic function")
json_df = read_json(json_path , json_schema)
json_df.printSchema()
json_df.show(truncate=False)

print(" flatten the data frame which is a custom schema ")

flatten_df=  json_df.select("*" , "properties.name" , "properties.storesize").drop("properties").select("*", explode("employees").alias("new_employess")).drop("employees").select("*","new_employess.empId" , "new_employess.empName" ).drop("new_employess")
flatten_df.show()

print("3. find out the record count when flattened and when it's not flattened(find out the difference why you are "
      "getting more count)")
print("Count before flatten" ,json_df.count())
print("Count after flatten" ,flatten_df.count())

print("4. Differentiate the difference using explode, explode outer, posexplode functions")
 # Explode the 'employees' array
exploded_df = json_df.select("id", "properties", explode("employees").alias("employee"))
print("Applied explode on the employees array")
exploded_df.show(truncate=False)

# posexplode the employees array
posexploded_df = json_df.select("id", "properties", posexplode("employees").alias("pos", "employee"))
print("Applied posexplode on the employees array")
posexploded_df.show(truncate=False)

#explode_outer on the employees array
exploded_outer_df = json_df.select("id", "properties", explode_outer("employees").alias("employee_outer"))
print("Applied explode_outer on the employees array")
exploded_outer_df.show(truncate=False)

#posexplode_outer on the exployees array
pos_explode_outer = json_df.select("id","properties",posexplode_outer("employees").alias("posexplode_outer" , "employee"))
print("Applied posexplode_outer on the employees array")
pos_explode_outer.show(truncate=False)

print("5. Filter the id which is equal to 1001")
filter_df = flatten_df.filter(flatten_df["empId"]==1001)
filter_df.show()

def toSnakeCase(dataframe):
    for column in dataframe.columns:
        snake_case_col = ''
        for char in column:
            if char.isupper():
                snake_case_col += '_' + char.lower()
            else:
                snake_case_col += char
        dataframe = dataframe.withColumnRenamed(column, snake_case_col)
    return dataframe

snake_case_df = toSnakeCase(flatten_df)
snake_case_df.show()


# 7. Add a new column named load_date with the current date
print("7. Add a new column named load_date with the current date")
load_date_df = snake_case_df.withColumn("load_date", current_date())
load_date_df.show()

# 8. create 3 new columns as year, month, and day from the load_date column
print("8. create 3 new columns as year, month, and day from the load_date column")
year_month_day_df = load_date_df.withColumn("year", year(load_date_df.load_date))\
    .withColumn("month", month(load_date_df.load_date))\
    .withColumn("day", day(load_date_df.load_date))
year_month_day_df.show()