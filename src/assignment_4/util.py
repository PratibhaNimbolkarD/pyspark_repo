from pyspark.sql import SparkSession
from pyspark.sql.functions import explode , posexplode , explode_outer , posexplode_outer , current_date , year, month,day
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType,LongType



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
def spark_session():
    spark = SparkSession.builder.appName('nested_json_file').getOrCreate()
    return spark


def read_json(spark,path,schema):
    json_df = spark.read.json(path , multiLine=True , schema=schema)
    return json_df

print("1. Read JSON file provided in the attachment using the dynamic function")
def reading_json(spark):
    json_df = read_json(spark ,json_path , json_schema)
    json_df.printSchema()
    return json_df
print(" flatten the data frame which is a custom schema ")

def flatten_df(json_df):
    flatten_df=  json_df.select("*" , "properties.name" , "properties.storesize").drop("properties").select("*", explode("employees").alias("new_employess")).drop("employees").select("*","new_employess.empId" , "new_employess.empName" ).drop("new_employess")
    flatten_df.show()
    return flatten_df

print("3. find out the record count when flattened and when it's not flattened(find out the difference why you are "
      "getting more count)")
def json_df_count(json_df):
    print("Count before flatten" ,json_df.count())
    return json_df.count()

def flatten_df_count(json_df):
    flatten_df1 = flatten_df(json_df)
    print("Count after flatten" ,flatten_df1.count())
    return flatten_df1.count()

print("4. Differentiate the difference using explode, explode outer, posexplode functions")
 # Explode the 'employees' array
def expode_df(json_df):
    exploded_df = json_df.select("id", "properties", explode("employees").alias("employee"))
    print("Applied explode on the employees array")
    exploded_df.show(truncate=False)
    return exploded_df

# posexplode the employees array
def posexploded_df(json_df):
    posexploded_df = json_df.select("id", "properties", posexplode("employees").alias("pos", "employee"))
    print("Applied posexplode on the employees array")
    posexploded_df.show(truncate=False)
    return posexploded_df

#explode_outer on the employees array

def exploded_outer_df(json_df):
    exploded_outer_df = json_df.select("id", "properties", explode_outer("employees").alias("employee_outer"))
    print("Applied explode_outer on the employees array")
    exploded_outer_df.show(truncate=False)
    return exploded_outer_df

#posexplode_outer on the exployees array
def pos_explode_outer(json_df):
    pos_explode_outer = json_df.select("id","properties",posexplode_outer("employees").alias("posexplode_outer" , "employee"))
    print("Applied posexplode_outer on the employees array")
    pos_explode_outer.show(truncate=False)
    return pos_explode_outer

print("5. Filter the id which is equal to 1001")
def filter_df(json_df):
    flatten = flatten_df(json_df)
    filter_df = flatten.filter(flatten_df["empId"]==1001)
    filter_df.show()
    return filter_df

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
def load_date_df(snake_case_df):
    load_date_df = snake_case_df.withColumn("load_date", current_date())
    load_date_df.show()
    return load_date_df

# 8. create 3 new columns as year, month, and day from the load_date column
print("8. create 3 new columns as year, month, and day from the load_date column")
def year_month_day_df(snake_case_df):
    load_date_df1 = load_date_df(snake_case_df)
    year_month_day_df = load_date_df1.withColumn("year", year(load_date_df1.load_date))\
    .withColumn("month", month(load_date_df1.load_date))\
    .withColumn("day", day(load_date_df1.load_date))
    year_month_day_df.show()
    return year_month_day_df