from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, current_date, year, month, day, posexplode, explode_outer, posexplode_outer
from pyspark.sql.types import StringType, StructType, IntegerType, StructField, ArrayType

def create_session():
    spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()
    return spark


path="../../resource/nested_json_file.json"

schema=StructType([
    StructField("id",IntegerType(),True),
    StructField("properties",StructType([
        StructField("name",StringType(),True),
        StructField("storeSize",StringType(),True)
    ]),True),
    StructField("employees",ArrayType(StructType([
        StructField("empId",IntegerType(),True),
        StructField("empName",StringType(),True)
    ]),True),True)
])
def read_file(spark,path,schema):
    df=spark.read.json(path,schema=schema,multiLine=True)
    return df

def flat_df(df):
    temp_flat_df = df.withColumn("employee", explode("employees")).drop("employees")

    flatted_df = temp_flat_df.withColumn("empId", temp_flat_df.employee.empId).withColumn("empName",
                                                                                          temp_flat_df.employee.empName) \
        .withColumn("name", temp_flat_df.properties['name']).withColumn("storeSize",
                                                                        temp_flat_df.properties['storeSize']).drop(
        "employee", "properties")
    return flatted_df

def explode_display(read_df):
    return read_df.select(read_df.id, read_df.properties, explode(read_df.employees))
def explode_outer_display(read_df):
    return read_df.select(read_df.id, read_df.properties, explode_outer(read_df.employees))
def posexplode_display(read_df):
    return read_df.select(read_df.id, read_df.properties, posexplode(read_df.employees))
def posexplode_outer_display(read_df):
    return read_df.select(read_df.id, read_df.properties, posexplode_outer(read_df.employees))

def check_id(df):
    return df.filter(df.id == "0001")

def convert_lower(txt):
    new=""
    for i in txt:
        if i.islower():
            new=new+"".join(i)
        else:
            temp=f"_{i}"
            new=new+"".join(temp.lower())
    return new

def add_current_date(df):
    date_df = df.withColumn("load_date", current_date())
    return date_df

def add_year_month(date_df):
    result = date_df.withColumn("year", year(date_df["load_date"])).withColumn("month", month(date_df["load_date"])).withColumn("day", day(date_df["load_date"]))
    return result