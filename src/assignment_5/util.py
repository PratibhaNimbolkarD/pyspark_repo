from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import current_date, avg
from pyspark.sql.types import StringType, StructType, IntegerType, StructField, ArrayType


def create_session():
    spark = SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()
    return spark


data1 = ((11, "james", "D101", "ny", 9000, 34),
         (12, "michel", "D101", "ny", 8900, 32),
         (13, "robert", "D102", "ca", 7900, 29),
         (14, "scott", "D103", "ca", 8000, 36),
         (15, "jen", "D102", "ny", 9500, 38),
         (16, "jeff", "D103", "uk", 9100, 35),
         (17, "maria", "D101", "ny", 7900, 40))

schema1 = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("State", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])

data2 = (("D101", "sales"),
         ("D102", "finance"),
         ("D103", "marketing"),
         ("D104", "hr"),
         ("D105", "support"))
schema2 = StructType([
    StructField("dept_id", StringType(), True),
    StructField("dept_name", StringType(), True)
])

data3 = (("ny", "newyork"),
         ("ca", "California"),
         ("uk", "Russia"))

schema3 = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)
])


def create_df(spark, data, schema):
    new = spark.createDataFrame(data=data, schema=schema)
    return new


def show_avg_salary(df):
    win_spec = Window.partitionBy("department")
    return df.withColumn("avg", avg(col=("salary")).over(win_spec)).select("department", "avg").distinct()

def employee_with_m(df1,df2):
    employee_with_m = df1.filter(df1.employee_name.startswith('m'))
    name_starts_with_m = employee_with_m.join(df2, employee_with_m["department"] == df2["dept_id"],
                                              "inner").select(employee_with_m.employee_name, df2.dept_name)
    return name_starts_with_m

def bonus_multiplication(df):
    bonus_df = df.withColumn("bonus", df.salary * 2)
    return bonus_df

def reorder(df):
    ordered_df = df.select("employee_id", "employee_name", "salary", "State", "Age", "department")
    return ordered_df


def inner_join(df1,df2):
    return df1.join(df2,df1.department==df2.dept_id ,"inner")
def left_join(df1,df2):
    return df1.join(df2,df1.department==df2.dept_id ,"left")
def right_join(df1,df2):
    return df1.join(df2,df1.department==df2.dept_id ,"right")

def country_name_replace(employee_df,country_df):
    employee_country_df = employee_df.join(country_df, employee_df.State == country_df.country_code).drop(
        "country_code", "state").select("employee_id", "employee_name", "department", "country_name", "salary", "Age")
    return employee_country_df

def lower_column(df):
    for column in  df.columns:
        df=df.withColumnRenamed(column,column.lower())
    return df.withColumn("load_date",current_date())