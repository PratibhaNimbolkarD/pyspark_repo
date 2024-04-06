import unittest
from src.assignment_4.util import *


class TestAssignment4(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark =SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_read_file(self):
        json_path="../resource/nested_json_file.json"
        test_df=read_file(self.spark,json_path,schema)

        expected_schema=StructType([
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
        expected_df=self.spark.read.json(json_path,schema=expected_schema,multiLine=True)
        self.assertEqual(expected_df.collect(),test_df.collect())
    def test_flat_df(self):
        json_path="../resource/nested_json_file.json"
        df=read_file(self.spark,json_path,schema)
        test_df=flat_df(df)

        expected_data = [
            (1001, 1001, 'Divesh',"ABC Pvt Ltd","Medium"),
            (1001, 1002, 'Rajesh',"ABC Pvt Ltd","Medium"),
            (1001, 1003, 'David',"ABC Pvt Ltd","Medium")
        ]
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True),
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True)
        ])

        expected_df = self.spark.createDataFrame(data=expected_data,schema=expected_schema)
        self.assertEqual(expected_df.collect(), test_df.collect())


    def test_explode_display(self):
        json_path = "../resource/nested_json_file.json"
        df = read_file(self.spark, json_path, schema)
        test_df=explode_display(df)
        self.assertEqual(3,test_df.count())

    def test_explode_outer_display(self):
        json_path = "../resource/nested_json_file.json"
        df = read_file(self.spark, json_path, schema)
        test_df=explode_outer_display(df)
        self.assertEqual(3,test_df.count())

    def test_posexplode_display(self):
        json_path = "../resource/nested_json_file.json"
        df = read_file(self.spark, json_path, schema)
        test_df=posexplode_display(df)
        self.assertEqual(3,test_df.count())

    def test_posexplode_outer_display(self):
        json_path = "../resource/nested_json_file.json"
        df = read_file(self.spark, json_path, schema)
        test_df=posexplode_outer_display(df)
        self.assertEqual(3,test_df.count())


    def test_check_id(self):
        json_path = "../resource/nested_json_file.json"
        df = read_file(self.spark, json_path, schema)
        test_df=check_id(df)
        self.assertEqual(0,test_df.count())

    def test_concert_lower(self):
        json_path = "../resource/nested_json_file.json"
        df = read_file(self.spark, json_path, schema)
        flatted=flat_df(df)
        for column in flatted.columns:
            test_df = flatted.withColumnRenamed(column, new=convert_lower(column))

        expected_data = [
            (1001, 1001, 'Divesh', "ABC Pvt Ltd", "Medium"),
            (1001, 1002, 'Rajesh', "ABC Pvt Ltd", "Medium"),
            (1001, 1003, 'David', "ABC Pvt Ltd", "Medium")
        ]
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("emp_id", IntegerType(), True),
            StructField("emp_name", StringType(), True),
            StructField("name", StringType(), True),
            StructField("store_size", StringType(), True)
        ])
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        self.assertEqual(expected_df.collect(),test_df.collect())

    def test_add_current_date(self):
        json_path = "../resource/nested_json_file.json"
        df = read_file(self.spark, json_path, schema)
        flatted = flat_df(df)
        test_df=add_current_date(flatted)


        expected_data = [
            (1001, 1001, 'Divesh', "ABC Pvt Ltd", "Medium"),
            (1001, 1002, 'Rajesh', "ABC Pvt Ltd", "Medium"),
            (1001, 1003, 'David', "ABC Pvt Ltd", "Medium")
        ]
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True),
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True)
        ])
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        expected_df=expected_df.withColumn("load_date", current_date())
        self.assertEqual(expected_df.collect(),test_df.collect())

    def test_add_year_month(self):
        json_path = "../resource/nested_json_file.json"
        df = read_file(self.spark, json_path, schema)
        flatted = flat_df(df)
        date_df = add_current_date(flatted)
        test_df=add_year_month(date_df)

        expected_data = [
            (1001, 1001, 'Divesh', "ABC Pvt Ltd", "Medium"),
            (1001, 1002, 'Rajesh', "ABC Pvt Ltd", "Medium"),
            (1001, 1003, 'David', "ABC Pvt Ltd", "Medium")
        ]
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True),
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True)
        ])
        date_load_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        date_load_df = date_load_df.withColumn("load_date", current_date())

        expected_df = date_load_df.withColumn("year", year(date_load_df["load_date"])).withColumn("month", month(date_load_df["load_date"])).withColumn("day", day(date_load_df["load_date"]))
        self.assertEqual(expected_df.collect(), test_df.collect())