import unittest

from pyspark.sql.types import FloatType

from src.assignment_5.util import *


class TestAssignment5(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()


    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_create_df(self):
        data3 = (("ny", "newyork"),
                 ("ca", "California"),
                 ("uk", "Russia"))

        schema3 = StructType([
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True)
        ])

        df = create_df(self.spark, data3, schema3)
        self.assertEqual(3,df.count())

    def test_show_avg(self):
        df=self.spark.createDataFrame(data1,schema1)
        test_df=show_avg_salary(df)
        expected_schema = StructType([
            StructField("department", StringType(), True),
            StructField("avg", FloatType(), True)
        ])
        expected_data=[("D101",8600.0),
                       ("D102",8700.0),
                       ("D103",8550.0)]

        expected_df = create_df(self.spark, expected_data, expected_schema)
        self.assertEqual(test_df.collect(), expected_df.collect())

    def test_employee_with_m(self):
        emp_df=create_df(self.spark,data1,schema1)
        dep_df2=create_df(self.spark,data2,schema2)
        test_df=employee_with_m(emp_df,dep_df2)

        expected_schema = StructType([
            StructField("employee_name", StringType(), True),
            StructField("dept_name", StringType(), True)
        ])
        expected_data = [("michel", "sales"),
                         ("maria", "sales")]

        expected_df = create_df(self.spark, expected_data, expected_schema)
        self.assertEqual(test_df.collect(),expected_df.collect())

    def test_bonus_multiplication(self):
        emp_df = create_df(self.spark, data1, schema1)
        test_df=bonus_multiplication(emp_df)

        expected_data = ((11, "james", "D101", "ny", 9000, 34,18000),
                 (12, "michel", "D101", "ny", 8900, 32,17800),
                 (13, "robert", "D102", "ca", 7900, 29,15800),
                 (14, "scott", "D103", "ca", 8000, 36,16000),
                 (15, "jen", "D102", "ny", 9500, 38,19000),
                 (16, "jeff", "D103", "uk", 9100, 35,18200),
                 (17, "maria", "D101", "ny", 7900, 40,15800))

        expected_schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("employee_name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("State", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("Age", IntegerType(), True),
            StructField("bonus", IntegerType(), True)
        ])
        expected_df = create_df(self.spark, expected_data, expected_schema)
        self.assertEqual(test_df.collect(), expected_df.collect())


    def test_reorder(self):
        emp_df = create_df(self.spark, data1, schema1)
        test_df=reorder(emp_df)
        expected_data = ((11, "james", "D101", "ny", 9000, 34),
                 (12, "michel", "D101", "ny", 8900, 32),
                 (13, "robert", "D102", "ca", 7900, 29),
                 (14, "scott", "D103", "ca", 8000, 36),
                 (15, "jen", "D102", "ny", 9500, 38),
                 (16, "jeff", "D103", "uk", 9100, 35),
                 (17, "maria", "D101", "ny", 7900, 40))

        expected_schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("employee_name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("State", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("Age", IntegerType(), True)
        ])

        expected_df = create_df(self.spark, expected_data, expected_schema)
        expected_df=expected_df.select("employee_id", "employee_name", "salary", "State", "Age", "department")
        self.assertEqual(test_df.collect(), expected_df.collect())

    def test_inner_join(self):
        emp_df = create_df(self.spark, data1, schema1)
        dep_df2 = create_df(self.spark, data2, schema2)

        test_df=inner_join(emp_df,dep_df2)
        expected_df=emp_df.join(dep_df2,emp_df.department==dep_df2.dept_id ,"inner")

        self.assertEqual(test_df.collect(),expected_df.collect())


    def test_left_join(self):
        emp_df = create_df(self.spark, data1, schema1)
        dep_df2 = create_df(self.spark, data2, schema2)

        test_df=left_join(emp_df,dep_df2)
        expected_df=emp_df.join(dep_df2,emp_df.department==dep_df2.dept_id ,"left")

        self.assertEqual(test_df.collect(),expected_df.collect())

    def test_right_join(self):
        emp_df = create_df(self.spark, data1, schema1)
        dep_df2 = create_df(self.spark, data2, schema2)

        test_df=right_join(emp_df,dep_df2)
        expected_df=emp_df.join(dep_df2,emp_df.department==dep_df2.dept_id ,"right")

        self.assertEqual(test_df.collect(),expected_df.collect())

    def test_country_name_replace(self):
        emp_df = create_df(self.spark, data1, schema1)
        con_df2 = create_df(self.spark, data3, schema3)
        test_df=country_name_replace(emp_df,con_df2)

        expected_data = ((13, "robert", "D102", "California", 7900, 29),
                         (14, "scott", "D103", "California", 8000, 36),
                         (11, "james", "D101", "newyork", 9000, 34),
                         (12, "michel", "D101", "newyork", 8900, 32),
                         (15, "jen", "D102", "newyork", 9500, 38),
                         (17, "maria", "D101", "newyork", 7900, 40),
                         (16, "jeff", "D103", "Russia", 9100, 35))

        expected_schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("employee_name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("Age", IntegerType(), True)
        ])

        expected_df = create_df(self.spark, expected_data, expected_schema)
        self.assertEqual(expected_df.collect(),test_df.collect())

    def test_lower_column(self):
        emp_df = create_df(self.spark, data1, schema1)
        con_df2 = create_df(self.spark, data3, schema3)
        new_emp_df=country_name_replace(emp_df, con_df2)
        test_df=lower_column(new_emp_df)

        expected_data = ((13, "robert", "D102", "California", 7900, 29),
                         (14, "scott", "D103", "California", 8000, 36),
                         (11, "james", "D101", "newyork", 9000, 34),
                         (12, "michel", "D101", "newyork", 8900, 32),
                         (15, "jen", "D102", "newyork", 9500, 38),
                         (17, "maria", "D101", "newyork", 7900, 40),
                         (16, "jeff", "D103", "Russia", 9100, 35))

        expected_schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("employee_name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("age", IntegerType(), True)
        ])

        new_df = create_df(self.spark, expected_data, expected_schema)
        expected_df=new_df.withColumn("load_date",current_date())
        self.assertEqual(test_df.collect(),expected_df.collect())


if __name__ == '__main__':
    unittest.main()