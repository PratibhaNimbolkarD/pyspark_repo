from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import udf

Dataset = [
 ("1234567891234567",),
 ("5678912345671234",),
 ("9123456712345678",),
 ("1234567812341122",),
 ("1234567812341342",)
]

Schema = (
 StructType([
 StructField("card_number" , StringType())
 ])
)
spark = SparkSession.builder.appName('credit_card').getOrCreate()
credit_card_df = spark.createDataFrame(data=Dataset , schema=Schema)
credit_card_df.show()

# reading csv file with inferschema
credit_card_df1 = spark.read.csv("../resource/card_number.csv" , header=True , inferSchema=True)
credit_card_df1.show()
credit_card_df1.printSchema()

# reading csv file with custom schema
credit_card_df_schema = spark.read.format("csv").option("header","true").schema(Schema).load("../resource/card_number.csv")
credit_card_df_schema.show()
credit_card_df_schema.printSchema()

#print number of partitions
number_of_partition = credit_card_df_schema.rdd.getNumPartitions()
print("Number of partitions before repartitioning:", number_of_partition)

# increase the partition by 5
increase_partition = credit_card_df_schema.rdd.repartition(number_of_partition+5)
increase_partition1 = increase_partition.getNumPartitions()
print("Print increased partition: ", increase_partition1)

# Decrease the partition size back to its original partition size
decrease_partition = credit_card_df_schema.rdd.repartition(increase_partition1 - 5)
decrease_partition1 = decrease_partition.getNumPartitions()
print("Back to its original partition size: ", decrease_partition1)

#created a function
def masked_card_number(card_number):
    masked_character = len(card_number)-4
    masked_number = ('*'*masked_character)+card_number[-4:]
    return masked_number

credit_card_udf = udf(masked_card_number , StringType())
#added the column of maskedcardnumber
credit_card_df_udf = credit_card_df_schema.withColumn("masked_card_number" , credit_card_udf(credit_card_df_schema['card_number']))
credit_card_df_udf.show()
