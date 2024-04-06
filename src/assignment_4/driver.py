from src.assignment_4.util import *
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')


spark = create_session()

# 1. Read JSON file provided in the attachment using the dynamic function

read_df=read_file(spark,path,schema)
logging.info("1. Read JSON file provided in the attachment using the dynamic function")
read_df.printSchema()
read_df.show()

#2. flatten the data frame which is a custom schema

flatted_df=flat_df(read_df)
logging.info("2. flatten the data frame which is a custom schema")
flatted_df.show()

# 3. find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count)
logging.info(f"Before Flatten: {read_df.count()} ")
logging.info("3. find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count)")
# logging.info(read_df.count())
logging.info(f"\nAfter Flatten: {flatted_df.count()}")
# logging.info(flatted_df.count())

logging.info("\n4. Differentiate the difference using explode, explode outer, posexplode functions")
# 4. Differentiate the difference using explode, explode outer, posexplode functions
logging.info("explode")
explode_display(read_df).show()
logging.info("explode_outer")
explode_outer_display(read_df).show()
logging.info("pos explode")
posexplode_display(read_df).show()
logging.info("pos explode outer")
posexplode_outer_display(read_df).show()


# 5. Filter the id which is equal to 0001
logging.info("5. Filter the id which is equal to 0001")
check_id(flatted_df).show()

# 6. convert the column names from camel case to snake case
logging.info("6. convert the column names from camel case to snake case")

for column in flatted_df.columns:
    flatted_df = flatted_df.withColumnRenamed(column,new=convert_lower(column))

flatted_df.show()

# 7. Add a new column named load_date with the current date

flat_snake_date_df=add_current_date(flatted_df)
logging.info("7. Add a new column named load_date with the current date")
flat_snake_date_df.show()

# 8. create 3 new columns as year, month, and day from the load_date column
result_df=add_year_month(flat_snake_date_df)
logging.info("8. create 3 new columns as year, month, and day from the load_date column")
result_df.show()