### **Question_1**
* Import required modules and create a SparkSession.
* Define schemas for purchase and product data, and create DataFrames.
* Display purchase and product data to inspect the contents.
* Filter purchase data to identify customers who bought only iphone13.
* Perform an inner join to find customers who upgraded from iphone13 to iphone14.
* Group purchase data by customer and count distinct product models.
* Filter customers who bought all models in the new product data.
* Display the resulting DataFrame to show customers who bought all models.

### **Question_2**
* SparkSession established for Spark interaction.
* Dataset defined as a list of tuples containing credit card numbers.
* Schema specified using StructType and StructField.
* DataFrame (credit_card_df) created with defined dataset and schema.
* CSV file "card_number.csv" read with automatic schema inference.
* DataFrame credit_card_df1 created from CSV file with inferred schema.
* Another DataFrame (credit_card_df_schema) created with custom schema applied to the same CSV file.
* Partition management implemented to adjust the number of partitions as needed.

### **Question_3**
* Data representing user activity defined as a list of tuples.
* Schema defined using StructType and StructField to structure the DataFrame.
* DataFrame created from the provided data and schema.
* Function rename_columns defined to rename DataFrame columns.
* Columns renamed in the DataFrame using the rename_columns function.
* Timestamp column converted to TimestampType using withColumn and cast.
* Data filtered to include only entries within the last 7 days using datediff and filter, and actions performed by each user within this period calculated by grouping and counting.




