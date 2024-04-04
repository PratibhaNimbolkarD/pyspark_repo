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


### **Question_4**
* Import required PySpark modules and initialize a Spark session.
* Define a custom schema to match the JSON structure.
* Create a function to read a JSON file with the provided schema.
* Read the JSON file into a DataFrame using the defined function.
* Flatten the DataFrame by selecting necessary columns and exploding the employees array.
* Count the records before and after flattening to observe the difference.
* Apply explode and posexplode functions to the DataFrame to understand their effects.
* Filter the DataFrame to find records with a specific empId.
* Convert column names to snake case using a custom function.
* Add a new column with the current date and extract year, month, and day from it.
* Display the DataFrame after each transformation to observe changes.


### **Question_5**
* Initialize Spark session and define custom schemas for employee_df, department_df, and country_df.
* Create DataFrames employee_df, department_df, and country_df with the defined schemas and sample data.
* Group employee_df by department and calculate the average salary for each department.
* Filter employee_df to retrieve employee names starting with 'm' and join with department_df to get corresponding department names.
* Create a new column 'bonus' in employee_df by multiplying 'salary' by 2.
* Reorder columns in employee_df to match the desired order of column names.
* Define a function dynamic_join to perform inner, left, and right joins between employee_df and department_df based on the specified join type.
* Update the 'State' column in employee_df with corresponding country names and rename it to 'country_name'.
* Define a function column_to_lower to convert column names to lowercase and apply it to the DataFrame.
* Add a new column 'load_date' with the current date to the DataFrame.
