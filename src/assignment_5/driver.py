from src.assignment_5.util import *
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')


spark = create_session()

# 1. create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way
logging.info("1. create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way")

employee_df = create_df(spark, data1, schema1)
department_df = create_df(spark, data2, schema2)
country_df = create_df(spark, data3, schema3)
#
employee_df.show()
department_df.show()
country_df.show()

# 2. Find avg salary of each department
logging.info("2. Find avg salary of each department")
show_avg_salary(employee_df).show()

# 3. Find the employee’s name and department name whose name starts with ‘m’
name_starts_with_m = employee_with_m(employee_df, department_df)
logging.info("3. Find the employee’s name and department name whose name starts with ‘m’")
name_starts_with_m.show()

# 4. Create another new column in  employee_df as a bonus by multiplying employee salary *2
employee_bonus_df = bonus_multiplication(employee_df)
logging.info("4. Create another new column in  employee_df as a bonus by multiplying employee salary *2")
employee_bonus_df.show()

# 5. Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department)
employee_ordered_df = reorder(employee_df)
logging.info("5. Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department)")
employee_ordered_df.show()

# 6. Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way

logging.info("6. Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way")
inner_join_df = inner_join(employee_df, department_df)
logging.info("inner")
inner_join_df.show()
left_join_df = left_join(employee_df, department_df)
logging.info("left")
left_join_df.show()
right_join_df = right_join(employee_df, department_df)
logging.info("right")
right_join_df.show()

# 7. Derive a new data frame with country_name instead of State in employee_df  Eg(11,“james”,”D101”,”newyork”,8900,32)

employee_country_df = country_name_replace(employee_df, country_df)
logging.info("7. Derive a new dataframe with country_name instead of State in employee_df Eg(11,“james”,”D101”,”newyork”,8900,32)")
employee_country_df.show()

# 8. convert all the column names into lowercase from the result of question 7in a dynamic way, add the load_date column with the current date


employee_country_lower_df = lower_column(employee_country_df)
logging.info("8. convert all the column names into lowercase from the result of question 7in a dynamic way, add the load_date column with the current date")
employee_country_lower_df.show()