from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("nirajProject").enableHiveSupport().getOrCreate()
max_id = spark.sql("SELECT max(id) FROM bigdata_nov_2024.person")
m_id = max_id.collect()[0][0]
str(m_id)

query = 'SELECT * FROM person WHERE id >' + str(m_id)

more_data = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("query", query) \
    .load()

# Added new column of email address 
#more_data_with_email = more_data.withColumn('email_address', F.concat(more_data['name'], F.lit('@gmail.com')))


# Show the updated DataFrame with the new column
#more_data_with_email.show()
more_data.show()
more_data.write.mode("append").saveAsTable("bigdata_nov_2024.person")
print("Successfully Load to Hive")

# spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar src/IncreamentalLoadPostgressToHive.py

# df2 = spark.read.csv("path/to/other_file.csv", header=True, inferSchema=True)
# joined_df = df.join(df2, on=["ID"], how="inner")

# df1.write.mode("overwrite").saveAsTable("product.dummy")
# hadoop fs -chmod -R 775 /warehouse/tablespace/external/hive/product.db/emp_info
# sudo -u hdfs hdfs dfs -chmod -R 777 /warehouse/tablespace/external/hive/product.db