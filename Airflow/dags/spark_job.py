import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

import pandas as pd
from pyspark.sql.functions import col, when, round, format_number, initcap, concat, lit
from pyspark.sql import types
from pyspark.sql import functions as F


##################################################################

spark = SparkSession.builder \
    .appName('spark_job_script') \
    .getOrCreate()

print("\n🟩 spark session running, now reading src data .... \n")

################################################################################

df_bus = spark.read.parquet('gs://ttc_data_lake_ttc-data-analytics/bus_delay_data/*')
df_subway = spark.read.parquet('gs://ttc_data_lake_ttc-data-analytics/subway_delay_data/ttc-subway*')
df_streetcar = spark.read.parquet('gs://ttc_data_lake_ttc-data-analytics/streetcar_delay_data/*')

print("\n🟩 src data read from gcs, now applying transformations ... \n")

# 1. dropping the index column
df_bus = df_bus.drop("__index_level_0__")
df_streetcar = df_streetcar.drop("__index_level_0__")

# 2. Convert common Long Type columns to Integer Type
for col_name in ["Min Delay", "Min Gap", "Vehicle"]:
    df_bus = df_bus.withColumn(col_name, col(col_name).cast("int"))
    df_subway = df_subway.withColumn(col_name, col(col_name).cast("int"))
    df_streetcar = df_streetcar.withColumn(col_name, col(col_name).cast("int"))
    # ↪ `col(col_name).cast("int")` will be the new value for `col_name`

# convert un-common Long Type columns to Integer Type
df_streetcar = df_streetcar.withColumn("Line", col("Line").cast("int"))
df_bus = df_bus.withColumn("Route", col("Route").cast("int"))

# 3. Capitalize the first letter of each word in the "Location" column
df_bus = df_bus.withColumn("Location", initcap("Location"))
df_streetcar = df_streetcar.withColumn("Location", initcap("Location"))
df_subway = df_subway.withColumn("Station", initcap("Station"))

# 4. removing time field from dates (modifying the DataFrame inside the loop won't affect the original DataFrames unless we reassign them)
dataframes = [df_bus, df_streetcar, df_subway]

for i in range(len(dataframes)):
    dataframes[i] = dataframes[i].withColumn('Date', F.to_date(dataframes[i].Date))

# Re-assign the modified DataFrames to their original variable names
df_bus, df_streetcar, df_subway = dataframes


# 5. Rename some columns
df_bus = df_bus \
    .withColumnRenamed('Route', 'Bus_Route') \
    .withColumnRenamed('Vehicle', 'Bus_Number')

df_streetcar = df_streetcar \
    .withColumnRenamed('Line', 'Strcar_Route') \
    .withColumnRenamed('Bound', 'Direction') \
    .withColumnRenamed('Vehicle', 'Strcar_Number')

df_subway = df_subway \
    .withColumnRenamed('Bound', 'Direction') \
    .withColumnRenamed('Vehicle', 'Train_Num')


df_zone_lookup = spark.read.parquet('gs://ttc_data_lake_ttc-data-analytics/subway_delay_data/ttc-delay-code.parquet')
# print(df_zone_lookup.count())
# df_zone_lookup.show()

# Drop the unwanted columns
columns_to_drop = ["Unnamed: 0", "Unnamed: 1", "Unnamed: 4", "Unnamed: 5"]
df_zone_lookup = df_zone_lookup.drop(*columns_to_drop)

# Rename some columns
df_subway = df_subway.withColumnRenamed("Delay Code", "Delay_Code")

df_zone_lookup = df_zone_lookup \
    .withColumnRenamed('Unnamed: 2', 'Delay_Code1') \
    .withColumnRenamed('Unnamed: 6', 'Delay_Code2') \
    .withColumnRenamed('Unnamed: 3', 'Code_Description1') \
    .withColumnRenamed('Unnamed: 7', 'Code_Description2') \

# Remove the first row
df_zone_lookup = df_zone_lookup.filter(df_zone_lookup.Delay_Code1 != "SUB RMENU CODE")

# merging duplicate columns by first seperate dfs for subway and srt and then apply union :

# Select and rename columns using alias
df_zone_lookup_sub = df_zone_lookup.select(
    df_zone_lookup.Delay_Code1.alias("Delay_Code"),
    df_zone_lookup.Code_Description1.alias("Code_Description")
)

# df_zone_lookup_sub.show()

df_zone_lookup_srt = df_zone_lookup.select(
    df_zone_lookup.Delay_Code2.alias("Delay_Code"),
    df_zone_lookup.Code_Description2.alias("Code_Description")
)

df_zone_lookup = df_zone_lookup_sub.unionAll(df_zone_lookup_srt).filter(col("Delay_Code").isNotNull())

df_subway = df_subway.join(df_zone_lookup, df_subway.Code == df_zone_lookup.Delay_Code)

df_subway = df_subway.drop('Code')

df_subway = df_subway.withColumn("Line", 
                                 when(col("Line") == "YU", "Yonge-University (YU)")
                                 .when(col("Line") == "BD", "Bloor-Danforth (BD)")
                                 .when(col("Line") == "SRT", "Scarborough (SRT)")
                                 .when(col("Line") == "SHP", "Sheppard (SHP)")
                                 .otherwise(col("Line")))


# pre-union : Create the list of columns present in the two datasets
common_colums = []
bus_columns = set(df_bus.columns)

for col in df_streetcar.columns:
    if col in bus_columns:
        common_colums.append(col)

# Create a column `service_type` indicating where the data comes from.

df_bus_sel = df_bus \
    .select(common_colums) \
    .withColumn('service_type', F.lit('bus'))

df_streetcar_sel = df_streetcar \
    .select(common_colums) \
    .withColumn('service_type', F.lit('streetcar'))

# Create a new DataFrame containing union of rows of green and yellow DataFrame.
df_road_delay_data = df_bus_sel.unionAll(df_streetcar_sel)


# Create the list of columns present in all three datasets
common_columns = []
bus_columns = set(df_bus.columns)
streetcar_columns = set(df_streetcar.columns)

for col in df_subway.columns:
    if col in bus_columns and col in streetcar_columns:
        common_columns.append(col)

# Create columns with 'service_type' for each DataFrame
df_bus_sel = df_bus.select(common_columns).withColumn('service_type', F.lit('bus'))
df_streetcar_sel = df_streetcar.select(common_columns).withColumn('service_type', F.lit('streetcar'))
df_subway_sel = df_subway.select(common_columns).withColumn('service_type', F.lit('subway'))

# Union the three DataFrames
df_all_delay_data = df_bus_sel.unionAll(df_streetcar_sel).unionAll(df_subway_sel)

# Append ", Toronto" to each value in the 'location' column so that heat map in looker focuses only on Toronto
df_road_delay_data = df_road_delay_data.withColumn("Location", concat(df_road_delay_data["Location"], lit(", Toronto")))

print("\n🟩 transformations applied, loading df to bigQuery ... \n")

###################################################################################################################


# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
temp_bucket = "spark_temp_ttc"
spark.conf.set('temporaryGcsBucket', temp_bucket)

# Saving the data to BigQuery (for overwrite : https://stackoverflow.com/q/72519200/19268172)
df_bus.write.format('bigquery') \
    .option('table', 'ttc_delays_data.bus_delays_table') \
	.mode("overwrite") \
    .save()

df_subway.write.format('bigquery') \
    .option('table', 'ttc_delays_data.subway_delays_table') \
	.mode("overwrite") \
    .save()

df_streetcar.write.format('bigquery') \
    .option('table', 'ttc_delays_data.streetcar_delays_table') \
	.mode("overwrite") \
    .save()

df_road_delay_data.write.format('bigquery') \
    .option('table', 'ttc_delays_data.road_delays_table') \
	.mode("overwrite") \
    .save()

df_all_delay_data.write.format('bigquery') \
    .option('table', 'ttc_delays_data.all_delays_table') \
	.mode("overwrite") \
    .save()

print("\n🟩 Tables created in BigQuery, script ends \n")

'''
gcloud dataproc jobs submit pyspark \
    --cluster=cluster-dc7b \
    --region=us-east4 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://ttc_data_lake_ttc-data-analytics/code/spark_job.py
'''