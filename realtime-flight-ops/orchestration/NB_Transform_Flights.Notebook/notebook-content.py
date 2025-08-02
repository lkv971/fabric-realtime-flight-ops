# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d2e44b41-81d0-4219-8883-3944df8eb261",
# META       "default_lakehouse_name": "LH_Flights",
# META       "default_lakehouse_workspace_id": "c7b896c1-9d81-4a71-be35-ff9535ac18c1",
# META       "known_lakehouses": [
# META         {
# META           "id": "d2e44b41-81d0-4219-8883-3944df8eb261"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

airports_schema = StructType([
    StructField("code", StringType()),
    StructField("icao", StringType()),
    StructField("name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("elevation", IntegerType()),
    StructField("url", StringType()),
    StructField("time_zone", StringType()),
    StructField("city_code", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("county", StringType()),
    StructField("type", StringType()) 
])

airports_data_path = "Files/raw/airports.csv"

df_airports = spark.read.csv(airports_data_path, schema = airports_schema, header = True)

window_airports = Window.orderBy("code")

df_dim_airports = df_airports.withColumn("AirportKey", row_number().over(window_airports))\
                             .filter(col("code").isNotNull()) \
                             .select("AirportKey",
                                 col("code").alias("IATACode"),
                                 col("icao").alias("ICAOCode"),
                                 col("name").alias("AirportName"),
                                 col("latitude").alias("Latitude"),
                                 col("longitude").alias("Longitude"),
                                 col("elevation").alias("Elevation"),
                                 col("time_zone").alias("TimeZone"),
                                 col("country").alias("Country"),
                                 col("city").alias("City"),
                                 col("state").alias("State"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

airlines_schema = StructType([
    StructField("iata_code", StringType()),
    StructField("icao_code", StringType()),
    StructField("name", StringType()),
    StructField("alias", StringType()),
    StructField("country", StringType())
])

airlines_data_path = "Files/raw/airlines.csv"

df_airlines = spark.read.csv(airlines_data_path, schema = airlines_schema, header = True)

window_airlines = Window.orderBy("iata_code")

df_dim_airlines = df_airlines.withColumn("AirlineKey", row_number().over(window_airlines))\
                             .filter(col("iata_code").isNotNull())\
                             .select("AirlineKey",
                                 col("iata_code").alias("IATACode"),
                                 col("icao_code").alias("ICAOCode"),
                                 col("name").alias("AirlineName"),
                                 "Country")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = "20250701"
end_date   = "20501231"

df_date_range = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])
df_dates = df_date_range.select(
    explode(sequence(to_date(col("start_date"), "yyyyMMdd"),
                     to_date(col("end_date"), "yyyyMMdd"),
                     expr("interval 1 day"))).alias("Date"))

window_dates = Window.orderBy("Date")

df_dim_dates = df_dates.withColumn("Year", year(col("Date"))) \
                   .withColumn("DateKey", row_number().over(window_dates)) \
                   .withColumn("Month", month(col("Date"))) \
                   .withColumn("Day", dayofmonth(col("Date"))) \
                   .withColumn("MonthName", date_format(col("Date"), "MMMM")) \
                   .withColumn("DayName", date_format(col("Date"), "EEEE")) \
                   .select("DateKey", "Date", "Year", "Month", "Day", "MonthName", "DayName") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_flights = spark.read.table("flights")

df_fact_flights = df_flights.filter((col("Airline").isNotNull()) & (col("Airline") != "empty"))\
                            .withColumn("Airline", regexp_replace(col("Airline"), "Air China LTD", "Air China"))\
                            .withColumn("DepartureDelay", col("DepartureDelay").cast(IntegerType()))\
                            .withColumn("ArrivalDelay", col("ArrivalDelay").cast(IntegerType()))\
                            .withColumn("FlightDate", to_date(col("FlightDate")))\
                            .withColumn("FlightNumber", concat(col("AirlineIATA"), col("FlightNumber"))) \
                            .join(df_dim_dates, df_dim_dates["Date"] == col("FlightDate"), how = "left")\
                            .join(df_dim_airlines, df_dim_airlines["IATACode"] == col("AirlineIATA"), "left")\
                            .join(df_dim_airports.alias("dep"), col("dep.IATACode") == col("DepartureIATA"), "left")\
                            .withColumnRenamed("AirportKey", "DepartureAirportKey")\
                            .join(df_dim_airports.alias("arr"), col("arr.IATACode") == col("ArrivalIATA"), "left")\
                            .withColumnRenamed("AirportKey", "ArrivalAirportKey") \
                            .select("IngestedAt", 
                                    "FlightDate", 
                                    col("DepartureSched").alias("ScheduledDeparture"),
                                    col("ArrivalSched").alias("ScheduledArrival"),
                                    col("DepartureDelay").cast(IntegerType()),
                                    col("ArrivalDelay").cast(IntegerType()),
                                    "DepartureAirportKey",
                                    "ArrivalAirportKey",
                                    "AirlineKey",
                                    "FlightNumber",
                                    "Status")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_airports = df_dim_airports.dropDuplicates(subset=["IATACode"])
df_dim_airlines = df_dim_airlines.dropDuplicates(subset=["IATACode"])
df_dim_dates = df_dim_dates.dropDuplicates(subset=["Date"])
df_fact_flights = df_fact_flights.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

overwrite_tables = {
    "dimairlines": df_dim_airlines,
    "dimairports": df_dim_airports,
    "dimdates": df_dim_dates
}

append_tables = {
    "factflights": df_fact_flights
}

key_cols = ["FlightDate", "FlightNumber"]

def make_merge_condition(keys):
    return " AND ".join([f"t.{c} = s.{c}" for c in keys])

for table_name, append_df in append_tables.items():
    try:
        target = DeltaTable.forName(spark, table_name)
        condition = make_merge_condition(key_cols)
        (target.alias("t").merge(append_df.alias("s"), condition).whenMatchedDelete() .execute())
        (target.alias("t").merge(append_df.alias("s"), condition).whenNotMatchedInsertAll().execute())
        print(f"Table {table_name} upserted successfully using key: {key_cols}")
    except Exception as e:
        if "is not a Delta table" in e.desc:
            append_df.write.mode("overwrite").saveAsTable(f"{table_name}")
            print(f"Created new Delta table {table_name}")
        else:
            print(f"Error upserting '{table_name}': {e}")
            
for table_name, overwrite_df in overwrite_tables.items():
    try:
        overwrite_df.write.mode("overwrite").saveAsTable(f"{table_name}")
        print(f"Tables {table_name} overwritten successfully")
    except Exception as e:
        print(f"Overwritting error at {table_name}:{e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
