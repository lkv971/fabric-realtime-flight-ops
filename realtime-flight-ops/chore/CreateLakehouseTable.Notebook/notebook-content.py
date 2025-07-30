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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

flights_schema = StructType([
    StructField("FlightDate", TimestampType()),
    StructField("Status", StringType()),
    StructField("DepartureIATA", StringType()),
    StructField("DepartureSched", TimestampType()),
    StructField("DepartureDelay", StringType()),
    StructField("ArrivalIATA", StringType()),
    StructField("ArriivalSched", TimestampType()),
    StructField("ArrivalDelay", StringType()),
    StructField("Airline", StringType()),
    StructField("FlightNumber", StringType()),
    StructField("IngestedAt", TimestampType())
])

df_flights = spark.createDataFrame([], schema=flights_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_flights.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("flights")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
