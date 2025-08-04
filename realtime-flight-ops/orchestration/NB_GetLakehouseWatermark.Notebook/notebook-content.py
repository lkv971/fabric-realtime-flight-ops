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

from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row = spark.sql("SELECT MAX(IngestedAt) AS MaxTime FROM LH_Flights.flights").first()

max_ts = row["MaxTime"] if row and row["MaxTime"] else "1970-01-01T00:00:00.000Z"

if max_ts != "1970-01-01T00:00:00.000Z":
    max_ts = max_ts.isoformat() + "Z"

mssparkutils.notebook.exit(max_ts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
