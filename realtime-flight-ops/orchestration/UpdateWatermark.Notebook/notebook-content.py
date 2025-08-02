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

lastModified = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
import json

payload = json.dumps({"lastModified": lastModified}, indent=2)

folder = "Files/watermarks"
file   = f"{folder}/Watermark.json"

mssparkutils.fs.mkdirs(folder)
mssparkutils.fs.put(file, payload, overwrite=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
