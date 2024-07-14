----
Landing Zone
----
Criteria: Use Glue Studio to ingest data from an S3 bucket

Submission Requirements: *customer_landing_to_trusted_LZ.py*, *accelerometer_landing_to_trusted_LZ.py*, and *step_trainer_trusted_LZ.py* Glue jobs have a node that connects to S3 bucket for customer, accelerometer, and step trainer landing zones.

----
Criteria: Manually create a Glue Table using Glue Console from JSON data

Submission Requirements: SQL DDL scripts *customer_landing.sql*, *accelerometer_landing.sql*, and *step_trainer_landing.sql* include all of the JSON fields in the data input files and are appropriately typed (not everything is a string).

----
Criteria: Use Athena to query the Landing Zone. 

Submission Requirements: Include screenshots showing various queries run on Athena, along with their results:
- Count of customer_landing: 956 rows
- The customer_landing data contains multiple rows with a blank shareWithResearchAsOfDate.
- Count of accelerometer_landing: 81273 rows
- Count of step_trainer_landing: 28680 rows
