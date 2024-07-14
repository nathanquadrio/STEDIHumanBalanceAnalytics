----
Trusted Zone
----
Criteria: Configure Glue Studio to dynamically update a Glue Table schema from JSON data

Submission Requirements: Glue Job Python code shows that the option to dynamically infer and update schema is enabled.
To do this, set the Create a table in the Data Catalog and, on subsequent runs, update the schema and add new partitions option to True.

----
Criteria: Use Athena to query Trusted Glue Tables

Submission Requirements: Include screenshots showing various queries run on Athena, along with their results:
Count of customer_trusted: 482 rows
The resulting customer_trusted data has no rows where shareWithResearchAsOfDate is blank.
Count of accelerometer_trusted: 40981 rows
Count of step_trainer_trusted: 14460 rows

----
Criteria: Filter protected PII with Spark in Glue Jobs

Submission Requirements: *customer_landing_to_trusted_TZ.py* has a node that drops rows that do not have data in the sharedWithResearchAsOfDate column.

----
Criteria: Join Privacy tables with Glue Jobs

Submission Requirements: *accelerometer_landing_to_trusted_TZ.py* has a node that inner joins the customer_trusted data with the accelerometer_landing data by emails. The produced table should have only columns from the accelerometer table.
