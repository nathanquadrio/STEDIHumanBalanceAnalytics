----
Curated Zone
----
Criteria: Write a Glue Job to join trusted data

Submission Requirements: *customer_trusted_to_curated.py* has a node that inner joins the customer_trusted data with the accelerometer_trusted data by emails. The produced table should have only columns from the customer table.

----
Criteria: Write a Glue Job to create curated data

Submission Requirements: step_trainer_trusted.py has a node that inner joins the step_trainer_landing data with the customer_curated data by serial numbers
*machine_learning_curated.py* has a node that inner joins the step_trainer_trusted data with the accelerometer_trusted data by sensor reading time and timestamps

----
Criteria: Use Athena to query Curated Glue Tables

Submission Requirements: Include screenshots showing various queries run on Athena, along with their results:
Count of customer_curated: 482 rows
Count of machine_learning_curated: 43681 rows

Remark: duplicates dropped using the Transform - SQL Query node with the SELECT DISTINCT query.
