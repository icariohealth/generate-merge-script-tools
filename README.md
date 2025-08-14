## Setup
- Install Java 17+
- Edit the properties file to customize the generated SQL.
## Configuration
| Main Key           | Description                                          |
|---------------|------------------------------------------------------|
| fileName      | Output SQL file name (without .sql extension)        |
| schemaName    | Target schema name in SQL                            |
| tableName     | Target table name                                    |
| columns       | Comma-separated list of table columns                |
| unitKeys      | Comma-separated list of columns used in ON clause    |  
### Example
```config
# File name
fileName=process_outreach_action_event_changes
# Schema name
schemaName=program_outreach
# Table name
tableName=outreach_action_event
# Columns
columns=id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms
# United keys
unitKeys=id, client_id
```
## Run
- Navigate to the folder containing the **mergeScriptTools.jar** file and open a terminal there
- Run the below command
```bash
java -jar mergeScriptTools.jar <properties file name>
```
- The generated SQL file will be placed in output folder
