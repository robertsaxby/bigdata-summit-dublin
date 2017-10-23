# Gaming Simulation
Generates and process scores per user and writes aggregates to BigQuery\
Includes Pub/Sub Injector and Dataflow streaming example 

## Variables
gcpProject="GCP project" \
pubsubTopic="Pub/Sub Topic" \
stagingBucket="gs://bucket_name/staging" \
bigqueryDataset="BigQuery Dataset" \
dataflowJobName="Dataflow Job name" 

## Pub/Sub Injector
mvn exec:java \
-Dexec.mainClass=org.apache.beam.injector.Injector \
-Dexec.args="$gcpProject $(echo $pubsubTopic | awk -F'/' '{print $4}') none none"

## Dataflow
### Run the streaming pipeline for the first time
mvn compile exec:java \
-Dexec.mainClass=org.apache.beam.game.Streaming \
-Dexec.args="--project=$gcpProject \
--tempLocation=$stagingBucket \
--runner=DataflowRunner \
--outputDataset=$bigqueryDataset \
--outputTableName=minute_team_scores \
--topic=$pubsubTopic \
--streaming \
--jobName=$dataflowJobName"

### Update the running streaming pipeline
mvn compile exec:java \
-Dexec.mainClass=org.apache.beam.game.Streaming \
-Dexec.args="--project=$gcpProject \
--tempLocation=$stagingBucket \
--runner=DataflowRunner \
--outputDataset=$bigqueryDataset \
--outputTableName=minute_team_scores \
--topic=$pubsubTopic \
--streaming \
--jobName=$dataflowJobName\
--update"