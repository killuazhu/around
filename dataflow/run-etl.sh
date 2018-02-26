#!/bin/bash -x

YOUR_BUCKET_NAME=dataflow-around-195403
mvn compile exec:java -Dexec.mainClass=com.around.PostDumpFlow -Dexec.args=" --project=kyle-gae-195403 --stagingLocation=gs://$YOUR_BUCKET_NAME/staging/ --tempLocation=gs://$YOUR_BUCKET_NAME/output --runner=DataflowPipelineRunner --jobName=dataflow-intro"
