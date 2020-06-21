# Amazon Kinesis Analytics Token Refresh Sample (1)

## WARNING: This code is for demonstration purposes only.

## To run program (without compilation)

mvn clean compile exec:java -Dexec.mainClass="com.amazonaws.services.kinesisanalytics.StreamingJob"

## To run (previously compiled) program

mvn exec:java -Dexec.mainClass="com.amazonaws.services.kinesisanalytics.StreamingJob"

## Key points

1. Refresh token using [MockTokenSource.java](src/main/java/com/amazonaws/services/kinesisanalytics/sources/MockTokenSource.java)
2. Use broadcast state to fetch existing token in [SampleStatefulOperator.java](src/main/java/com/amazonaws/services/kinesisanalytics/SampleStatefulOperator.java)
3. Make sure to buffer any incoming records that arrive before token is available in process function.