AWS Lambda function to get data from RDS and push messages to SNS topic

This Lambda function can be configured to run every day at 12AM using cloud watch event triggers.
This will query  the AWS RDS table where the events, messages, mobile no,etc are configured(stored using the http://aswathy-dinarajan
/notification-config and  http://aswathy-dinarajan/notification-config-backend projects)
The DB credentials will be retried from AWSParameter store
and retrieve only those records which matches the current date and month.
The matched records columns will be pushed to an SNS topic

Technologies used : AWS Lambda, aws sdk, Maven, Java 8,AWS SNS,AWS Parameter store
