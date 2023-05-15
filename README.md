# IMC_RT
## Download Apache Kafka:
Go to the Apache Kafka website: https://kafka.apache.org/downloads
Download the latest stable release of Apache Kafka for Windows
## Start ZooKeeper
Start ZooKeeper by running the following command:
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
## Start Kafka
Start Kafka processing using following command:
.\bin\windows\kafka-server-start.bat .\config\server.properties
## Running producer and consumer
Now, move into streaming directory and run producer.py and consumer.py to push and consume message in kafka 