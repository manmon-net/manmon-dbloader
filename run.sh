#!/bin/bash

export ZOOKEEPER_SERVERS=manmon-zookeeper:2181
export DBID=1
export KAFKA_TOPICS=data_min,data_5min,data_15min,data_hour,data_day,data_week,data_month
export KAFKA_SERVERS=manmon-kafka:9092
export KAFKA_GROUPID=manmon
export KAFKA_MAX_POLL_RECORDS=1000
export KAFKA_POLL_FREQUENCY=1000
export DB_CHECK_INTERVAL=1
export TOPICINFO=data_min:1:1:14,data_5min:1:1:60,data_15min:1:1:180,data_hour:1:1:360,data_day:1:1:720,data_week:1:1:3600,data_month:1:1:7200
export EXTRACT_THREADS=4
export DB_THREADS=4

sleep 10

java -jar /home/manmon-dbloader/manmon-dbloader.jar
