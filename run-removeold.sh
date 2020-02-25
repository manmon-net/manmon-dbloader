#!/bin/bash

export DBID=1
export TOPICINFO=data_min:1:1:14,data_5min:1:1:60,data_15min:1:1:180,data_hour:1:1:360,data_day:1:1:720,data_week:1:1:3600,data_month:1:1:7200

java -cp /home/manmon-dbloader/manmon-dbloader.jar net.manmon.dbloader.RemoveOldTables
