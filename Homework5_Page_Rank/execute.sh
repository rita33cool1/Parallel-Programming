#!/bin/bash

# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/CalculateAverage/Output/
#hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage /user/shared/CalculateAverage/Input /user/TA/CalculateAverage/Output
#hdfs dfs -cat /user/TA/CalculateAverage/Output/part-*

#if [[ "$2" == "" ]]; then
#	iter=3
#else
#	iter=$2
#fi

error_limit=$3
iter=$1
INPUT_FILE=/user/ta/PageRank/input-$2
OUTPUT_FILE=Page_Rank/output
PARSE_TMP_FILE=Page_Rank/parse_output_tmp
APPEND_TMP_FILE=Page_Rank/append_output_tmp
CALCULATE_TMP_FILE=Page_Rank/calculate_output_tmp
JAR=Page_Rank.jar

hdfs dfs -rm -r $PARSE_TMP_FILE
hdfs dfs -rm -r $APPEND_TMP_FILE
hdfs dfs -rm -r $CALCULATE_TMP_FILE
hdfs dfs -rm -r $OUTPUT_FILE
hadoop jar $JAR page_rank.Page_Rank $INPUT_FILE $PARSE_TMP_FILE $APPEND_TMP_FILE $CALCULATE_TMP_FILE $OUTPUT_FILE $iter $error_limit

hdfs dfs -getmerge $OUTPUT_FILE pagerank_$2.out
