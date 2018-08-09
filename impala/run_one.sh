#!/bin/bash

if [ -z "$1" ]; then
  echo 'Usage: ./run_one.sh [db_name] [file_name] [impala_server]'
else
  db_name=$1
fi

file_name=$(basename $2)
impala_server=$3

#./clear_cluster_cache.sh
  
echo executing $file_name
 
SECONDS=0
  
impala-shell -i $impala_server -d $db_name -f $file_name
  
duration=$SECONDS
  
echo end execution of $file_name
echo $file_name $duration'sec'
