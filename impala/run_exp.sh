#!/bin/bash

if [ -z "$1" ]; then
  echo 'Usage: ./run_exp.sh [db_name] [impala_server]'
else
  db_name=$1  
fi

impala_server=$2

for file in ./query[0-9][0-9].sql
do
  #out_file=$(basename $file) | cut -d'.' -f2
  
  file_name=$(basename $file)

  ./clear_cluster_cache.sh
  
  echo executing $file_name >> output.out
  
  SECONDS=0
  
  impala-shell -i $impala_server -d $db_name -f $file_name >> output.out
  
  duration=$SECONDS
  
  echo end execution of $file_name >> output.out
  echo $file_name $duration'sec' >> elapsed_time.out
done
