#!/bin/bash

for file in ./query*.sql
do
  out_file=$(basename $file) | cut -d'.' -f2
  echo executing $(basename $file)
  impala-shell -d tpcds_bin_partitioned_textfile_100 -f $(basename $file)
done
