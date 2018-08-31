# playground_examples

**Requirements:**

1.Internet access to download files from preview.unraveldata.com and hive-testbench from github

2.zip and tar command

3.Python 2.6 and newer (Python 2.6 requires argparse module)


**To run the examples:**

``python playground-examples.py``

**To run part of the example use one of the following arguments:**

[-spark] [-hive] [-workflow] [--spark-steaming] [-impala] [-autoaction]

**By default all server address are using localhost:**

[--hive-host] [--hdfs-master] [--impala-server]

**There are these 3 arguments to controll which query/example to run:**
[--dateset-size] [--impala-query] [--spark-example]

<br/>

**In order to get hive before and after Set these in Clouder Manager (CM) and restart CM:**
1. yarn.scheduler.minimum-allocation-mb to 256 Mib
2. yarn.scheduler.increment-allocation-mb to 128 MiB
