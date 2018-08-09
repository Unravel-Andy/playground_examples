#!/bin/bash

for i in 101 102 103 104 105
do
ssh root@172.36.1.$i 'echo 3 > /proc/sys/vm/drop_caches'; echo 'clearing cache for 172.36.1.'$i &
done
wait
