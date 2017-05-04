#!/bin/bash

for ((i=0;i<200;i++))
do
    TABLE_ID=$i; ../bin/logstash -f ../pipeline/product.conf
done