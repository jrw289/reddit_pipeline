#!/bin/bash

tablename=$1
filepath=$2
timestamp=$(date '+%Y_%m_%d_%H%M%S')
filename="$filepath/${tablename}_${timestamp}.sql" 

echo $filename
pg_dump -U python -h localhost -p 5432 -Fc -d postgres -t $tablename > $filename 
