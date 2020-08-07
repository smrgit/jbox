#!/bin/bash

p=$1
d=$2
v=$3

t="$v""T"

echo " "
echo " in matView ... " $1  $2  $3 
echo " "
echo " "

src="$p"."$d"."$v"
echo $src

dest="$p":"$d"."$t"
echo $dest

## -dry_run or nodry_run

bq --quiet query --allow_large_results \
        --destination_table=$dest \
        --replace=true \
        --nouse_legacy_sql \
        --nodry_run \
        "SELECT * FROM $src"


python3 $GIT_HOME/smrgit/jbox/bbq/bbqTableExplore.py -p $p -d $d -t $t

