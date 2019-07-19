#!/bin/bash


inFile=$1

sed -e '1,$s/        /	/g' $inFile | \
    sed -e '1,$s/           /	/g' | \
    sed -e '1,$s/          /	/g' | \
    sed -e '1,$s/         /	/g' | \
    sed -e '1,$s/       /	/g' | \
    sed -e '1,$s/     /	/g' | \
    sed -e '1,$s/    /	/g' | \
    sed -e '1,$s/   /	/g' | \
    sed -e '1,$s/  /	/g' | \
    sed -e '1,$s/ /	/g' | \
    sed -e '1,$s/		/	/g' | sed -e '1,$s/^	//g' >& $inFile.t


