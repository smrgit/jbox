
f=$1

echo " "
echo $f
date

rm -fr $f.look
rm -fr $f.ht
rm -fr $f.h
rm -fr $f.n

head -1 $f >& $f.h
## ~/scripts/transpose $f.h >& $f.ht
~/git_home/smrgit/jbox/bbq/transpose $f.h >& $f.ht

sed -e '1d' $f > $f.n
wc -l $f
wc -l $f.n

echo " "
echo " "

maxK=`wc -l $f.ht | sed -e '1,$s/ /	/g' | cut -f1`
echo $maxK

echo " "
echo " "

for k in $(eval echo "{1..$maxK}")
    do
        echo $k
        echo " " >> $f.look
        echo $k >> $f.look
        cut -f $k $f.h >> $f.look

        rm -fr $f.t
        rm -fr $f.s
        cut -f $k $f.n >& $f.t
        sort -T /home/sheila_reynolds/scratch $f.t | uniq -c | sort -T /home/sheila_reynolds/scratch -nr >& $f.s
        wc -l $f.s >> $f.look
        head -10 $f.s >> $f.look
        tail -10 $f.s >> $f.look
   done

rm -fr $f.s $f.t $f.n $f.h

