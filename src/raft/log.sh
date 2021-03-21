for i in `seq $1`
do 
	echo "----------------$i--------------"
	tail "./log/$i.log"
done
