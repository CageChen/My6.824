
for i in `seq $1`
do
	echo "---------------$i-------------"
	#go test -run 2A > ./log/$i.log
	go test -run 2A | grep "ok"
done
