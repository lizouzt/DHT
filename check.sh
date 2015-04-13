#!/bin/sh

#check MySQL_Slave Status
#crontab time 00:10
python=`which python`
echo "============================HeyMan=============================="
read -p "input sessions num:" SNUM
read -p "input clean threshold value:" TNUM

while true;
do
    count=`ps -fe | grep "python" | grep "dhtcollector.py" | grep -v "grep"`
    if [ "$?" != "0" ]; then
        echo "Fuck this: " "$python /Users/Taoz/Git/dht/dhtcollector.py -n $SNUM -t $TNUM"
        $python /Users/Taoz/Git/dht/dhtcollector.py -n $SNUM -t $TNUM
    fi
    sleep 30
done