#!/bin/sh

#check MySQL_Slave Status
#crontab time 00:10
MYSQL_USER="root"
MYSQL_PWD="vpspwd@2013"
MYSQL_SLAVE_LOG="/tmp/check_mysql_slave.log"
EMAIL="tao.z.web@qq.com"
 
MYSQL_PORT=`netstat -na|grep "LISTEN"|grep "3306"|awk -F[:" "]+ '{print $5}'`
MYSQL_IP=`ifconfig eth0|grep "inet addr" | awk -F[:" "]+ '{print $4}'`
MYSQL_STATUS=$(/usr/bin/mysqladmin -u${MYSQL_USER} -p${MYSQL_PWD} status | grep -i "Flush tables")
TORRENTS_COUNT=$(/usr/bin/mysql -u${MYSQL_USER} -p${MYSQL_PWD} -S /var/run/mysqld/mysqld.sock -e "select count(*) from test.torrents")
IO_ENV=`echo $MYSQL_STATUS | awk ' {print $14}'`
QUESTIONS=`echo $MYSQL_STATUS | awk ' {print $6} '`
NOW=$(date -d today +'%Y-%m-%d %H:%M:%S')

echo "============================HeyMan=============================="
echo "Torrents total: `echo $TORRENTS_COUNT | awk ' {print $2} '`"

if [ "$MYSQL_PORT" = "3306" ];then
  echo "mysql is running!"
else
  echo "mysql has shutdown!"
fi

if [ "$IO_ENV" -gt "2" -a "$QUESTIONS" -lg "5000" ];then
  echo "Not need to flush."
else
  echo "[ $NOW ] Mysql is block. Need flush-host!" >> "$MYSQL_STATUS"
  $(/usr/bin/mysqladmin -u${MYSQL_USER} -p${MYSQL_PWD} flush-hosts)
  #cat "$MYSQL_SLAVE_LOG" | mail -s "WARN! ${MySQL_IP}_replicate_error" "$EMAIL"
fi

echo "================================================================"

exit 0
