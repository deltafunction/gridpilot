#!/bin/sh

#
# update_mysql_accounts.sh
#
# Script to synchronize the mysql access rights with virtual organization.
# All users get a database created, carrying the name of their certificate
# subject (with / replaced by | and spaces with _). Their login name is
# the cksum of their certificate (printf "$subject" | cksum).
# The database is world readable and modifyable by the user.
#
# Frederik Orellana, Geneva, September 2006
#

VO_URL="https://www.gridfactory.org/vos/db_users.txt"
ROOT_PASSWORD=""
# Set if you have local catalog(s)
LOCAL_REPLICAS=""
LOCAL_CATALOG="gridpilot"

#vo=`curl $VO_URL | grep Orellana`
vo=`curl --insecure $VO_URL`

# first some necessary entries
# INSERT INTO db VALUES ('%', '', '', 'Y', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N');
# INSERT INTO db VALUES ('%', 'localreplicas', 'dq2user', 'Y', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N');
# INSERT INTO db VALUES ('%', 'atlas_ch', 'dq2user', 'Y', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N');

echo
echo "----------------- new sync ---------------"
echo

echo $vo | sed 's/\" \"/\n/g' | sed 's| \/|\n/|g' | sed 's/\"//g' | grep -r -v '^#' | while read subject
do
  dbName=`printf "$subject" | tr " " "_" | tr "/" "|" | tr "@" "_" | tr "." "_" | sed 's/\"//g' | sed 's/^|//g'`
  dbName=`echo ${dbName:0:64}`
  userHash=`printf "$subject" | cksum | awk '{print $1}'`
  echo "subject: $subject"
  echo "user hash: $userHash"
  # first the user databases
  echo "db: $dbName"
  mysql -uroot -p$ROOT_PASSWORD $dbName -e"SHOW TABLES;" >&/dev/null
  if [ $? -ne 0 ]; then
    echo "database $dbName not found, creating..."
    mysql -uroot -p$ROOT_PASSWORD -e"CREATE DATABASE \"$dbName\";"
    echo "creating and granting privileges to user $userHash"
    mysql -uroot -p$ROOT_PASSWORD -e"GRANT ALL ON \"$dbName\".* TO '$userHash'@'%' REQUIRE SUBJECT '$subject';"
    # comment this out if you don't want world readable databases
    mysql -uroot -p$ROOT_PASSWORD -e"INSERT INTO mysql.db (host,db,select_priv) VALUES('%','$dbName','y');"
    if [ "$LOCAL_REPLICAS" != "" ]; then
      # now the virtual file catalog 'localreplicas' (synchronized with LFC)
      mysql -uroot -p$ROOT_PASSWORD -e"SHOW GRANTS FOR '$userHash'@'%';" | grep \"$LOCAL_REPLICAS\" > /dev/null
      if [ $? -ne 0 ]; then
        echo "granting privileges to user $userHash on $LOCAL_REPLICAS"
        mysql -uroot -p$ROOT_PASSWORD -e"GRANT ALL ON \"$LOCAL_REPLICAS\".* TO '$userHash'@'%' REQUIRE SUBJECT '$subject';"
      fi
    fi
    if [ "$LOCAL_CATALOG" != "" ]; then
      # now the  shared file catalog
      mysql -uroot -p$ROOT_PASSWORD -e"SHOW GRANTS FOR '$userHash'@'%';" | grep \"$LOCAL_CATALOG\" > /dev/null
      if [ $? -ne 0 ]; then
        echo "granting privileges to user $userHash on $LOCAL_CATALOG"
        mysql -uroot -p$ROOT_PASSWORD -e"GRANT ALL ON \"$LOCAL_CATALOG\".* TO '$userHash'@'%' REQUIRE SUBJECT '$subject';"
      fi
    fi
    echo
    echo
  fi
done
mysql -uroot -p$ROOT_PASSWORD -e"FLUSH PRIVILEGES;"
