#!/bin/bash


cur_path=$(cd `dirname $0`; pwd)
log_name=$cur_path/restore_log.log
bk_fold=/test_db_backup
remote_fold=/jubao-db-backup
bin_log=master-bin
echo "$(date)" >> $log_name
ssh db_sync_KK@121.40.204.200 ls $remote_fold | grep jubao_.*\.sql.gz | tee -a $cur_path/remote_sql_files.mark >> $log_name 2>&1
if [[ "$?" != "0" ]]; then
    echo "Error on local125 when list remote back up sql files"
    mail -s "Error on local125 when restore db" < $log_name
    exit 1
fi
last_remote_sql_file=$(sort $cur_path/remote_sql_files.mark | tail -1)
echo "$(date): last remote sql file is: $last_remote_sql_file" | tee -a $log_name

if [[ -z $last_remote_sql_file ]]; then


    echo "$(date): No sql backup file on remote server" |tee -a $log_name
    mail -s "Error on local125 when restore db" < $log_name
    exit 1
fi

echo -n $(date) >> $log_name
rsync -a db_sync_KK@121.40.204.200:$remote_fold/$last_remote_sql_file :$remote_fold/${bin_log}.* $bk_fold/ >> $log_name 2>&1
if [[ "$?" != "0" ]]; then
    echo "$(date): Error on local125 when rsync remote files to local" | tee -a $log_name
    mail -s "Error on local125 when rsync remote files to local" < $log_name
    exit 1
fi



