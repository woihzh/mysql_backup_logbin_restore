#!/usr/bin/env bash
cur_path=$(cd `dirname $0`; pwd)
log_name=$cur_path/restore_log.log
bk_fold=/test_db_backup
remote_fold=/jubao-db-backup
bin_log=master-bin


echo "$(date)" > $log_name