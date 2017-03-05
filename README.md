### db_backup.py
* if args is "backu", use mysqldump to full backup mysql database and rsync to remote backup store
* if args is "rsync", using rsync to sync binlog to remote backup store
### restore_db_use_bin_log.py
* execute mysql bin log in backuping database to keep up with master database
### restore_to_local.py
* resotre whole mysqldumped sql file to database and execute mysqlbinlog to keep up with master database
