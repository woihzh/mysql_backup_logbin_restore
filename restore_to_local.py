#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
   find db backup file and bin logs in local fold which define in configure file, restore to remote host's mysql.
rsync sql.gz file and bin logs to remote host, then  use terminal ssh to execute command on remote host.
check mysql setting of 'max_allowed_packet' if the value little then 16M break, because the product mysql set
mysqldump variable 'max_allowed_packet' 16M
    a configure file 'restore_to_local.conf' is required to setting variables. the format of the conf file is like:
[backup]
fold = /ssss/ssss
sql_prefix = xxxxx
binlog_prefix = master-bin

[remote]
host = 10.13.2.57
user = root
## the password not required
password = kkkkkk
fold = /root/db_resto

[db]
user = root
password = ssssss
database = jkkkjkj

[mail]
smtp_server = smtp.xxxxx.com
login_name = klk_gaojing@xxxxx.com
password = EdI_FMdsfsf
alarm_list = sllklkuo@jkjkj.net sjdsj@sfsfjs.com

[gaojing]
token_default = 8290afe7038825cbe9d479b248e9c6a8i
id_default = 46217
token_message = 300dd439a02c790035897d027e07fff552
id_message = 50889
'''
import re
import os
import json
#import pexpect
import time
import requests
import logging
try:
    import subprocess32 as subprocess
except:
    import subprocess

try:
    import ConfigParser
except:
    import configparser as ConfigParser
from envelopes import Envelope

current_path = os.path.abspath(os.path.dirname(__file__))
error_file = os.path.join(current_path, "restore_local_to_error.log")
logging.basicConfig(filename= error_file,
                    level=logging.INFO,
                    filemode='w',
                    format='%(asctime)s file:%(filename)s fun:%(funcName)s line:%(lineno)d %(levelname)s: %(message)s',
                    )

def read_conf(conf_name=os.path.join(current_path,"restore_to_local.conf")):
    #print(conf_name)
    conf = ConfigParser.ConfigParser()
    conf.read(conf_name)
    return (conf.get('backup', 'fold'),conf.get('backup', 'sql_prefix'),conf.get('backup', 'binlog_prefix'),
            conf.get('remote', 'host'),conf.get('remote', 'user'),conf.get('remote', 'password'),conf.get('remote', 'fold'),
            conf.get('db', 'user'), conf.get('db', 'password'), conf.get('db', 'database'),
            conf.get('mail', 'smtp_server'), conf.get('mail', 'login_name'), conf.get('mail', 'password'),
            conf.get('mail', 'alarm_list'),
            conf.get('gaojing', 'token_default'), conf.get('gaojing', 'id_default'),
            conf.get('gaojing', 'token_message'), conf.get('gaojing', 'id_message')
            )

backup_fold, backup_sql_prefix, backup_binlog_prefix,\
remote_host, remote_user, remote_password, remote_fold,\
db_user, db_password, db_database,\
mail_server, mail_username, mail_password, mail_alarm_list,\
gaojing_default_token, gaojing_default_id, gaojing_message_token, gaojing_message_id = read_conf()

def bash(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = p.communicate()
    if result[1] or p.returncode:
        return_code = 1
    else:
        return_code = 0
    output = "stdout:\n%s\nstderr:\n%s" % (result[0], result[1])
    return {"code": return_code, "output": output}

def send_gaojing(service_id, token, message):
    data = {"service_id": service_id,
            "description": message,
            "event_type": "trigger"
           }
    resp = requests.post("http://gaojing.baidu.com/event/create",
        data = json.dumps(data),
        headers = {
            "servicekey": token,
        },
        timeout=3, verify=False
    )
    result = json.loads(resp.content)
    print("The baidu Gaojing return message is: %s" % result["message"])

def send_mail(server=mail_server, user=mail_username, password=mail_password,
              receive_list=mail_alarm_list, email_subject="Restore db to %s is not working!"% remote_host,
              attach_file=error_file):
    #print(receive_list)
    rec_list = receive_list.split()
    #print(type(rec_list))
    #print(rec_list)
    with open(error_file, 'r') as f:
        content=f.read()
    envelope = Envelope(from_addr=(user),
                        to_addr=rec_list,
                        subject=email_subject,
                        text_body=content
                        )
    envelope.add_attachment(attach_file)
    envelope.send(server, login=user, password=password, tls=True)

def copy_pub_key(host, user, password):
    pass

# check mysqld setting variables 'max_allowed_packet'
def check_mysql_setting(host=remote_host, user=remote_user, db_user=db_user, db_password=db_password):
    cmd = 'ssh %s@%s mysql -u %s -p%s -Nse \\\"show variables like \\\'max_allowed_packet\\\'\;\\\"' % (
        remote_user, remote_host, db_user, db_password)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = p.communicate()
    if result[1] or p.returncode:
        print("error when checking remote mysqld setting: %s" % result[1])
        logging.error("error when checking remote mysqld setting")
        send_mail()
        exit()
    else:
        max_allowed_packet = int(result[0].split()[1])
        print("the remote max_allowed_packet setting is: %s" % max_allowed_packet)
        logging.info("the remote max_allowed_packet setting is: %s" % max_allowed_packet)
        if max_allowed_packet < 16777216:
            print("'max_allowed_packet' setting in remote mysqld was lower then 16M, exit")
            logging.error("'max_allowed_packet' setting in remote mysqld was lower then 16M, exit")
            send_mail()
            exit()
        else:
            print("'max_allowed_packet' check ok!")

def get_last_sql_file(fold, regular_m=backup_sql_prefix):
    files = os.listdir(fold)
    last_file = ''
    for item in files:
        if re.match(r'^%s[\s\S]*?\.sql\.gz$' % regular_m, item) and item > last_file:
            last_file = item
    return last_file

def get_bin_logs(fold, sql_name, regular_m=backup_binlog_prefix):
    re_result = re.match('^.*_(%s\.\d+)_(\d+)\.sql\.gz$' % regular_m, sql_name)
    start_logbin_name = ""
    pos = ""
    #print(fold, sql_name, regular_m)
    if re_result:
        print("find binlog recode in sql backup file name")
        start_logbin_name = re_result.group(1)
        print("the start logbin is: %s" % start_logbin_name)
        pos = re_result.group(2)
        print("pos is: %s" % pos)
    else:
        print("can't find recorded binlog in sql backup file name")
        logging.error("can't find recorded binlog in sql backup file name")

    files = os.listdir(fold)
    bin_logs = []
    for item in files:
        if re.match('^%s\.\d+$' % regular_m, item) and item >= start_logbin_name:
            bin_logs.append(item)
    return bin_logs, pos

if __name__ == '__main__':
    last_backup = get_last_sql_file(backup_fold)
    if not last_backup:
        print("Can't find backup sql file in %s" % backup_fold)
        logging.error("Can't find backup sql file in %s" % backup_fold)
        send_mail()
        exit()
    print('last backup sql file is: %s' % last_backup)
    logging.info('last backup sql file is: %s' % last_backup)
    binlogs, pos = get_bin_logs(backup_fold, last_backup)
    if not binlogs or not pos:
        print("No bin log files under %s" % backup_fold)
        logging.error("No bin log files under %s" % backup_fold)
        send_mail()
        exit()
    binlogs.sort()
    # if bin log date time is old then 2h, exit
    last_binlog_time = os.stat(os.path.join(backup_fold, binlogs[-1])).st_mtime
    if time.time() - last_binlog_time > 7200:
        print("bin log in local backup fold is old then 2 hours, exit")
        logging.error("Trying to restore db to %s from local 51 but bin log in local51 backup \
        fold is old then 2 hours, exit" % remote_host)
        send_mail(email_subject="Fail: local51 restore to %s" % remote_host)
        exit()
    binlog_str = ' '.join(binlogs)
    print("bin logs to restore are: %s" % binlog_str)
    logging.info("bin logs to restore are: %s" % binlog_str)

    # remove remote files under "remote_fold"
    cmd = 'ssh %s@%s rm -rf %s/*' % (remote_user, remote_host, remote_fold)
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    # scp file to remote file
    cmd = 'cd %s; rsync -t %s %s %s@%s:%s/' % (backup_fold, binlog_str, last_backup, remote_user, remote_host,
                                             remote_fold)
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    cmd = 'ssh %s@%s mysql -u %s -p%s -e \\\"drop database if exists %s\;create database %s default character \
    set utf8mb4\;\\\"' % (remote_user, remote_host, db_user, db_password, db_database, db_database)
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    cmd = 'ssh %s@%s zcat %s\|grep -av \\\"SQL SECURITY DEFINER\\\"\|mysql -u %s -p%s %s' % (
        remote_user, remote_host, os.path.join(remote_fold, last_backup), db_user, db_password, db_database)
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    if db_database == "jubao":
        cmd = "ssh %s@%s cd %s \;mysqlbinlog --no-defaults -D \
        --start-position=%s %s \|sed \\\'/\`jubaopen\`@\`172.16.%%.%%\`/s//\`root\`@\`localhost\`/\\\' \
        \|grep -avE \\\'grant\|revoke\|SET PASSWORD\\\' \|mysql -u %s -p%s" % (
            remote_user, remote_host, remote_fold, pos, binlog_str, db_user, db_password)
    else:
        # use sed to replace database name in binlog out
        cmd = "ssh %s@%s cd %s \;mysqlbinlog --no-defaults -D \
        --start-position=%s %s \|sed \\\'/\`jubaopen\`@\`172.16.%%.%%\`/s//\`root\`@\`localhost\`/\\\' \
        \|grep -avE \\\'grant\|revoke\|SET PASSWORD\\\' \|sed \\\'/\`jubao\`/s/jubao/%s/\\\'\|mysql -u %s -p%s" % (
            remote_user, remote_host, remote_fold, pos, binlog_str, db_database, db_user, db_password)
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    #cmd = 'ssh %s@%s rm -rf %s/*' % (remote_user, remote_host, remote_fold)
    #result = bash(cmd)
    #if result["code"] != 0:
    #    print(result["output"])
    #    logging.error(result["output"])
    #    send_mail()
    #    #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
    #    exit()
    print("succeed in restore database %s to %s" % (db_database, remote_host))
    logging.info("succeed in restore database %s to %s" % (db_database, remote_host))
    send_mail(email_subject="Success: in restore database %s to %s" % (db_database, remote_host))
