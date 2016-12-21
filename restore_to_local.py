#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import os
import json
import time
import requests
import logging

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
            conf.get('db', 'host'), conf.get('db', 'user'), conf.get('db', 'password'), conf.get('db', 'port'),
            conf.get('mail', 'smtp_server'), conf.get('mail', 'login_name'), conf.get('mail', 'password'),
            conf.get('mail', 'alarm_list'),
            conf.get('gaojing', 'token_default'), conf.get('gaojing', 'id_default'),
            conf.get('gaojing', 'token_message'), conf.get('gaojing', 'id_message')
            )

backup_fold, backup_sql_prefix, backup_binlog_prefix,\
db_host, db_user, db_password, db_port,\
mail_server, mail_username, mail_password, mail_alarm_list,\
gaojing_default_token, gaojing_default_id, gaojing_message_token, gaojing_message_id = read_conf()

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
              receive_list=mail_alarm_list, email_subject="Rsync on aliyun200 from IDC is not working!",
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

def get_last_sql_file(fold, regular_m=backup_sql_prefix):
    files = os.listdir(fold)
    last_file = ''
    for item in files:
        if re.match(r'^%s[\s\S]*?\.sql\.gz$' % regular_m, item) and item > last_file:
            last_file = item
    return last_file

def get_bin_logs(fold, sql_name, regular_m=backup_binlog_prefix):
    re_result = re.match('^[\s\S]*?_(%s\.\d+)_pos-(\d+)\.sql\.gz$' % regular_m, sql_name)
    start_logbin_name = ""
    pos = ""
    if re_result:
        start_logbin_name = re_result.group(1)
        pos = re_result.group(2)
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
    binlog_str = ' '.join(binlogs)

    # scp file to remote file
    cmd = 'cd %s; scp -p %s %s %s@%s:%s/' % (backup_fold, binlog_str, last_backup, )
    cmd = 'zcat |sed '18,31d'|grep -av "SQL SECURITY DEFINER"'