#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
from pid import PidFile
import paramiko
import os
import requests
import json
import re
import arrow
import logging
import gzip
from envelopes import Envelope
try:
    import subprocess32 as subprocess
except:
    import subprocess

# ConfigParser have been renamed to configparser in python 3.x
try:
    import ConfigParser
except:
    import configparser as ConfigParser

#argv = sys.argv[1]
mysqldump="/usr/bin/mysqldump"
#mysqldump="/usr/local/mariadb/bin/mysqldump"
current_path = os.path.abspath(os.path.dirname(__file__))
error_file = os.path.join(current_path, "error.log")
logging.basicConfig(filename= error_file,
                    level=logging.INFO,
                    filemode='w',
                    format='%(asctime)s file:%(filename)s fun:%(funcName)s line:%(lineno)d %(levelname)s: %(message)s',
                    )

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

def bash(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = p.communicate()
    if result[1] or p.returncode:
        return_code = 1
    else:
        return_code = 0
    output = "stdout:\n%s\nstderr:\n%s" % (result[0], result[1])
    return {"code": return_code, "output": output}

def read_conf(conf_name=os.path.join(current_path,"db_backup.conf")):
    #print(conf_name)
    conf = ConfigParser.ConfigParser()
    conf.read(conf_name)
    return (conf.get('local', 'fold'), conf.get('local', 'prefix'),
            conf.get('db', 'login_name'), conf.get('db', 'password'), conf.get('db', 'databases'),
            conf.get('db', 'log_bin_fold'),
            conf.get('mail', 'smtp_server'), conf.get('mail', 'login_name'), conf.get('mail', 'password'),
            conf.get('mail', 'alarm_list'),
            conf.get('remote', 'host'), conf.getint('remote', 'port'), conf.get('remote', 'login_name'),
            conf.get('remote', 'password'), conf.get('remote', 'fold'),
            conf.get('gaojing', 'token_default'), conf.get('gaojing', 'id_default'),
            conf.get('gaojing', 'token_message'), conf.get('gaojing', 'id_message')
            )

# get variables from out config file
backup_fold, name_prefix, db_user, db_password, db_databases, db_log_bin_fold, mail_server, mail_username,\
mail_password, mail_alarm_list, remote_host, remote_port, remote_user, remote_password, remote_fold,\
gaojing_token_default, gaojing_id_default, gaojing_token_message, gaojing_id_message = read_conf()

def send_mail(server=mail_server, user=mail_username, password=mail_password,
              receive_list=mail_alarm_list, email_subject="Error in database backup",
              attach_file=error_file):
    #print(receive_list)
    rec_list = receive_list.split()
    #print(type(rec_list))
    #print(rec_list)
    with open(error_file, 'r') as f:
        content=f.read()
    envelope = Envelope(from_addr=user,
                        to_addr=rec_list,
                        subject=email_subject,
                        text_body=content
                        )
    envelope.add_attachment(attach_file)
    envelope.send(server, login=user, password=password)

def scp(source_file, des_file, host=remote_host, port=remote_port, user=remote_user, password=remote_password):
    des_parent_fold=os.path.dirname(des_file)
    ssh_h = paramiko.SSHClient()
    ssh_h.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #try:
    ssh_h.connect(host, port=port, username=user, password=password)
    _, stdout, stderr = ssh_h.exec_command('mkdir %s' % des_parent_fold, timeout=5)
    if stdout.channel.recv_exit_status():
        raise Exception(stderr.read())
    sftp_h = ssh_h.open_sftp()
    sftp_h.put(source_file, des_file)
    sftp_h.close()
    ssh_h.close()
    #except Exception as e:
    #    exit()

# mysqldump and gzip and scp to remote fold
def db_backup():
    today=arrow.now().format('YYYY-MM-DD')
    file_list = os.listdir(backup_fold)
    for item in file_list:
        if re.match('^%s_%s.*\.(gz)|(sql)$' % (name_prefix, today), item):
            print("today's sql or sql.gz file is already existed!")
            logging.error("today's sql or sql.gz file is already existed!")
            send_mail()
            #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
            exit()

    sql_file_name = name_prefix + "_" + today + ".sql" # file name after mysqldump and gzip comporessed
    sql_file_name_abs = os.path.join(backup_fold, sql_file_name)
    sql_zip_file_abs = sql_file_name_abs + ".gz"

    # if today's fold already existed, exit!
    if os.path.isfile(sql_file_name_abs):
        print("the today's backup sql is already existed! quit!")
        logging.error("the today's backup sql is already existed! quit!")
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    if os.path.isfile(sql_zip_file_abs):
        print("the today's backup sql zip file is already existed! quit!")
        logging.error("the today's backup sql zip file is already existed! quit!")
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    # mysqldump and gzip
    cmd='%s -u %s -p%s --master-data=2 --single-transaction %s > %s' % (
        mysqldump, db_user, db_password, db_databases, sql_file_name_abs)
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        if os.path.isfile(sql_file_name_abs):
            os.remove(sql_file_name_abs)
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    cmd='gzip %s' % sql_file_name_abs
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        if os.path.isfile(sql_file_name_abs):
            os.remove(sql_file_name_abs)
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    # to get master info and rename sql.gz file to contain master info
    i=0
    binlog_file = ''
    binlog_pos = ''
    f = gzip.open(sql_zip_file_abs, 'r')
    for line in f:
        if re.match('^-- CHANGE MASTER TO.*', line):
            binlog_file, binlog_pos = re.match(
                '^-- CHANGE MASTER TO MASTER_LOG_FILE=\'(.*)\', MASTER_LOG_POS=(.*);$', line).groups()
            f.close()
            break
        else:
            if i > 100:
                f.close()
                print("No Master bin log information in sql.gz file!")
                logging.error("No Master bin log information in sql.gz file!")
                #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
                exit()
            else:
                i = i + 1
    if not binlog_file or not binlog_pos:
        print("can't find master information in head 100 line of sql backup file: %s! exit" % sql_zip_file_abs)
        logging.error("can't find master information in head 100 line of sql backup file: %s! exit" % sql_zip_file_abs)
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    new_file_name = os.path.join(os.path.dirname(sql_zip_file_abs), name_prefix + "_" + today +
                                                 "_" + binlog_file + "_" + binlog_pos + ".sql.gz")
    try:
        os.rename(sql_zip_file_abs, new_file_name)
    except Exception as e:
        print(str(e))
        logging.error(str(e))
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()

    print("begain to rsync backup fold")
    cmd='rsync --port=%d -a %s %s@%s:%s' % (remote_port, new_file_name, remote_user, remote_host, remote_fold)
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        send_mail()
        send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()



# sync db_binlog
def db_binlog_sync():
    dir_list = os.listdir(backup_fold)
    last_backup_file = ""
    if dir_list:
        for item in dir_list:
            if re.match('^%s_\d\d\d\d-\d\d-\d\d_.*_\d+\.sql.gz$' % name_prefix, item) and item > last_backup_file:
                last_backup_file = item
    else:
        print("No fold under backup fold! exit")
        logging.error("No fold under backup fold! exit")
        send_mail()
        exit()
    if not last_backup_file:
        print("No backup file under backup fold! exit")
        logging.error("No backup file under backup fold! exit")
        send_mail()
        exit()
    else:
        print("the last backup file is: %s" % last_backup_file)
        logging.error("the last backup file is: %s" % last_backup_file)
    first_binlog_name = re.match('^%s_\d\d\d\d-\d\d-\d\d_(.*)_.*\.sql\.gz$' % name_prefix, last_backup_file).groups()[0]
    if not first_binlog_name:
        print("can't obtain bin log file name from backup file name!")
        logging.error("can't obtain bin log file name from backup file name!")
        send_mail()
        exit()
    else:
        binlog_prefix = first_binlog_name.split('.')[0]
        print("first_binlog_name is: %s" % first_binlog_name)
        logging.error("first_binlog_name is: %s" % first_binlog_name)

    # find bin log need to copy to local backup fold
    file_need_sync = []
    file_list = os.listdir(db_log_bin_fold)
    #print(file_list)
    for item in file_list:
        if re.match('^%s\.\d+$' % binlog_prefix, item) and item >= first_binlog_name:
            #print(item)
            file_need_sync.append(item)
    if file_need_sync:
        files = (" " + db_log_bin_fold + os.path.sep).join(file_need_sync)
        files = db_log_bin_fold + os.path.sep + files
        print("the files to be rsynced are: %s" % files)
        logging.error("the files to be rsynced are: %s" % files)
        cmd = 'rsync -t %s %s' % (files, backup_fold)
        result=bash(cmd)
        if result["code"] != 0:
            print(result["output"])
            logging.error(result["output"])
            send_mail()
            exit()
        rsync_time_mark = os.path.join(backup_fold, "RSYNC_TIME_MARK")
        cmd='touch %s' % rsync_time_mark
        result = bash(cmd)
        if result["code"] != 0:
            print(result["output"])
            logging.error(result["output"])
            send_mail()
            exit()
        cmd='rsync --port=%d -t %s %s %s@%s:%s' % (remote_port, files, rsync_time_mark,
                                                remote_user, remote_host, remote_fold)
        result = bash(cmd)
        if result["code"] != 0:
            print(result["output"])
            logging.error(result["output"])
            send_mail()
            exit()
    else:
        print("No bin log needed to sync, only rsync time mark file!")
        logging.error("No bin log needed to sync, only rsync time mark file!")
        rsync_time_mark = os.path.join(backup_fold, "RSYNC_TIME_MARK")
        cmd='touch %s' % rsync_time_mark
        result = bash(cmd)
        if result["code"] != 0:
            print(result["output"])
            logging.error(result["output"])
            send_mail()
            exit()
        cmd='rsync --port=%d -t %s %s@%s:%s' % (remote_port, rsync_time_mark,
                                                   remote_user, remote_host, remote_fold)
        result = bash(cmd)
        if result["code"] != 0:
            print(result["output"])
            logging.error(result["output"])
            send_mail()
            exit()

if __name__ == '__main__':
    argv="rsync"
    with PidFile(piddir=backup_fold):
        if argv == "backup":
            db_backup()
        elif argv == "rsync":
            db_binlog_sync()
        else:
            print("usage: python db_backup.py backup or python db_backup.py rsync")


