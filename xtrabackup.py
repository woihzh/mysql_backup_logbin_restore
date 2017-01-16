#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    use innobackupex to backup database and scp to remote server
    the runner user should in group of mysql, otherwise innobackupex would have no right to read files uder mysqld
work fold.
    a configure file 'xtrabackup.conf' is required to setting variables. the format of the conf file is like:
[log]
name = xtrabackup_error.log
xtra_name = innobackupex.log

[backup]
fold = /xtrabackup_jubao_
prefix = jubao

[remote]
host = 10.13.2.57
user = root
## the password not required
password = kkkkkk
fold = /xtrabackup_xss

[db]
user = xtryx
password = ssssss
database = jklsldf

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
"""

import re
import os
import json
# import pexpect
# import time
import requests
import logging
from envelopes import Envelope
try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess
try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

current_path = os.path.abspath(os.path.dirname(__file__))


# read variables from configure file
def read_conf(conf_name=os.path.join(current_path, "restore_to_local.conf")):
    # print(conf_name)
    conf = ConfigParser.ConfigParser()
    conf.read(conf_name)
    return (conf.get('log', 'name'),
            conf.get('backup', 'fold'), conf.get('backup', 'prefix'),
            conf.get('remote', 'host'), conf.get('remote', 'user'), conf.get('remote', 'fold'),
            conf.get('db', 'user'), conf.get('db', 'password'), conf.get('db', 'database'),
            conf.get('mail', 'smtp_server'), conf.get('mail', 'login_name'), conf.get('mail', 'password'),
            conf.get('mail', 'alarm_list'), conf.get('mail', 'sub_success'), conf.get('mail', 'sub_fail'),
            conf.get('gaojing', 'token_default'), conf.get('gaojing', 'id_default'),
            conf.get('gaojing', 'token_message'), conf.get('gaojing', 'id_message')
            )
log_error, \
    backup_fold, backup_prefix, \
    remote_host, remote_user, remote_fold, \
    db_user, db_password, db_database, \
    mail_server, mail_username, mail_password, mail_alarm_list, mail_sub_success, mail_sub_fail, \
    gaojing_default_token, gaojing_default_id, gaojing_message_token, gaojing_message_id = read_conf()

# set logging file and format
error_file = os.path.join(current_path, log_error)
logging.basicConfig(filename=error_file,
                    level=logging.INFO,
                    filemode='w',
                    format='%(asctime)s file:%(filename)s fun:%(funcName)s line:%(lineno)d %(levelname)s: %(message)s',
                    )
# set innobackupex log file


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
    data = {
            "service_id": service_id,
            "description": message,
            "event_type": "trigger"
           }
    resp = requests.post("http://gaojing.baidu.com/event/create",
                         data=json.dumps(data),
                         headers={
                                  "servicekey": token,
                                 },
                         timeout=3,
                         verify=False
                         )
    result = json.loads(resp.content)
    print("The baidu Gaojing return message is: %s" % result["message"])


def send_mail(email_sub, server=mail_server, user=mail_username, password=mail_password,
              receive_list=mail_alarm_list,
              attach_file=error_file):
    # print(receive_list)
    rec_list = receive_list.split()
    # print(type(rec_list))
    # print(rec_list)
    with open(error_file, 'r') as f:
        content = f.read()
    envelope = Envelope(from_addr=user,
                        to_addr=rec_list,
                        subject=email_sub,
                        text_body=content
                        )
    envelope.add_attachment(attach_file)
    envelope.send(server, login=user, password=password, tls=True)


def innobackup(user, password, database, fold):
    backup_abs_fold = ""
    cmd = 'innobackupex --user=%s --password=%s --databases="mysql %s" %s/' % (user, password, database,
                                                                               fold)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = p.communicate()
    if p.returncode:
        print(result[1])
        logging.error(result[1])
        send_mail(mail_sub_fail)
        # send_gaojing(gaojing_id_message, gaojing_token_message, mail_sub_fail)
        exit()
    else:
        content = result[1].split('\n')
        if re.match('.* completed OK!', content[-2]):
            print("success innobackupex db %s" % db_database)
            logging.info("success innobackup db %s" % db_database)
            try:
                backup_abs_fold = re.match(".* Backup created in directory '(.*)'", content[-8]).group(1)
            except Exception as e:
                print(str(e))
                logging.error(str(e))
                send_mail(mail_sub_fail)
                # send_gaojing(gaojing_id_message, gaojing_token_message, mail_sub_fail)
                exit()
        else:
            print("Fail when innobackupex db %s" % db_database)
            logging.error(result[1])
            send_mail(mail_sub_fail)
            # send_gaojing(gaojing_id_message, gaojing_token_message, mail_sub_fail)
    return backup_abs_fold


def innoapply(fold):
    cmd = "innobackupex --apply-log %s" % fold
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = p.communicate()
    if p.returncode:
        print(result[1])
        logging.error(result[1])
        send_mail(mail_sub_fail)
        # send_gaojing(gaojing_id_message, gaojing_token_message, mail_sub_fail)
        exit()
    else:
        content = result[1].split('\n')
        if re.match('.* completed OK!', content[-2]):
            print("success innobackupex apply-log db %s" % db_database)
            logging.info("success innobackup apply-log db %s" % db_database)
        else:
            print("Fail when innobackupex apply-log db %s" % db_database)
            logging.error(result[1])
            send_mail(mail_sub_fail)
            # send_gaojing(gaojing_id_message, gaojing_token_message, mail_sub_fail)


def compress(fold):
    fold_name = os.path.basename(fold)
    backup_file_name = os.path.join(backup_fold, backup_prefix + "-" + fold_name + "tar.gz")
    cmd = "tar -C %s -czf %s %s" % (backup_fold, backup_file_name, fold_name)
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        send_mail(mail_sub_fail)
        # send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    return backup_file_name


def scp(source_file, dest_fold):
    cmd = "scp %s %s@%s:%s/" % (source_file, remote_user, remote_host, dest_fold)
    result = bash(cmd)
    if result["code"] != 0:
        print(result["output"])
        logging.error(result["output"])
        send_mail(mail_sub_fail)
        # send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()

if __name__ == '__main__':
    if os.path.isdir(backup_fold):
        cmd = "rm -rf %s/*" % backup_fold
        result = bash(cmd)
        if result["code"] != 0:
            print(result["output"])
            logging.error(result["output"])
            send_mail(mail_sub_fail)
            # send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
            exit()
    else:
        print("backup fold: %s not existed" % backup_fold)
        logging.error("backup fold: %s not existed" % backup_fold)
        send_mail(mail_sub_fail)
        exit()
    result_fold = innobackup(db_user, db_password, db_database, backup_fold)
    innoapply(result_fold)
    result_file = compress(result_fold)
    scp(result_file, remote_fold)
    send_mail(mail_sub_success)
