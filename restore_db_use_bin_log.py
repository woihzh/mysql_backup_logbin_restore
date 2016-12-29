#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import requests
import json
import re
import logging
import time
import arrow
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

current_path = os.path.abspath(os.path.dirname(__file__))
error_file = os.path.join(current_path, "error.log")
mark_file = os.path.join(current_path, "mark_restore_db.conf")
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

def read_conf(conf_name=os.path.join(current_path, mark_file)):
    #print(conf_name)
    conf = ConfigParser.ConfigParser()
    conf.read(conf_name)
    return (conf.get('binlog', 'last_file'),
            conf.getint('binlog','last_pos'),
            conf.get('db', 'login_name'), conf.get('db', 'password'),
            conf.get('mail', 'smtp_server'), conf.get('mail', 'login_name'), conf.get('mail', 'password'),
            conf.get('mail', 'alarm_list'),
            conf.get('gaojing', 'token_default'), conf.get('gaojing', 'id_default'),
            conf.get('gaojing', 'token_message'), conf.get('gaojing', 'id_message')
            )

binlog_file_abs, binlog_pos, db_user, db_password, mail_server, mail_username, \
mail_password,mail_alarm_list,gaojing_token_default, gaojing_id_default, gaojing_token_message, \
gaojing_id_message = read_conf()
print("binlog_file_abs: %s" % binlog_file_abs)

def write_conf(last_file, last_pos, conf_name=os.path.join(current_path, mark_file)):
    conf = ConfigParser.ConfigParser()
    conf.read(conf_name)
    conf.set('binlog','last_file', last_file)
    conf.set('binlog','last_pos', last_pos)
    with open(conf_name,'w') as f:
        conf.write(f)

def send_mail(server=mail_server, user=mail_username, password=mail_password,
              receive_list=mail_alarm_list, email_subject="Error in restore database ",
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

def get_binlogs_newer(fold, regex, file_name):
    #print("fold: %s, regex: %s, file_name: %s" % (fold, regex, file_name))
    files = os.listdir(fold)
    return_file = []
    for item in files:
        #print("item: " + item)
        if re.match('^%s\.\d+$' % regex, item) and item >= file_name:
            return_file.append(item)
    return return_file

def get_last_bin_pos(filename):
    cmd = 'mysqlbinlog --no-defaults %s | tail -10 | tac' % filename
    child = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    result = child.communicate()
    #print(result[0])
    re_result = re.match('^[\s\S]*? end_log_pos (\d+) [\s\S]*', result[0])
    pos = ''
    if re_result:
        pos = re_result.groups()[0]
    else:
        print("Can't get last bin log pos from bin log file: %s" % filename)
        logging.error("Can't get last bin log pos from bin log file: %s" % filename)
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    return int(pos)

if __name__ == '__main__':
    today = arrow.now().format('YYYY-MM-DD') + " 00:45:00"
    print("today is: %s" % today)
    if not binlog_file_abs or not binlog_pos:
        print("can't get information about sql file and binlog, exit!")
        logging.error("can't get information about sql file and binlog, exit!")
        exit()
    if not os.path.isfile(binlog_file_abs):
        print("can't find binlog file: %s" % binlog_file_abs)
        logging.error("can't find binlog file: %s" % binlog_file_abs)
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    work_fold = os.path.dirname(binlog_file_abs)
    binlog_name = os.path.basename(binlog_file_abs)
    binlog_prefix = binlog_name.split('.')[0]
    rsync_time_mark = os.path.join(work_fold, "RSYNC_TIME_MARK")
    try:
        mtime = os.stat(rsync_time_mark).st_mtime
        mark_file_mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mtime))
        print("sync mark file time is: %s" % mark_file_mtime)
    except Exception as e:
        print("rsync mark file is not existed")
        logging.error("rsync mark file is not existed")
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    if mark_file_mtime < today:
        print("Last rsync time is only: %s, the last rsync time should newer then 1:00." % mark_file_mtime)
        logging.error("Last rsync time is only: %s, the last rsync time should newer then 1:00." % mark_file_mtime)
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    binlogs = get_binlogs_newer(work_fold, binlog_prefix, binlog_name)
    if not binlogs or not binlog_name in binlogs:
        print("can't find bin log files under work fold")
        logging.error("can't find bin log files under work fold")
        send_mail()
        #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
        exit()
    if len(binlogs) > 1:
        binlogs.sort()
        last_binlog = binlogs[-1]
        print("bin logs to restore are: %s " % str(binlogs))
        logging.info("bin logs to restore are: %s " % str(binlogs))
        last_pos = get_last_bin_pos(os.path.join(work_fold,last_binlog))
        print("last position in last bin log file %s is: %s" % (last_binlog, last_pos))
        logging.info("last position in last bin log file %s is: %s" % (last_binlog, last_pos))
        binlogs_str = ' '.join(binlogs)
        cmd = 'cd %s;mysqlbinlog --no-defaults -D --start-position=%s --stop-position=%s %s\
         | mysql -u %s -p%s' % (work_fold, binlog_pos, last_pos, binlogs_str, db_user, db_password)
        result = bash(cmd)
        if result["code"] != 0:
            print(result["output"])
            logging.error(result["output"])
            send_mail()
            #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
            exit()
        write_conf(os.path.join(work_fold, last_binlog), last_pos)
        print("Success: aliyun200 execute bin logs to db")
        logging.info("Success: aliyun200 execute bin logs")
        send_mail(email_subject="Succeess aliyun200 execute bin logs jubao")
    else:
        last_pos = get_last_bin_pos(binlog_file_abs)
        print("last_pos: %s, type is: %s" % (last_pos, type(last_pos)))
        print("recorded pos is: %s, type is: %s" % (binlog_pos, type(binlog_pos)))
        logging.info("last_pos: %s" % last_pos)
        if last_pos > binlog_pos:
            cmd = 'cd %s;mysqlbinlog --no-defaults -D --start-position=%s --stop-position=%s %s\
                   | mysql -u %s -p%s' % (work_fold, binlog_pos, last_pos, binlogs[0], db_user, db_password)
            result = bash(cmd)
            if result["code"] != 0:
                print(result["output"])
                logging.error(result["output"])
                send_mail()
                #send_gaojing(gaojing_id_message, gaojing_token_message, "weekly mysqldump of product db fail")
                exit()
            write_conf(os.path.join(work_fold, binlogs[0]), last_pos)
            print("Success: aliyun200 execute bin logs")
            logging.info("Success: aliyun200 execute bin logs to jubao")
            send_mail(email_subject="Success aliyun200 execute bin logs to jubao")
        elif last_pos == binlog_pos:
            print("no newer enters in bin log to restore")
            logging.info("no newer enters in bin log to restore")
            send_mail(email_subject="no newer enters in bin log to restore on aliyun200")
        else:
            print("error: recoded binlog pos larger then new pos in bin log file on aliyun200")
            logging.error("error: recoded binlog pos larger then new pos in bin log file on aliyun200")
            send_mail()
    # compare sql query result from remote with local
    sql_mark = ""
    with open(os.path.join(work_fold, "SQL_QUERY_MARK"), 'r') as f:
        sql_mark = f.read()
    yesterday = yesterday = arrow.now().replace(days=-1).format('YYYY-MM-DD')
    cmd = "mysql -u %s -p%s %s -Nse \"select truncate(sum(pirp.nb_principal),2) as 'daishoubenjing', truncate(sum(pirp.nb_interest),2) as \
     'daishouzonglixi' from fiz_plan_invest_repay_plan pirp left join fiz_plan_invest pi on pirp.fk_plan_invest_id = \
     pi.pk_id left join fiz_plan p on pi.fk_plan_id = p.pk_id where date(pirp.dt_date) ='%s' \
     and p.dc_platform ='01' order by 1;\"" % (db_user, db_password, "jubao", yesterday)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = p.communicate()
    if result[1] or p.returncode:
        print("Error in execute mysql query on aliyun200 "+result[1])
        logging.error(result[1])
        send_mail()
        exit()
    elif sql_mark == result[0]:
        print("Aliyun200 identical to product db")
        logging.info("Aliyun200 identical to product db")
        send_mail(email_subject="Success aliyun200 compare db jubao with product")
    else:
        print("Aliyun db jubao not identical with product")
        logging.error("Aliyun db jubao not identical with product")
        send_mail(email_subject="Fail aliyun200 compare db jubao with product")


