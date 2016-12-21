#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
error_file = os.path.join(current_path, "check_error.log")
logging.basicConfig(filename= error_file,
                    level=logging.INFO,
                    filemode='w',
                    format='%(asctime)s file:%(filename)s fun:%(funcName)s line:%(lineno)d %(levelname)s: %(message)s',
                    )

def read_conf(conf_name=os.path.join(current_path,"check_rsync_status.conf")):
    #print(conf_name)
    conf = ConfigParser.ConfigParser()
    conf.read(conf_name)
    return (conf.get('file', 'name'),
            conf.get('mail', 'smtp_server'), conf.get('mail', 'login_name'), conf.get('mail', 'password'),
            conf.get('mail', 'alarm_list'),
            conf.get('gaojing', 'token_default'), conf.get('gaojing', 'id_default'),
            conf.get('gaojing', 'token_message'), conf.get('gaojing', 'id_message')
            )

mark_file, mail_server, mail_username, mail_password, mail_alarm_list,gaojing_default_token, gaojing_default_id,\
gaojing_message_token, gaojing_message_id = read_conf()

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

if __name__ == '__main__':
    mark_file_time = os.stat(mark_file).st_mtime
    print('mark_file_time is: %s, type is: %s' % (mark_file_time, type(mark_file_time)))
    now_time = time.time()
    print('now time is: %s' % now_time)
    if now_time - mark_file_time > 7200:
        logging.error("rsyn of db bin log have not work for at least 2h on aliyun200.")
        send_mail()
