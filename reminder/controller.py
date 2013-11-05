# -*- coding: UTF-8 -*-

import eventlet
from eventlet import wsgi
from webob.dec import wsgify
import webob
from common.controller import Controller
from db import hadoop_hbase as driver
from fs import os as fs

from common.controller import render

user_db = driver.UserDB()
reminder_db = driver.ReminderDB()

class ReminderController(Controller):
    @webob.dec.wsgify
    def list(self, req):
        me = user_db.get_user_from_token(req.headers['X-Token'])
        reminder = reminder_db.unread_list(me['key'])
        return render({
                  "reminders": reminder
               })

    @webob.dec.wsgify
    def create(self, req, body):
        me = user_db.get_user_from_token(req.headers['X-Token'])
        reminder = reminder_db.create(me['key'], body['content'], body['link'], body['from_user_id'])
        return render({
            "user_id": me['key'],
            "content": body['content'],
            "link": body['link'],
            "from_user_id": body['from_user_id']
        })

    @webob.dec.wsgify
    def list_last_ten_read(self, req):
        me = user_db.get_user_from_token(req.headers['X-Token'])
        reminder = reminder_db.last_ten_read_list(me['key'])
        return render({
                  "reminders": reminder
               })

    @webob.dec.wsgify
    def set_read(self, req):
        me = user_db.get_user_from_token(req.headers['X-Token'])
        reminder = reminder_db.set_read(me['key'])
        return render({
                  "status": "set read"
               })


