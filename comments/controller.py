import eventlet
from eventlet import wsgi
from webob.dec import wsgify
import webob
from common.controller import Controller
from db import hadoop_hbase as driver

from common.controller import render

import datetime
from datetime import timedelta

user_db = driver.UserDB()
comment_db = driver.CommentDB()

class CommentController(Controller):
    @webob.dec.wsgify
    def create(self, req, body, pin_id):
        user = user_db.get_user_from_token(req.headers['X-Token'])
        comment = comment_db.create(pin_id, user['key'], body['content'])
        return render({
                    "comments": comment
                })

    @webob.dec.wsgify
    def list(self, req, pin_id):
        if req.query_string == '':
            comments = comment_db.list(pin_id)
        else:
            comments = comment_db.list(pin_id, last_row=req.query_string)

        for cmt in comments:
            cmt['author'] = user_db.get_user_by_id(cmt['user_id'])
        return render({
                    "comments": comments
                })

    @webob.dec.wsgify
    def reply(self, req, body, pin_id, comment_id):
        user = user_db.get_user_from_token(req.headers['X-Token'])
        return render(
                    comment_db.reply(pin_id, user['key'], comment_id, body['content'])
               )

    @webob.dec.wsgify
    def update(self, req, body, comment_id):
        comment_db.update(comment_id, **body)

    @webob.dec.wsgify
    def delete(self, req, comment_id):
        comment_db.delete(comment_id)


