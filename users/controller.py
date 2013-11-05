# -*- coding: UTF-8 -*-

import eventlet
from eventlet import wsgi
from webob.dec import wsgify
import webob
from common.controller import Controller
from db import hadoop_hbase as driver
from common.controller import render
import re
import datetime
from datetime import timedelta
import util.sendmail as email

user_db = driver.UserDB()

class UserController(Controller):
    @webob.dec.wsgify
    def token(self, req, body):
        check_user = user_db.get_user_by_email(body['email'])

        if check_user.has_key('error'):
            return render({
                "error": "email not exists"
            })

        if user_db.authenticate(body['email'], body['password']):
            user = user_db.get_user_by_email(body['email'])
            user.pop('token')
            user.pop('token_expired')
            token = user_db.create_token(user['key'])
        else:
            return render({
                "error": "password incorrect."
            })

        # Result for display
        expired = datetime.datetime.now() + timedelta(hours=1)
        result = {
            "token": token,
            "user": user,
            "expired": str(expired)
        }

        return render(result)

    @webob.dec.wsgify
    def get_user_from_token(self, req, token):
        user = user_db.get_user_from_token(token)
        return render(user)

    @webob.dec.wsgify
    def list(self, req):
        last_row = req.query_string
        result = {
            "users": user_db.list(last_row)
        }
        return render(result)

    @webob.dec.wsgify
    def create(self, req, body):
        # validation
        pattern = re.compile(r'[\w\-]+\@[\w\-]+\.[\w]+')
        match = pattern.match(body['email'])
        if match is None:
            return render({
                "error": "not email format"
            })

        # prevent repeatation
        user_check_name = user_db.get_user_by_name(body['username'])
        user_check_email = user_db.get_user_by_email(body['email'])

        if not (user_check_name.has_key('error') and user_check_email.has_key('error')):
            return render({
                "error": "username or email exists"
            })

        user = user_db.create(**body)

        email.send_mail(body['email'], "邮箱验证", '''%s，您好。
欢迎使用大数居系统，我们诚心为您服务。
请点击以下链接，
以验证您的账户。

http://192.168.1.187:8087/check/code/%s''' % (body['username'], user['validation_code']))

        return render(user)

    @webob.dec.wsgify
    def update(self, req, body, id):
        user = user_db.update(id, **body)

        return webob.Response(req.body)

    @webob.dec.wsgify
    def update_password(self, req, body):
        user = user_db.update_password(body['email'], body['password'], body['new_password'])

        return webob.Response(req.body)

    @webob.dec.wsgify
    def delete(self, req, id):
        try:
            user_db.delete(id)
        except:
            raise "Unexpected error."

    @webob.dec.wsgify
    def profile(self, req, id):
        user = user_db.profile(id)

        return render({
            "user": user
        })

    @webob.dec.wsgify
    def search(self, req, name):
        users = user_db.search_user(name)

        return render({
            "users": users
        })

    @webob.dec.wsgify
    def validate_email(self, req, validate_code):
        validate_email = user_db.validate_email(validate_code)

        return render(validate_email)

    @webob.dec.wsgify
    def check_email(self, req, email):
        check_user = user_db.get_user_by_email(email)

        if check_user.has_key('error'):
            return render({
                "status": "pass"
            })

        return render({
            "status": "failed"
        })

    @webob.dec.wsgify
    def check_username(self, req, username):
        check_user = user_db.get_user_by_name(username)

        if check_user.has_key('error'):
            return render({
                "status": "pass"
            })

        return render({
            "status": "failed"
        })

