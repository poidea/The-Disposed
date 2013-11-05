# -*- coding: UTF-8 -*-

import eventlet
from eventlet import wsgi
from webob.dec import wsgify
import webob
from common.controller import Controller
from db import hadoop_hbase as driver
from fs import os as fs

from common.controller import render
from common.controller import need_login

import simplejson as json

user_db = driver.UserDB()
pin_db = driver.PinDB()
comment_db = driver.CommentDB()

class PinController(Controller):
    @webob.dec.wsgify
    #@need_login
    def list(self, req):
        if req.query_string == "":
            pins = pin_db.list()
        else:
            pins = pin_db.list(last_row=req.query_string)

        for pin in pins:
            user = user_db.get_user_by_id(pin['author_id'])
            pin['author'] = user
            #pin['img_info'] = json.loads(pin['img_info'])

        return render({
                    "pins": pins
               })

    #@need_login
    @webob.dec.wsgify
    def list_by_catalog_id(self, req, id):
        if req.query_string == "":
            pins = pin_db.list(id)
        else:
            pins = pin_db.list(id, last_row=req.query_string)

        for pin in pins:
            user = user_db.get_user_by_id(pin['author_id'])
            pin['author'] = user
            pin['img_info'] = json.loads(pin['img_info'])

        return render({
                    "pins": pins
                })

    @webob.dec.wsgify
    #@need_login
    def list_by_user(self, req, id, catalog_id=None):
        if req.query_string == "":
            pins = pin_db.list_by_user(id, catalog_id)
        else:
            pins = pin_db.list_by_user(id, catalog_id, last_row=req.query_string)

        for pin in pins:
            user = user_db.get_user_by_id(pin['author_id'])
            pin['author'] = user
            pin['img_info'] = json.loads(pin['img_info'])

        return render({
                    "pins": pins
               })

    @webob.dec.wsgify
    #@need_login
    def create(self, req, body, id, given_id=None):
        user = user_db.get_user_from_token(req.headers['X-Token'])
        if body.has_key('movie_id'):
            pin = pin_db.create_movie(user['key'], body['introduction'], id, body['movie_id'])
        else:
            pin = pin_db.create(given_id, user['key'], body['introduction'], id)
        return render(pin)

    @webob.dec.wsgify
    #@need_login
    def delete(self, req, id):
        pin_db.delete(id)

    @webob.dec.wsgify
    #@need_login
    def update(self, req, body, id):
        pin_db.update(id, **body)

    @webob.dec.wsgify
    #@need_login
    def get(self, req, id):
        return render({
                    "pin": pin_db.get(id),
                    "comments": comment_db.list(id)
               })

    @webob.dec.wsgify
    #@need_login
    def search(self, req, body):
        if req.query_string == '':
            pin = pin_db.search(body['keyword'])
        else:
            pin = pin_db.search(body['keyword'], last_row=req.query_string)

        return render({
            "pins": pin
        })



