import eventlet
from eventlet import wsgi
from webob.dec import wsgify
import webob
from common.controller import Controller
from db import hadoop_hbase as driver
from fs import os as fs

from common.controller import render

import Image

user_db = driver.UserDB()
fo_db = driver.FollowDB()

class FollowController(Controller):
    @webob.dec.wsgify
    def list_following(self, req, id):
        return render(fo_db.list_following(id))

    @webob.dec.wsgify
    def list_follower(self, req, id):
        return render(fo_db.list_follower(id))

    @webob.dec.wsgify
    def fo(self, req, user_id):
        user = user_db.get_user_from_token(req.headers['X-Token'])
        if user['key'] == user_id:
            return render({
                "error": "cannot fo yourself"
            })

        if not fo_db.is_follower(user['key'], user_id): # main
            f = fo_db.fo(user['key'], user_id)
            return render(f)

        user_check = user_db.get_user_by_id(user_id)

        if user_check.has_key('error'):
            return render({
                "error": "cannot follow a user that not exists"
            })
        
        return render({
            "status": "followed"
        })

    @webob.dec.wsgify
    def unfo(self, req, user_id):
        user = user_db.get_user_from_token(req.headers['X-Token'])
        fo_db.unfo(user['key'], user_id)

        return render({
            "status": "unfollowed"
        })
