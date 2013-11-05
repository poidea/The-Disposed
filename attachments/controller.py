import eventlet
from eventlet import wsgi
from webob.dec import wsgify
import webob
from common.controller import Controller
from db import hadoop_hbase as driver
from common.controller import render

import uuid
import datetime
from datetime import timedelta
import os
from fs import os as fs

import Image
import time

import string

def new_id_2():
    return str(10000000000 - time.time()).replace('.', '_')

user_db = driver.UserDB()
pin_db = driver.PinDB()
pin_fs = fs.FileSystem()

PATH = '/home/pin/static'

class AttachmentController(Controller):
    @webob.dec.wsgify
    def thumbnail(self, req, id):
        try:
            filename = "/thumbnails/%s.jpg" % id
            response = webob.Response(content_type=pin_fs.mimetype(filename))
            response.body = pin_fs.read(filename)
        except:
            filename = "/thumbnails/%s" % id
            response = webob.Response(content_type=pin_fs.mimetype(filename))
            response.body = pin_fs.read(filename)

        return response

    @webob.dec.wsgify
    def source(self, req, id):
        filename = "/original_sources/%s" % id
        response = webob.Response(content_type=pin_fs.mimetype(filename))
        response.body = pin_fs.read(filename)
        return response

    @webob.dec.wsgify
    def avatar(self, req, id):
        filename = "/avatars/%s" % id
        response = webob.Response(content_type=pin_fs.mimetype(filename))
        response.body = pin_fs.read(filename)
        return response

    @webob.dec.wsgify
    def avatar_small(self, req, id):
        filename = "/avatars_small/%s" % id
        response = webob.Response(content_type=pin_fs.mimetype(filename))
        response.body = pin_fs.read(filename)
        return response

    def _gen_thumbnail(self, id):
        WIDTH = 260
        HEIGHT_LIMIT = 600
        img = Image.open(pin_fs.locate('/original_sources/%s' % id))
        if img.format == 'GIF':
            pin_fs.copy('/original_sources/%s' % id, '/thumbnails/%s' % id)
        else:
            width, height = img.size
            h = height * WIDTH / width
            if h > HEIGHT_LIMIT:
                h = HEIGHT_LIMIT
            out = img.resize((WIDTH, height * WIDTH / width), Image.BILINEAR)
            crop = out.crop((0, 0, 260, h))
            crop.save(pin_fs.locate('/thumbnails/%s.jpg' % id), quality=100)

    def _gen_middle_size(self, id):
        WIDTH = 260
        img = Image.open(pin_fs.locate('/original_sources/%s' % id))
        if img.format == 'GIF':
            pin_fs.copy('/original_sources/%s' % id, '/middle_size/%s' % id)
        else:
            width, height = img.size
            h = height * WIDTH / width
            out = img.resize((WIDTH, height * WIDTH / width), Image.BILINEAR)
            crop = out.crop((0, 0, 260, h))
            crop.save(pin_fs.locate('/middle_size/%s.jpg' % id), quality=100)

    @webob.dec.wsgify
    def upload(self, req):
        img_id = new_id_2()
        pin_fs.write('/original_sources/%s' % img_id, req.body)
        self._gen_thumbnail(img_id)
        self._gen_middle_size(img_id)

        im = Image.open(pin_fs.locate('/original_sources/%s' % img_id))

        if im.format == 'JPEG':
            img_format = 'jpg'
        else:
            img_format = string.lower(im.format)
        
        return render({
            "id": img_id
        })

    @webob.dec.wsgify
    def upload_avatar(self, req, user_id):
        pin_fs.write('/avatars/%s' % user_id, req.body)
        return render({
            "status": "success"
        })

    @webob.dec.wsgify
    def upload_avatar_small(self, req, user_id):
        pin_fs.write('/avatars_small/%s' % user_id, req.body)
        return render({
            "status": "success"
        })

    @webob.dec.wsgify
    def middle_size(self, req, id):
        try:
            filename = "/middle_size/%s.jpg" % id
            response = webob.Response(content_type=pin_fs.mimetype(filename))
            response.body = pin_fs.read(filename)
        except:
            filename = "/middle_size/%s" % id
            response = webob.Response(content_type=pin_fs.mimetype(filename))
            response.body = pin_fs.read(filename)

        return response


