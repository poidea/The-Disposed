# -*- coding: UTF-8 -*-

import db.driver as driver

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

import datetime
from datetime import timedelta

import hashlib
import uuid
import time

import Image
from fs import os as fs
import simplejson as json

import string

pw_salt = 'p[s.31,\=-1`GD'

def new_id_1():
    return str(time.time()).replace('.', '_')

def new_id_2():
    return str(10000000000 - time.time()).replace('.', '_')

# Hbase Result format -> json
def fmt(data):
    res = []
    for result in data:
        single = {
            "key": result.row
        }
        for key, val in result.columns.iteritems():
            single[key[5:]] = val.value
        res.append(single)
    return res

def client():
    transport = TSocket.TSocket('192.168.1.241', 9090)
    transport = TTransport.TBufferedTransport(transport)

    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = Hbase.Client(protocol)
    transport.open()

    return client

def cn_encode(func):
    def _encode(*args, **kwargs):
        arr = []
        for val in args:
            if isinstance(val, unicode):
                arr.append(val.encode('utf8'))
            else:
                arr.append(val)
        dic = {}
        for (key, val) in kwargs.iteritems():
            if isinstance(kwargs[key], unicode):
                dic[key] = val.encode('utf8')
            else:
                dic[key] = val
        ret = func(*arr, **dic)
        return ret
    return _encode

def _cn_decode_iter(nodes):
    if type(nodes) == type({}):
        tmp = {}
        for (key, val) in nodes.iteritems():
            tmp[key] = _cn_decode_iter(val)
        return tmp
    else:
        if isinstance(nodes, unicode) or isinstance(nodes, str):
            return nodes.decode('utf8')
        else:
            return nodes

def cn_decode(func):
    def _encode(*args, **kwargs):
        ret = func(*args, **kwargs)
        if type(ret) == type([]):
            ab = []
            for i in ret:
                ab.append(_cn_decode_iter(i))
        else:
            ab = _cn_decode_iter(ret)
        return ab
    return _encode

class UserDB(driver.UserDatabaseDriver):
    def __create_table(self):
        c = client()
        columns = [
            ColumnDescriptor(name='info:username', maxVersions=5),
            ColumnDescriptor(name='info:password', maxVersions=15),
            ColumnDescriptor(name='info:email', maxVersions=5),
            ColumnDescriptor(name='info:gender', maxVersions=2),
            ColumnDescriptor(name='info:birthday', maxVersions=3),
            ColumnDescriptor(name='info:introduction', maxVersions=10),
            ColumnDescriptor(name='info:token'),
            ColumnDescriptor(name='info:token_expired')
        ]
        c.createTable('users', columns)

    def _password(self, password):
        m = hashlib.sha256()
        m.update(password + pw_salt)
        hashed_pw = m.hexdigest().upper()
        return hashed_pw

    def authenticate(self, email, password):
        user = self.get_user_by_email(email)
        hashed_pw = user['password']
        return hashed_pw == self._password(password)

    def create_token(self, id):
        c = client()
        token = str(uuid.uuid4())
        expired = datetime.datetime.now() + timedelta(hours=1)
        mutations = [
            Mutation(column="info:token", value=token),
            Mutation(column="info:token_expired", value=str(expired))
        ]
        c.mutateRow('users', id, mutations, {})
        return token

    @cn_decode
    def get_user_from_token(self, token):
        c = client()
        scanner = TScan()
        scanner.filterString = "SingleColumnValueFilter('info', 'token', =, 'binary:%s', true, true)" % token
        _id = c.scannerOpenWithScan('users', scanner, None)
        _list = c.scannerGetList(_id, 10)
        row = fmt(_list)[0]
        return row

    @cn_decode
    def get_user_by_id(self, id):
        c = client()
        row = c.getRow('users', id, {})
        result = fmt(row)[0]
        return result

    @cn_decode
    def get_user_by_name(self, username):
        c = client()
        scanner = TScan()
        scanner.filterString = "SingleColumnValueFilter('info', 'username', =, 'binary:%s', true, true)" % username
        _id = c.scannerOpenWithScan('users', scanner, None)
        _list = c.scannerGetList(_id, 10)
        try:
            row = fmt(_list)[0]
        except IndexError:
            return {
                "error": "no specified user"
            }
        return row

    @cn_decode
    def get_user_by_email(self, email):
        c = client()
        scanner = TScan()
        scanner.filterString = "SingleColumnValueFilter('info', 'email', =, 'binary:%s', true, true)" % email
        _id = c.scannerOpenWithScan('users', scanner, None)
        _list = c.scannerGetList(_id, 10)
        try:
            row = fmt(_list)[0]
        except IndexError:
            return {
                "error": "no specified user"
            }
        return row

    @cn_decode
    def get_user_by_valcode(self, valcode):
        c = client()
        scanner = TScan()
        scanner.filterString = "SingleColumnValueFilter('info', 'valid', =, 'binary:%s', true, true)" % valcode
        _id = c.scannerOpenWithScan('users', scanner, None)
        _list = c.scannerGetList(_id, 10)
        
        try:
            row = fmt(_list)[0]
        except IndexError:
            return {
                "error": "no specified user"
            }
        return row

    def update_token(self, token):
        user = self.get_user_from_token(token)
        new_token = str(uuid.uuid4())
        self.update(user['key'], token=new_token)
        return new_token

    @cn_decode
    def profile(self, id):
        result = self.get_user_by_id(id)
        try:
            result.pop('info:password')
            result.pop('info:token')
        except:
            pass
        return result

    @cn_encode
    def update(self, id, **kwargs):
        c = client()
        mutations = []
        if kwargs.has_key('username'):
            mutations.append(Mutation(column="info:username", value=kwargs['username']))
        if kwargs.has_key('email'):
            mutations.append(Mutation(column="info:email", value=kwargs['email']))
        if kwargs.has_key('gender'):
            mutations.append(Mutation(column="info:gender", value=kwargs['gender']))
        if kwargs.has_key('birthday'):
            mutations.append(Mutation(column="info:birthday", value=kwargs['birthday']))
        if kwargs.has_key('introduction'):
            mutations.append(Mutation(column="info:introduction", value=kwargs['introduction']))
        
        c.mutateRow('users', id, mutations, {})

    def update_password(self, email, password, new_password):
        if (self.authenticate(email, password)):
            c = client()
            user = self.get_user_by_email(email)
            mutations = [
                Mutation(column="info:password", value=self._password(new_password))
            ]
            c.mutateRow('users', user['key'], mutations, {})
        else:
            return {
                "error": "original password is not correct."
            }

    @cn_encode
    def create(self, email, username, password, gender, birthday, introduction):
        c = client()
        valid = str(uuid.uuid4())
        pin_fs = fs.FileSystem()

        mutations = [
            Mutation(column="info:username", value=username),
            Mutation(column="info:password", value=self._password(password)),
            Mutation(column="info:email", value=email),
            Mutation(column="info:gender", value=gender),
            Mutation(column="info:birthday", value=birthday),
            Mutation(column="info:introduction", value=introduction),
            Mutation(column="info:token", value=""),
            Mutation(column="info:token_expired", value=""),
            Mutation(column="info:valid", value=valid)
        ]
        id = new_id_1()
        c.mutateRow('users', id, mutations, {})

        return {
            "username": username,
            "password": password,
            "email": email,
            "gender": gender,
            "birthday": birthday,
            "introduction": introduction,
            "token": "",
            "token_expired": "",
            "validation_code": valid
        }

    # uid is the validation code
    def validate_email(self, valcode):
        user = self.get_user_by_valcode(valcode)

        if user.has_key('error'):
            return {
                "error": "the user is valid"
            }
        else:
            c = client()
            mutations = [
                Mutation(column="info:valid", value="true"),
            ]
            c.mutateRow('users', user['key'], mutations, {})

            return {
                "status": "successfully validated."
            }

    def delete(self, id):
        c = client()
        c.deleteAllRow('users', id, {})

    @cn_decode
    def list(self, last_row=None, each_page=30):
        c = client()
        scanner = TScan()
        if last_row != None:
            scanner.startRow = last_row
        id = c.scannerOpenWithScan('users', scanner, None)
        result = c.scannerGetList(id, each_page)
        return fmt(result)

    @cn_encode
    @cn_decode
    def search_user(self, substr, last_row=None, each_page=30):
        c = client()
        # calculate start row/end row
        start_row = (page - 1) * each_page + 1
        # scanner
        scanner = TScan()
        if last_row is not None:
            scanner.startRow = last_row
        scanner.filterString = "SingleColumnValueFilter('info', 'username', =, 'substring:%s', true, true)" % substr
        id = c.scannerOpenWithScan('users', scanner, None)
        result = c.scannerGetList(id, each_page)
        return fmt(result)


class PinDB(driver.PinDatabaseDriver):
    def __create_table(self):
        c = client()
        columns = [
            ColumnDescriptor(name='info:introduction', maxVersions=5),
            ColumnDescriptor(name='info:comment_count'),
            ColumnDescriptor(name='info:favorite_count'),
            ColumnDescriptor(name='info:board_id'),
            ColumnDescriptor(name='info:author_id'),
            ColumnDescriptor(name='info:type'),
            ColumnDescriptor(name='info:movie_id')
        ]
        c.createTable('pins', columns)

    @cn_decode
    def list(self, catalog=None, last_row=None, each_page=30):
        c = client()
        scanner = TScan()
        if last_row is not None:
            scanner.startRow = last_row
        if catalog != None:
            scanner.filterString = "SingleColumnValueFilter('info', 'board_id', =, 'binary:%s', true, true)" % catalog
        id = c.scannerOpenWithScan('pins', scanner, None)
        result = c.scannerGetList(id, each_page)
        if last_row == None:
            return fmt(result)
        else:
            return fmt(result)[1:]

    @cn_decode
    def list_by_user(self, id, catalog_id=None, last_row=None, each_page=30):
        c = client()
        scanner = TScan()
        if last_row is not None:
            scanner.startRow = last_row
        if catalog_id != None:
            scanner.filterString = "(SingleColumnValueFilter('info', 'author_id', =, 'binary:%s', true, true) AND SingleColumnValueFilter('info', 'board_id', =, 'binary:%s', true, true))" % (id, catalog_id)
        else:
            scanner.filterString = "SingleColumnValueFilter('info', 'author_id', =, 'binary:%s', true, true)" % id
        id = c.scannerOpenWithScan('pins', scanner, None)
        result = c.scannerGetList(id, each_page)
        if last_row == None:
            return fmt(result)
        else:
            return fmt(result)[1:]

    @cn_encode
    def create(self, given_id, author_id, introduction, board_id, movie_id=None):
        c = client()
        pin_fs = fs.FileSystem()

        # read image info
        try:
            img_src = Image.open(pin_fs.locate('/original_sources/%s.jpg' % given_id))
        except:
            img_src = Image.open(pin_fs.locate('/original_sources/%s' % given_id))
        src_width, src_height = img_src.size

        try:
            thumbnail = Image.open(pin_fs.locate('/thumbnails/%s.jpg' % given_id))
        except:
            thumbnail = Image.open(pin_fs.locate('/thumbnails/%s' % given_id))
        thumbnail_width, thumbnail_height = thumbnail.size

        try:
            middle_size = Image.open(pin_fs.locate('/middle_size/%s.jpg' % given_id))
        except:
            middle_size = Image.open(pin_fs.locate('/middle_size/%s' % given_id))
        middle_size_width, middle_size_height = middle_size.size

        img_info = {
            'img_src_width': src_width,
            'img_src_height': src_height,
            'thumbnail_width': thumbnail_width,
            'thumbnail_height': thumbnail_height,
            'middle_size_width': middle_size_width,
            'middle_size_height': middle_size_height
        }

        # img extension
        img_ext = img_src.format
        if img_ext == 'JPEG':
            img_ext = 'jpg'
        img_ext = string.lower(img_ext)

        mutations = [
            Mutation(column="info:introduction", value=introduction),
            Mutation(column="info:comment_count", value='0'),
            Mutation(column="info:favorite_count", value='0'),
            Mutation(column="info:board_id", value=str(board_id)),
            Mutation(column="info:author_id", value=str(author_id)),
            Mutation(column="info:img_ext", value=img_ext),
            Mutation(column="info:img_info", value=json.dumps(img_info))
        ]

        if movie_id != None:
            mutations.append(Mutation(column="info:type", value="movie"))
            mutations.append(Mutation(column="info:movie_id", value=movie_id))
        else:
            mutations.append(Mutation(column="info:type", value="picture"))
            mutations.append(Mutation(column="info:movie_id", value=""))

        c.mutateRow('pins', given_id, mutations, {})
        return {
            "id": given_id
        }

    @cn_encode
    def create_movie(self, author_id, introduction, board_id, movie_id):
        return self.create(new_id_2(), author_id, introduction, board_id, movie_id)

    @cn_encode
    def update(self, pin_id, **kwargs):
        c = client()
        mutations = []
        if kwargs.has_key('introduction'):
            mutations.append(Mutation(column="info:introduction", value=kwargs['introduction']))
        if kwargs.has_key('board_id'):
            mutations.append(Mutation(column="info:board_id", value=kwargs['board_id']))

        c.mutateRow('pins', pin_id, mutations, {})

    def delete(self, pin_id):
        c = client()
        c.deleteAllRow('pins', pin_id, {})

    @cn_decode
    def get(self, pin_id):
        c = client()
        row = c.getRow('pins', pin_id, {})
        return fmt(row)[0]

    def up(self, pin_id):
        pin = self.get(pin_id)
        self.update(pin_id, int(pin['favorite_count']) + 1)

    def cancel(self, pin_id):
        pin = self.get(pin_id)
        self.update(pin_id, int(pin['favorite_count']) - 1)

    # Put a patch to image size.
    def img_size_to_id(self, pin_id, width, height, new_width, new_height):
        c = client()
        mutations = []
        mutations.append(Mutation(column="info:img_src_w", value=str(width)))
        mutations.append(Mutation(column="info:img_src_h", value=str(height)))
        mutations.append(Mutation(column="info:img_t_w", value=str(new_width)))
        mutations.append(Mutation(column="info:img_t_h", value=str(new_height)))

        c.mutateRow('pins', pin_id, mutations, {})

    @cn_encode
    @cn_decode
    def search(self, keyword, last_row=None, page_size=30):
        c = client()
        scanner = TScan()
        if last_row is not None:
            scanner.startRow = last_row
        scanner.filterString = "SingleColumnValueFilter('info', 'introduction', =, 'substring:%s', true, true)" % keyword
        id = c.scannerOpenWithScan('pins', scanner, None)
        result = c.scannerGetList(id, page_size)
        if last_row == None:
            return fmt(result)
        else:
            return fmt(result)[1:]


class CommentDB(driver.CommentDatabaseDriver):
    def __create_table(self):
        c = client()
        columns = [
            ColumnDescriptor(name='info:pin_id'),
            ColumnDescriptor(name='info:user_id'),
            ColumnDescriptor(name='info:content', maxVersions=3)
        ]
        c.createTable('comments', columns)

    @cn_decode
    def list(self, pin_id=None, last_row=None, each_page=30):
        c = client()
        scanner = TScan()
        if last_row is not None:
            scanner.startRow = last_row
        if pin_id != None:
            scanner.filterString = "SingleColumnValueFilter('info', 'pin_id', =, 'binary:%s', true, true)" % pin_id
        id = c.scannerOpenWithScan('comments', scanner, None)
        result = c.scannerGetList(id, each_page)
        if last_row == None:
            return fmt(result)
        else:
            return fmt(result)[1:]

    @cn_encode
    def create(self, pin_id, user_id, content):
        c = client()
        mutations = [
            Mutation(column="info:pin_id", value=pin_id),
            Mutation(column="info:user_id", value=user_id),
            Mutation(column="info:content", value=content),
            Mutation(column="info:depth", value="0"),
        ]
        comment_id = new_id_1()
        c.mutateRow('comments', comment_id, mutations, {})
        return {
            "key": comment_id,
            "pin_id": pin_id,
            "user_id": user_id,
            "content": content
        }

    @cn_encode
    def get(self, comment_id):
        c = client()
        row = c.getRow('comments', comment_id, {})
        result = fmt(row)[0]
        return result

    @cn_encode
    def reply(self, pin_id, user_id, comment_id, content):
        comment = self.get(comment_id)
        depth = int(comment['depth']) + 1

        c = client()
        mutations = [
            Mutation(column="info:pin_id", value=pin_id),
            Mutation(column="info:user_id", value=user_id),
            Mutation(column="info:content", value=content),
            Mutation(column="info:depth", value=str(depth))
        ]
        c.mutateRow('comments', '%s_%s' % (comment_id, new_id_1()), mutations, {})
        return {
            "key": comment_id,
            "pin_id": pin_id,
            "user_id": user_id,
            "content": content,
            "depth": depth
        }

    @cn_encode
    def update(self, comment_id, **kwargs):
        c = client()
        mutations = []
        if kwargs.has_key('introduction'):
            mutations.append(Mutation(column="info:introduction", value=kwargs['introduction']))
        if kwargs.has_key('email'):
            mutations.append(Mutation(column="info:email", value=kwargs['email']))
        if kwargs.has_key('username'):
            mutations.append(Mutation(column="info:username", value=kwargs['username']))
        if kwargs.has_key('content'):
            mutations.append(Mutation(column="info:content", value=kwargs['content']))
        c.mutateRow('comments', comment_id, mutations, {})

    def delete(self, comment_id):
        c = client()
        c.deleteAllRow('comments', comment_id, {})

class FollowDB(driver.FollowDatabaseDriver):
    def __create_table(self):
        c = client()
        columns = [
            ColumnDescriptor(name='info:A'),
            ColumnDescriptor(name='info:B'),
        ]
        c.createTable('follows', columns)

    def fo(self, user_id_A, user_id_B):
        c = client()
        user = UserDB()

        if user.get_user_by_id(user_id_B).has_key('error'):
            return {
                "error": "cannot follow the user twice."
            }

        mutations = [
            Mutation(column="info:A", value=user_id_A),
            Mutation(column="info:B", value=user_id_B)
        ]
        fo_id = new_id_1()
        c.mutateRow('follows', fo_id, mutations, {})
        return {
            "id": fo_id,
            "A": user_id_A,
            "B": user_id_B
        }

    def _search_relationship_keys(self, user_id_a, user_id_b):
        c = client()
        scanner = TScan()
        scanner.filterString = "(SingleColumnValueFilter('info', 'A', =, 'binary:%s', true, true) AND SingleColumnValueFilter('info', 'B', =, 'binary:%s', true, true))" % (user_id_a, user_id_b)
        scan_id = c.scannerOpenWithScan('follows', scanner, None)
        result = c.scannerGetList(scan_id, 20)

        return fmt(result)

    def unfo(self, user_id_a, user_id_b):
        c = client()
        result = self._search_relationship_keys(user_id_a, user_id_b)

        for r in result:
            c.deleteAllRow('follows', r['key'], {})

        return {
            "status": "unfo"
        }

    @cn_decode
    def list_following(self, user_id, last_row=None, each_page=30):
        c = client()
        # start/stop rows
        scanner = TScan()
        if last_row != None:
            scanner.startRow = last_row
        # scanner
        scanner.filterString = "SingleColumnValueFilter('info', 'A', =, 'binary:%s', true, true)" % user_id
        id = c.scannerOpenWithScan('follows', scanner, None)
        result = c.scannerGetList(id, each_page)
        # load users
        user = UserDB()
        user_list = []
        for r in result:
            try:
                u = user.get_user_by_id(r.columns['info:B'].value)
            except IndexError:
                pass
            user_list.append(u)

        if last_row == None:
            return user_list
        else:
            return user_list[1:]

    @cn_decode
    def list_follower(self, user_id, last_row=None, each_page=30):
        c = client()
        # start/stop rows
        scanner = TScan()
        if last_row != None:
            scanner.startRow = last_row
        # scanner
        scanner.filterString = "SingleColumnValueFilter('info', 'B', =, 'binary:%s', true, true)" % user_id
        id = c.scannerOpenWithScan('follows', scanner, None)
        result = c.scannerGetList(id, each_page)
        # load users
        user = UserDB()
        user_list = []
        for r in result:
            try:
                u = user.get_user_by_id(r.columns['info:A'].value)
            except IndexError:
                pass
            user_list.append(u)

        if last_row == None:
            return user_list
        else:
            return user_list[1:]

    def is_follower(self, user_id, user_id_2):
        c = client()
        scanner = TScan()
        # scanner
        scanner.filterString = "(SingleColumnValueFilter('info', 'A', =, 'binary:%s', true, true) AND SingleColumnValueFilter('info', 'B', =, 'binary:%s', true, true))" % (user_id, user_id_2)
        id = c.scannerOpenWithScan('follows', scanner, None)
        result = c.scannerGetList(id, 10)
        r = fmt(result)
        if len(r) == 0:
            return False
        else:
            return True


class ReminderDB:
    def __create_table(self):
        c = client()
        columns = [
            ColumnDescriptor(name='user_id'),
            ColumnDescriptor(name='content'),
            ColumnDescriptor(name='link'),
            ColumnDescriptor(name='from_user_id'),
            ColumnDescriptor(name='read')
        ]
        c.createTable('reminder', columns)

    @cn_encode
    def create(self, user_id, content, link, from_user_id):
        c = client()
        mutations = [
            Mutation(column="info:user_id", value=user_id),
            Mutation(column="info:content", value=content),
            Mutation(column="info:link", value=link),
            Mutation(column="info:from_user_id", value=from_user_id),
            Mutation(column="info:read", value="unread"),
        ]
        remind_id = new_id_1()
        c.mutateRow('reminder', remind_id, mutations, {})
        return {
            "remind_id": remind_id,
            "user_id": user_id,
            "content": content,
            "link": link,
            "from_user_id": from_user_id,
        }

    @cn_decode
    def unread_list(self, user_id):
        c = client()
        scanner = TScan()
        scanner.filterString = "SingleColumnValueFilter('info', 'read', =, 'binary:unread', true, true) AND SingleColumnValueFilter('info', 'user_id', =, 'binary:%s', true, true)" % user_id
        scan_id = c.scannerOpenWithScan('reminder', scanner, None)
        result = c.scannerGetList(scan_id, 100)
        return fmt(result)

    @cn_decode
    def last_ten_read_list(self, user_id):
        c = client()
        scanner = TScan()
        scanner.filterString = "SingleColumnValueFilter('info', 'read', =, 'binary:read', true, true) AND SingleColumnValueFilter('info', 'user_id', =, 'binary:%s', true, true)" % user_id
        scan_id = c.scannerOpenWithScan('reminder', scanner, None)
        result = c.scannerGetList(scan_id, 10)
        return fmt(result)

    def set_read(self, user_id):
        li = self.unread_list(user_id)
        c = client()
        mutations = [
            Mutation(column="info:read", value="read"),
        ]
        for l in li:
            c.mutateRow('reminder', l['key'], mutations, {})


class ProductDB:
    pass


class ShoppingDB:
    pass


#class DingDB:
#    def ding(self):
#        c = client()
#        mutations = [
#            Mutation(column="info:user_id", value=user_id),
#            Mutation(column="info:content", value=content),
#            Mutation(column="info:link", value=link),
#            Mutation(column="info:from_user_id", value=from_user_id),
#            Mutation(column="info:read", value="unread"),
#        ]
#        remind_id = new_id_1()
#        c.mutateRow('reminder', remind_id, mutations, {})
#        return {
#            "remind_id": remind_id,
#            "user_id": user_id,
#            "content": content,
#            "link": link,
#            "from_user_id": from_user_id,
#        }


