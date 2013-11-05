from fs import os as fs
import Image
import db.hadoop_hbase as h

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

import simplejson as json

pindb = h.PinDB()
pin_fs = fs.FileSystem()

last_row = None
ls = pindb.list()

def client():
    transport = TSocket.TSocket('192.168.1.241', 9090)
    transport = TTransport.TBufferedTransport(transport)

    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = Hbase.Client(protocol)
    transport.open()

    return client

while len(ls) > 0:
    for i, val in enumerate(ls):
        try:
            try:
                img_src = Image.open(pin_fs.locate('/original_sources/%s.jpg' % val['key']))
            except:
                img_src = Image.open(pin_fs.locate('/original_sources/%s' % val['key']))

            src_width, src_height = img_src.size

            try:
                thumbnail = Image.open(pin_fs.locate('/thumbnails/%s.jpg' % val['key']))
            except:
                thumbnail = Image.open(pin_fs.locate('/thumbnails/%s' % val['key']))

            thumbnail_width, thumbnail_height = thumbnail.size

            try:
                middle_size = Image.open(pin_fs.locate('/middle_size/%s.jpg' % val['key']))
            except:
                middle_size = Image.open(pin_fs.locate('/middle_size/%s' % val['key']))

            middle_size_width, middle_size_height = middle_size.size

            img_info = {
                'img_src_width': src_width,
                'img_src_height': src_height,
                'thumbnail_width': thumbnail_width,
                'thumbnail_height': thumbnail_height,
                'middle_size_width': middle_size_width,
                'middle_size_height': middle_size_height
            }
        except:
            img_info = {}

        c = client()
        mutations = [
            Mutation(column="info:img_info", value=json.dumps(img_info)),
        ]
        c.mutateRow('pins', val['key'], mutations, {})

        last_row = val['key']

    ls = pindb.list(last_row=last_row)


