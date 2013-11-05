# -*- coding: UTF-8 -*-

import eventlet
from eventlet import wsgi
from webob.dec import wsgify
import webob
import routes.middleware
from common.routing import dispatch

from users.routing import user_mappers
from pins.routing import pin_mappers
from comments.routing import comment_mappers
from attachments.routing import attachment_mappers
from fo.routing import fo_mappers
from reminder.routing import reminder_mappers

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

mapper = routes.Mapper()

# initialize Mappers
user_mappers(mapper)
pin_mappers(mapper)
comment_mappers(mapper)
attachment_mappers(mapper)
fo_mappers(mapper)
reminder_mappers(mapper)

router = routes.middleware.RoutesMiddleware(dispatch, mapper)
socket = eventlet.listen(('', 8080))
wsgi.server(socket, router)
