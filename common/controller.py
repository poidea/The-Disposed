from webob.dec import wsgify
import webob
import simplejson
import db.hadoop_hbase as h

def render(dictionary):
    return webob.Response(simplejson.dumps(dictionary))

def need_login(func):
    def _need_login(*args, **kwargs):
        user_db = h.UserDB()
        try:
            user_db.get_user_from_token(args[1].headers['X-Token'])
        except IndexError:
            return render({
                "error": "need login"
            })
        ret = func(*args, **kwargs)
        return ret
    return _need_login

class Controller:
    @webob.dec.wsgify
    def __call__(self, req):
        arg_dict = req.environ['wsgiorg.routing_args'][1]
        del arg_dict['controller']

        action = arg_dict.pop('action')
        call = getattr(self, action)
        params = arg_dict

        if req.content_type == 'application/json':
            body = simplejson.loads(req.body)
            result = call(req, body, **params)
        else:
            result = call(req, **params)

        if isinstance(result, webob.Response):
            if result.content_type != 'image/jpg' and \
                result.content_type != 'image/bmp' and \
                result.content_type != 'image/gif' and \
                result.content_type != 'image/png':
                result.content_type = 'application/json'

        return result


