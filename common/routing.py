from webob.dec import wsgify
import webob

@webob.dec.wsgify
def dispatch(req):
    match = req.environ['wsgiorg.routing_args'][1]

    if not match:
        return webob.exc.HTTPNotFound()

    app = match['controller']
    return app
