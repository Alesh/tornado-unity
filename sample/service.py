""" Sample service
"""
import sys
import os.path
import tornado.web
import unity
from unity import Service, ModuleConfig as Config, fqc_name


class IndexHandler(tornado.web.RequestHandler):
    """ Index page
    """

    def get(self):
        self.render('index.html')

    async def post(self):
        message = self.get_argument('message')
        result = await self.application.service.remote_call('__main__.ProcessA', 'sync_call', message)
        self.write({'result': result})


class Application(tornado.web.Application):
    """ Service console
    """

    def __init__(self, service):
        self.service = service
        tornado.web.Application.__init__(self, [
            (r'/', IndexHandler),
            (r'/send', IndexHandler),
        ], **{
            'template_path': os.path.join(os.path.dirname(__file__), 'templates'),
            'static_path': os.path.join(os.path.dirname(__file__), 'static'),
            'debug': service.config.debug,
        })


class ProcessA(unity.SubProcess):
    """ Process A
    """

    def sync_call(self, marker):
        return {'sync_call': fqc_name(self), 'marker': marker}


class ProcessB(unity.SubProcess):
    """ Process B
    """

    async def async_call(self, marker):
        return {'async_call': fqc_name(self), 'marker': marker}


if __name__ == '__main__':

    default_conf = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'default.conf')
    if len(sys.argv) > 1 and sys.argv[1].endswith('.conf'):
        config = Config(default_conf, sys.argv[1])
    else:
        config = Config(default_conf)
    service = Service(config)
    service.start(Application, ProcessA, ProcessB)
