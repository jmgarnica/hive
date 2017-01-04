from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from xmlrpc.client import ServerProxy
from socketserver import DatagramRequestHandler, UDPServer
from threading import local
from struct import pack

# thread static "addr" tuple<string, int>: address to propageate
_local_data = local() 

class HiveXMLRPCServer(SimpleXMLRPCServer):
    
    def __init__(self, addr,
                 requestHandler = SimpleXMLRPCRequestHandler,
                 logRequests = True,
                 encoding = None,
                 bind_and_activate = True, 
                 use_builtin_types = False):

        return super().__init__(addr, 
                                requestHandler,
                                logRequests, True,
                                encoding, bind_and_activate,
                                use_builtin_types)

    def _dispatch(self, method, params):
        return super()._dispatch(method, (self.get_request()[1],*params))

class PropagationHandler(DatagramRequestHandler):

    def handle(self):

        if not "addr" in _local_data.__dict__:
           return

        self.wfile.write(pack('<i{0}si'.format(len(self._local_data.addr[0])),
                             _local_data.addr[0],
                             _local_data.addr[1]))

        return super().handle()

    @staticmethod
    def setdata(addr):
        """setdata: tuple<string, int> -> NoneType"""
        _local_data.addr = addr