from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from xmlrpc.client import ServerProxy
from socketserver import DatagramRequestHandler, UDPServer
from threading import local
from struct import pack

# thread static "addr" tuple<string, int>: address to propageate
_local_data = local() 

class HiveXMLRPCRequestHandler(SimpleXMLRPCRequestHandler):

    def do_POST(self):
        self.server.addr = self.client_address
        return super().do_POST()
    
class HiveXMLRPCServer(SimpleXMLRPCServer):
    
    def __init__(self, addr,
                 logRequests = False,
                 encoding = None,
                 bind_and_activate = True, 
                 use_builtin_types = False):

        self.addr = None
        return super().__init__(addr, 
                                HiveXMLRPCRequestHandler,
                                logRequests, True,
                                encoding, bind_and_activate,
                                use_builtin_types)

    def _dispatch(self, method, params):

        if method.split("_")[0] == "Hive":            
            return super()._dispatch(method, ((self.addr[0], params[0]),*(params[1:])))
        else:
            return super()._dispatch(method, params)

class ServerProxyHiveWrapper:
    
    def __init__(self, addr, server_port):
        self._proxy =  ServerProxy("http://{0}:{1}".format(addr[0], addr[1]))
        self._prot = server_port

    def __getattr__(self, name):
                
        if name.split("_")[0] == "Hive":
            return ServerProxyHiveWrapper._called(self._proxy.__getattr__(name), self._prot)
        else:
            return self._proxy.__getattr__(name)

    @staticmethod
    def _called(f, p1):          
         def call(*p, **k):
             return f(p1, *p, **k)
        
         return call

class PropagationHandler(DatagramRequestHandler):

    def handle(self):

        if "addr" in _local_data.__dict__:
            s = _local_data.addr[0].encode()
            self.wfile.write(pack('<i{0}si'.format(len(s)),
                                 len(s), s, _local_data.addr[1]))
            return super().handle()

        if "sock" in _local_data.__dict__:
            self.wfile.write(pack("<i", _local_data.sock))
            return super().handle()

    @staticmethod
    def setdata(addr):
        """setdata: (tuple<string, int> || int) -> None"""
        if isinstance(addr, int):
            _local_data.sock = addr
        else:
            _local_data.addr = addr