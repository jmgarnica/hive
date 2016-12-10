import xmlrpc
from threading import Thread
import socket

class HiveNode:
    
   
    def __init__(self, rpc_port : int, *, enterpoint : (str, int) = None, broadcastport : int = None):
        self.rpc_server = xmlrpc.server.SimpleXMLRPCServer(("localhost", rpc_port))
        self._rpc_thread = Thread(target = self.rpc_server.serve_forever)
        
        self._broadcast = broadcastport
        self._addrs = [] if enterpoint == None else [enterpoint]
        self.current_serv = []

    def _propagate_address(self):

        broadcast = socket.socket(type = socket.SOCK_DGRAM)
        broadcast.bind(("localhost", self._broadcast))
        
        def propagation_loop():
            #TODO: implement
            pass

        self._propagate_address_thread = Thread(target = propagation_loop)
        self._propagate_address_thread.start()

    def _locate_server(self):
        pass

    def find_my_ip():
        pass

    def get_proxy(self, client : (str, int) = None):
        pass