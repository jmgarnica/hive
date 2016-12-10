import xmlrpc
from threading import Thread
import socket
from struct import pack, unpack

class HiveNode:
    
    max_attemps = 10
    time_to_wait = 0.5

    def __init__(self, rpc_port : int, *, enterpoint : (str, int) = None, broadcastport : int = None, server : bool = False):
        self.rpc_server = xmlrpc.server.SimpleXMLRPCServer(("localhost", rpc_port))
        self._rpc_thread = Thread(target = self.rpc_server.serve_forever)

        self._broadcast_port = broadcastport
        self._current_serv = enterpoint if not server else find_my_ip()
        self._is_server = server

    def find_my_ip(self):
        pass

    def _propagate_address(self):

        HOST = ''
        PORT = self._broadcast_port
        ADDR = HOST, PORT
        BUFSIZ = 100

        bcast_sock = socket.socket(type = socket.SOCK_DGRAM)
        bcast_sock.bind(ADDR)
        bcast_sock.settimeout(self.time_to_wait)

        def propagation_loop():
            while True:
                if(self._current_serv):
                    try:
                        data, naddr = bcast_sock.recvfrom(BUFSIZ)
                        print(data)
                        data = pack('<i{0}si'.format(len(self._current_serv[0])),len(self._current_serv[0]), *self._current_serv)
                        bcast_sock.sendto(data, naddr)
                    except socket.timeout:
                        pass

        self._propagate_address_thread = Thread(target = propagation_loop)
        self._propagate_address_thread.start()

    def _locate_server(self):
        HOST = '<broadcast>'
        PORT = self._broadcast_port
        ADDR = HOST,PORT
        BUFSIZ = 100

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(self.time_to_wait)
        s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
        for x in range(self.max_attemps): 
            s.sendto('heeelp i need a server',ADDR)
            try:
                data, naddr = s.recvfrom(BUFSIZ)
                i = unpack('<i', data[:4])[0]
                self._current_serv = unpack('<{0}si'.format(i[0]),data[4:8+i])
            except socket.timeout:
                pass



    def get_proxy(self, client : (str, int) = None):
        if not client:
            if self._current_serv:
                client = self._try_connect(self._current_serv)
                if client:
                    return client
            self._locate_server()
            client = self._try_connect(self._current_serv)
            if client:
                return client

        else:
            client = self._try_connect(client)
            if client:
                return client

        raise Exception

    def _try_connect(self, addr):
        for x in range(self.max_attemps):
            try:
                client = xmlrpc.client.ServerProxy("http://{0}:{0}".format(addr[0], addr[1]))
                return client
            except:
                pass