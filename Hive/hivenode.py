import rpcserver
from threading import Thread
from struct import pack, unpack

class HiveNode:
    
    max_attempts = 10
    time_to_wait = 0.5

    def __init__(self, rpc_port : int, broadcastport : int):
        self.rpc_server = rpcserver.HiveXMLRPCServer(("localhost", rpc_port))
        self.rpc_server.register_function(self.Hive_node_test)

        self._rpc_thread = Thread(target = self.rpc_server.serve_forever)
        self._rpc_thread.start()

        self.broadcastport = broadcastport
        self.udp_server = None
        self._udp_server_thread = None

    def Hive_node_test(self):

        return "ACK"

    def propagate_address(self, addr):

        def propagation():
            rpcserver.PropagationHandler.setdata(addr)
            self.udp_server.serve_forever()

        if self.udp_server != None:
            self.udp_server.shutdown()
            self.udp_server = None

        self.udp_server = rpcserver.UDPServer(("localhost", self.broadcastport), rpcserver.PropagationHandler)
        Thread(target = propagation, daemon = True).start()

    def _locate_server(self):
        HOST = '<broadcast>'
        PORT = self.broadcastport
        ADDR = HOST,PORT
        BUFSIZ = 100

        if self.udp_server != None:
            self.udp_server.shutdown()
            self.udp_server = None

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(self.time_to_wait)
        s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
        for x in range(self.max_attempts): 
            s.sendto('heeelp i need a server',ADDR)
            try:
                data, naddr = s.recvfrom(BUFSIZ)
                i = unpack('<i', data[:4])[0]
                self._current_serv = unpack('<{0}si'.format(i[0]),data[4:8+i])
            except socket.timeout:
                pass

    def get_proxy(self, target : (str, int) = None):
        if not target:
            if self._current_serv:
                target = self._try_connect(self._current_serv)
                if target:
                    return target
            self._locate_server() #TODO: make a ban list for bad broadcast responses
            target = self._try_connect(self._current_serv)
            if target:
                self.propagate_address(self._current_serv)
                return target

        else:
            target = self._try_connect(target)
            if target:
                return target

        raise Exception

    def _test_connection(self, proxy):
        try:
            result = proxy.Hive_node_test()
            if result == "ACK":
                return True
            
            return False

        except:
            return False

    def _try_connect(self, addr):
        for x in range(self.max_attempts):
            try:
                client = xmlrpc.client.ServerProxy("http://{0}:{0}".format(addr[0], addr[1]))
                if self.test_connection(client):
                    return client
            except:
                pass

    def set_current_server(self, addr):
        self._current_serv = addr
        if addr:
            if self._try_connect(addr):
                self.propagate_address(addr)
        elif self.udp_server != None:
            self.udp_server.shutdown()
            self.udp_server = None
