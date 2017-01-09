import rpcserver
from threading import Thread, RLock, Condition
from struct import pack, unpack
from collections import namedtuple

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

class Task:
    """
        keyargs:
        [R]name / _name                    str                 the name of the tast, must be include in the dependencies dict
        [R]data / _data                    str                 serialized data to start the task
        [ ]owner / _owner                  (str, int)          IP direction of HIVE-RPC server awaiting the result
        [R]id / _id                        int                 the id of the task in the process where it comes from
        [ ]result / _result                str                 the result of the task (serialized)
        [R]dependencies / _dependencies    {str: Dependency}   a dictionary of dependencies, the value of each key is a tuple (code, path)
        """

    def __init__(self, **keyargs):
        
        params = [("name", True), ("data", True), ("owner", False), ("id", True), ("result", False), ("dependencies", True)]

        for p in params:
            if p[0] in keyargs:
                self.__setattr__("_" + p[0], keyargs[p[0]])
            elif "_" + p[0] in keyargs:
                self.__setattr__("_" + p[0], keyargs["_" + p[0]])
            elif p[1]:
                raise Exception("argument required: " + p[0])

        if not self._name in self._dependencies:
            raise Exception("target task must be included in dependencies dict")

        self.done = False

    @property
    def task_id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data
    
    @property
    def result(self):
        return self._result

    @result.setter
    def set_result(self, result : str) -> None:
        self._result = result

    @property
    def owner(self):
        return self._owner

    def dep_import(self, name):
        
        if not name in self._dependencies:
            raise Exception("use this to import only task dependencies")
        
        from importlib import import_module
        return import_module(self._dependencies[name].path)

class Dependency:
    def __init__(self, code = None, path = None):
        self.code = code
        self.path = path

    def __getitem__(self, key):
        if key == 0:
            return self.code
        if key == 1:
            return self.path
        if key == "code":
            return self.code
        if key == "path":
            return self.path

        raise AttributeError()

    def __setitem__(self, key, value):
        if key == 0:
            self.code = value
        if key == 1:
            self.path = value
        if key == "code":
            self.code = value
        if key == "path":
            self.path = value

        raise AttributeError()

class ServerInfo:

    def __init__(self, enter_address = None):
       
        self.address = None
        self.predecessor = None
        self.successor = None
        self.connected = False
        self._lock = RLock()
        self.enter_address = enter_address
        self.hello = None
        self._hello_condition = Condition()
        self._register_condition = Condition()

    @property
    def lock(self):
        return self._lock

    @property
    def hello_condition(self):
        return self._hello_condition

    @property
    def register_condition(self):
        return self._register_condition

AwaitedTask = namedtuple('AwaitedTask', ['task', 'condition'])