import rpcserver
import socket
from threading import Thread, RLock, Condition
from struct import pack, unpack, error
from collections import namedtuple

class HiveNode:
    
    max_attempts = 10
    time_to_wait = 0.1

    def __init__(self, rpc_port : int, broadcast_port : int, propagation_port : int = None, auto_propagation = True):

        self.rpc_server = rpcserver.HiveXMLRPCServer(("localhost", rpc_port))
        self.rpc_server.register_function(self.Hive_node_test)
        self.rpc_server_at = rpc_port

        self._rpc_thread = Thread(target = self.rpc_server.serve_forever)
        self._rpc_thread.start()

        self.broadcast_port = broadcast_port
        self.auto_propagation = auto_propagation
        self.propagation_port = propagation_port if propagation_port else broadcast_port
        self._current_serv = (None, None, None)

        self.udp_server = None
        self._udp_server_thread = None
        
        self.broadcast_blacklist = set()
        self.broadcast_blacklist_cond = Condition()
        self.clear_blacklist_start()

    def clear_blacklist_start(self):

        def clear():
            while True:
                with self.broadcast_blacklist_cond:
                    if not self.broadcast_blacklist_cond.wait(HiveNode.time_to_wait*20):
                        print("cleaning")
                        self.broadcast_blacklist.clear()

        Thread(target= clear, daemon= True).start()

    def Hive_node_test(self, a):
        print("{0} -> Node Test".format(a))
        return "ACK"

    def propagate_address(self, addr):

        def propagation():
            rpcserver.PropagationHandler.setdata(addr)
            self.udp_server.serve_forever()

        if self.udp_server != None:
            self.udp_server.shutdown()
            self.udp_server = None

        self.udp_server = rpcserver.UDPServer(("localhost", self.propagation_port), rpcserver.PropagationHandler)
        Thread(target = propagation, daemon = True).start()

    def _locate_server(self):
        HOST = '<broadcast>'
        PORT = self.broadcast_port
        ADDR = HOST,PORT
        BUFSIZ = 100

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(self.time_to_wait)
        s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)

        self_message = True
        k = 0
        while k < self.max_attempts:

            s.sendto('hive server location'.encode(),ADDR)
            if self_message:
                s.sendto('hive server location'.encode(), 0, ("127.0.0.1", PORT))

            try:
                j = 0
                with self.broadcast_blacklist_cond:
                    n = len(self.broadcast_blacklist) + 1

                while j < n:
                    data, naddr = s.recvfrom(BUFSIZ)

                    with self.broadcast_blacklist_cond:
                        if naddr not in self.broadcast_blacklist:
                            break
                    
                    j += 1

                try:
                    i = unpack('<i', data[:4])[0]

                    try:
                        self._current_serv = unpack('<{0}si'.format(i),data[4:8+i])
                        self._current_serv = (self._current_serv[0].decode(), self._current_serv[1], naddr)
                        return

                    except error:
                        self._current_serv = (naddr[0], i, naddr)
                        return

                except error:
                    with self.broadcast_blacklist_cond:
                        self.broadcast_blacklist.add(naddr)
                    

            except socket.timeout as to:
                print(to)

            except OSError as o:
                print(o)
                self_message = False

            k += 1

        self._current_serv = (None, None, None)

    def get_proxy(self, target : (str, int) = None, owntest = False):
        if not target:
            if self._current_serv[0]:
                target = self._try_connect(self._current_serv[:2], owntest)
                if target:
                    return target

            i = 0
            while i < HiveNode.max_attempts:
                print("locate server ", i)
                self._locate_server()

                if self._current_serv[0]:
                    target = self._try_connect(self._current_serv[:2], owntest, False)
                    if target:

                        with self.broadcast_blacklist_cond:
                            self.broadcast_blacklist_cond.notify()

                        if self.auto_propagation:
                            self.propagate_address(self._current_serv[:2])
                        return target
                    else:
                        with self.broadcast_blacklist_cond:
                            self.broadcast_blacklist.add(self._current_serv[2])
                        self._current_serv = (None, None, None)

                i += 1
        else:
            target = self._try_connect(target, owntest)
            if target:
                return target

        raise Exception

    def _test_connection(self, proxy, paddr):
        try:
            result = proxy.Hive_node_test()
            if result == "ACK":
                return True
            
            return False

        except Exception as e:
            print(e)
            return False

    def _try_connect(self, addr, owntest, repeat = True):
        for x in range(self.max_attempts if repeat else 1):
            try:
                client = rpcserver.ServerProxyHiveWrapper(addr, self.rpc_server_at)
                if self._test_connection(client, addr if owntest else None):
                    return client
            except Exception as e:
                print(e)

    def set_current_server(self, addr):
        self._current_serv = (*addr, None)

        if not self.auto_propagation:
            return

        if addr:
            if self._try_connect(addr, False):
                self.propagate_address(addr)
        elif self.udp_server != None:
            self.udp_server.shutdown()
            self.udp_server = None


            
class Task:
    """
        keyargs:
        [R]name / _name                    str                 the name of the tast, must be a key in the dependencies dict
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