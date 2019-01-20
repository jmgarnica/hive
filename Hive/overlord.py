from threading import Thread, Condition, RLock, Lock
import queue
import hivenode

class Overlord(hivenode.HiveNode):

    def __init__(self, host_port : int, enter_point : (str, int) = None,
                 broadcast_port : int = 4000, propagation_port : int = 4000):
        super().__init__(host_port, broadcast_port, propagation_port, False)

        self._register_server()

        self.propagate_address(host_port)
        if __name__ == "__main__":
            print("overlord running")

        self.main_server = None
        self.backup_server = None
        self.stand_alone = True
        self.connection_lock = Condition()
        self.enter_cond = Condition()

        self.self_test_addr = None
        self.self_test_cond = Condition()

    def _register_server(self):
        self.rpc_server.register_function(self.Hive_overlord_self_test)
        self.rpc_server.register_function(self.Hive_EnterRing)
        self.rpc_server.register_function(self.Hive_Receive_EnterRing)
        self.rpc_server.register_function(self.Hive_SetPredecesor)
        self.rpc_server.register_function(self.Hive_Address)
        self.rpc_server.register_function(self.Hive_Who)
        self.rpc_server.register_function(self.Hive_Replicate)
        self.rpc_server.register_function(self.Hive_Consume)
        self.rpc_server.register_function(self.Hive_PutTask)
        self.rpc_server.register_function(self.Hive_GetTask)
        self.rpc_server.register_function(self.Hive_GetTask)
        self.rpc_server.register_function(self.Hive_Hello)
        self.rpc_server.register_function(self.Hive_Register)

    #RPC may end

    def Hive_EnterRing(self, a : (str, int)) -> None:
        print("{0} -> Enter Ring".format(a))
        pass

    def Hive_Receive_EnterRing(self, a: (str, int), o: (str, int), name: str) -> None:
        print("{0} -> Receive Enter Ring".format(a))
        pass

    def Hive_SetPredecesor(self, a : (str, int)) -> None:
        print("{0} -> Set Predecesor".format(a))
        pass

    def Hive_Address(self, a : (str, int), i : int) -> None:
        print("{0} -> Address".format(a))
        pass

    def Hive_Who(self, c : int, a : (str, int), i : int) -> None:
        print("{0} -> Who".format(a))
        pass

    def Hive_Replicate(self, t : {}, sf : int, sc : int, rc : int) -> None:
        print("{0} -> Replicate".format(a))
        pass

    def Hive_Consume(self, a : (str, int)) -> None:
        print("{0} -> Consume".format(a))
        pass

    def Hive_PutTask(self, t : {}) -> None:
        print("{0} -> Put Task".format(a))
        pass
    
    def Hive_GetTask(self, a : (str, int)) -> None:
        print("{0} -> Get Task".format(a))

    def Hive_Hello(self, a : (str, int)) -> None:
        print("{0} -> Hello".format(a))
        pass
    
    def Hive_Register(self, a : (str, int)):
        print("{0} -> Register".format(a))
        pass 

    #RPC other end

    def _Hive_SetPredecesor(self, target, a) -> None:
        pass

    #methods

    def Hive_overlord_self_test(self, a):
        
        print("{0} -> Overlord Self Connection Test".format(a))

        def test():
            with self.self_test_cond:
                if self.self_test_addr == a:
                    self.self_test_cond.notify()
        Thread(target= test, daemon= True).start()

    def _test_connection(self, proxy, paddr):
        t = super()._test_connection(proxy, paddr)

        if t and paddr:
            with self.self_test_cond:
                self.self_test_addr = paddr
                proxy.Hive_overlord_self_test()
                if self.self_test_cond.wait(self.time_to_wait):
                    return False

        return t
                    
    def get_server_proxy(self):
        with self.connection_lock:
            stage = 0 if self.stand_alone else 3
            proxy = None
            server = None
            #todo add to black list
            while not server:

                if stage == 0:
                    try:
                        proxy = self.get_proxy(owntest= True)
                        stage = 1
                        counter = self.max_attempts
                    except:
                        self.stand_alone = True
                        return None

                if stage == 1:
                    self.main_server = None
                    self.backup_server = None
                    waittime = self.time_to_wait*(self.max_attempts - counter + 1)
                    try:
                        proxy.Hive_EnterRing()
                        with self.enter_cond:

                            stage = 2
                            self.enter_cond.wait(waittime)
                            if not self.main_server:
                                stage = 1
                                counter -= 1
                            else:
                                server = proxy
                                self.stand_alone = False
                    except:
                        stage = 1
                        counter -= 1
                        if counter == 0:
                            stage = 0

                if stage == 3:
                    try:
                        server = get_proxy(self.main_server)
                    except:
                        stage = 4
                        counter = self.max_attempts

                if stage == 4:
                    waittime = self.time_to_wait*(self.max_attempts - counter + 1)

                    try:
                        proxy = get_proxy(self.backup_server)
                    except:
                        stage = 1
                        continue

                    try:
                        self.main_server = None
                        proxy.Hive_Who(-1, None, -1)

                        stage = 5
                        self.connection_lock.wait(waittime)
                        #TODO: notify in SetSuccesor
                        if not self.main_server:
                            stage = 4
                            counter -= 1
                        else:
                            stage = 3
                    except:
                        stage = 4
                        counter -= 1
                        if counter == 0:
                            stage = 0

            return server

if __name__ == "__main__":
    o = Overlord(4003)

    print(o.get_server_proxy())
    print(o.get_server_proxy())