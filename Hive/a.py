import rpcserver

def propagate_address(addr):

    udp_server = rpcserver.UDPServer(("localhost", 4000), rpcserver.PropagationHandler)

    rpcserver.PropagationHandler.setdata(addr)
    udp_server.serve_forever()

propagate_address(("localhost", 2000))