import socket
from struct import pack, unpack, error

def _locate_server():
        HOST = '<broadcast>'
        PORT = 4000
        ADDR = HOST,PORT
        BUFSIZ = 100

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
        for x in range(10): 
            s.sendto('hive server location'.encode(), 0, ADDR)
            #s.sendto('hive server location'.encode(), 0, ("127.0.0.1", PORT))
            try:
                data, naddr = s.recvfrom(BUFSIZ)
                i = unpack('<i', data[:4])[0]
                try:
                    return unpack('<{0}si'.format(i),data[4:8+i])
                except error:
                    return (naddr, i)
            except socket.timeout:
                pass

print(_locate_server())