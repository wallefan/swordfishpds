import socketserver
import os
import socket
import shutil

class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        for item in os.listdir('.'):
            if item.endswith('.csv'):
                self.wfile.write((item[:-4]+'\n').encode('ascii'))
        self.wfile.write(b'\n')
        requestedPack = self.rfile.readline().decode('ascii').strip()+'.txt'
        if not os.path.exists(os.path.join(os.path.curdir, requestedPack)):
            self.connection.sendall(b'FIN')
        else:
            print('transmitting pack', requestedPack)
            with open(requestedPack, 'rb') as f:
                self.connection.sendfile(f)

if __name__=='__main__':
    socketserver.TCPServer(('',21617), Handler).serve_forever()