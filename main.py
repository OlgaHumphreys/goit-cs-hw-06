from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from multiprocessing import Process
import mimetypes
import json
import urllib.parse
import pathlib
import socket
import logging

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb://mongodb:27017"

UDP_IP = '127.0.0.1'
UDP_PORT = 5000
def send_data_to_socket(data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = UDP_IP, 5000
    sock.sendto(data, server)
    sock.close()


class HttpGetHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        data = self.rfile.read(int(self.headers['Content-Length']))
        send_data_to_socket(data)
        self.send_response(302)
        self.send_header('Location', '/')
        self.end_headers()

    def do_GET(self):
        pr_url = urllib.parse.urlparse(self.path)
        match pr_url.path:
            case '/':
                self.send_html_file('index.html')
            case '/message':
                self.send_html_file('message.html')
            case _:
                if pathlib.Path().joinpath(pr_url.path[1:]).exists():
                    self.send_static()
                else:
                    self.send_html_file('error.html', 404)

    def send_html_file(self, filename, status_code=200):
        self.send_response(status_code)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        with open(filename, 'rb') as fd:
            self.wfile.write(fd.read())

    def send_static(self):
        self.send_response(200)
        mt = mimetypes.guess_type(self.path)
        if mt:
            self.send_header('Content-type', mt[0])
        else:
            self.send_header('Content-type', 'text/plain')
        self.end_headers()
        with open(f'.{self.path}', 'rb') as fd:
            self.wfile.write(fd.read())

def run_http_server(server_class=HTTPServer, handler_class=HttpGetHandler):
    server_address = ('0.0.0.0', 3000)
    http = server_class(server_address, handler_class)
    logging.info(f'Server start: {server_address}')

    try:
        http.serve_forever()
    except Exception as err:
        logging.error(f'Server error: {err}')
        http.server_close()

def save_data(data):
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi("1"))
    # Send a ping to confirm a successful connection
    db = client.final_hw
    data_parse = urllib.parse.unquote_plus(data.decode())
    try:
        data_parse = {key: value for key, value in [el.split('=') for el in data_parse.split('&')]}
        data_parse['date'] = str(datetime.now())
        db.messages.insert_one(data_parse)

    except ValueError as err:
        logging.error(f'Failed to parse data: {err}')
    except Exception as err:
        logging.error(f'Failed to write or read data: {err}')
    finally:
        client.close()

def run_socket_server(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = ip, port
    sock.bind(server)
    try:
        while True:
            data, address = sock.recvfrom(1024)
            save_data(data)

    except Exception as err:
        logging.error(f'Error: {err}')
        logging.info(f'Destroy server')
    finally:
        sock.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(threadName)s %(message)s')

    th_server = Process(target=run_http_server, args=(HTTPServer, HttpGetHandler))
    th_server.start()

    th_socket = Process(target=run_socket_server, args=(UDP_IP, UDP_PORT))
    th_socket.start()