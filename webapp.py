import sys
import queue
import threading
from flask import Flask, render_template
from flask_socketio import SocketIO

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret'
socketio = SocketIO(app, async_mode='threading', cors_allowed_origins="*")

connected_peers = {}
peer_config = {}
config_ready = threading.Event()
terminal_input_queue = queue.Queue()

class WebTerminalOut:
    def __init__(self, original_stream):
        self.original_stream = original_stream

    def write(self, message):
        self.original_stream.write(message)
        self.original_stream.flush()
        if message and message.strip() != '':
            socketio.emit('terminal_output', {'data': message}, namespace='/')

    def flush(self):
        self.original_stream.flush()

sys.stdout = WebTerminalOut(sys.stdout)
sys.stderr = WebTerminalOut(sys.stderr)

def monitor_peers():
    last_peers = []
    while True:
        current_peers = list(connected_peers.keys())
        if current_peers != last_peers:
            socketio.emit('update_peers', current_peers, namespace='/')
            last_peers = current_peers.copy()
        socketio.sleep(2)

def enviar_para_chat_web(peer_id, mensagem):
    socketio.emit('chat_message', {'peer': peer_id, 'msg': mensagem, 'type': 'received'}, namespace='/')

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('setup_peer')
def handle_setup(data):
    peer_config['name'] = data.get('name')
    peer_config['namespace'] = data.get('namespace')
    socketio.start_background_task(monitor_peers)
    config_ready.set()

@socketio.on('terminal_input')
def handle_input(data):
    comando = data.get('data', '')
    if comando:
        terminal_input_queue.put(comando)