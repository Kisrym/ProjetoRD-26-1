# webapp.py
import asyncio
import sys
from quart import Quart, render_template
import socketio
from core.peer_table import PeerTable

app = Quart(__name__)
app.config['SECRET_KEY'] = 'secret'

sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins="*")

asgi_app = socketio.ASGIApp(sio, app)

connected_peers = PeerTable()
peer_config = {}

# Objetos assíncronos globais
config_ready = asyncio.Event()
terminal_input_queue = asyncio.Queue()

_loop = None

class WebTerminalOut:
    def __init__(self, original_stream):
        self.original_stream = original_stream

    def write(self, message):
        self.original_stream.write(message)
        self.original_stream.flush()
        
        if message and message.strip() != '' and _loop is not None:
            async def send_output():
                await sio.emit('terminal_output', {'data': message}, namespace='/')
            
            _loop.call_soon_threadsafe(lambda: asyncio.create_task(send_output()))

    def flush(self):
        self.original_stream.flush()

def interceptar_terminal():
    sys.stdout = WebTerminalOut(sys.stdout)
    sys.stderr = WebTerminalOut(sys.stderr)
    print("[WEBAPP] Terminal integrado com sucesso.")

async def monitor_peers():
    """Tarefa assíncrona de monitoramento executada no loop principal."""
    last_state = None
    print("[WEBAPP] Monitor de peers iniciado.")
    while True:
        current_peers = []

        for peer_id, value in connected_peers.items():
            if value.get("connection_status") == "CONNECTED":
                current_peers.append(peer_id)

        namespace = peer_config.get('namespace', '')

        current_state = (current_peers, namespace)

        if current_state != last_state:
            await sio.emit('update_peers', {'peers' : current_peers, 'meu_namespace' : namespace}, namespace='/')
            last_state = current_state
        await asyncio.sleep(2)


async def enviar_para_chat_web(target_id, mensagem):
    """Corrotina assíncrona chamada pelos handlers do P2P."""
    await sio.emit(
        'chat_message', 
        {'target_id': target_id, 'msg': mensagem, 'type': 'received'}, 
        namespace='/'
    )


@app.route('/')
async def index():
    return await render_template('index.html')


@sio.on('setup_peer', namespace='/')
async def handle_setup(sid, data):
    peer_config['name'] = data.get('name')
    peer_config['namespace'] = data.get('namespace')
    
    # Inicia a task em background nativamente no asyncio
    asyncio.create_task(monitor_peers())
    
    # Libera o fluxo do main.py
    config_ready.set()


@sio.on('terminal_input', namespace='/')
async def handle_input(sid, data):
    comando = data.get('data', '')
    if comando:
        await terminal_input_queue.put(comando)