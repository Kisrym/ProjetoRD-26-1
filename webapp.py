# webapp.py
import asyncio
import sys
from quart import Quart, render_template
import socketio

app = Quart(__name__)
app.config['SECRET_KEY'] = 'secret'

sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins="*")

asgi_app = socketio.ASGIApp(sio, app)

connected_peers = {}
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


sys.stdout = WebTerminalOut(sys.stdout)
sys.stderr = WebTerminalOut(sys.stderr)


async def monitor_peers():
    """Tarefa assíncrona de monitoramento executada no loop principal."""
    last_peers = []
    print("[WEBAPP] Monitor de peers iniciado.")
    while True:
        current_peers = list(connected_peers.keys())
        if current_peers != last_peers:
            await sio.emit('update_peers', current_peers, namespace='/')
            last_peers = current_peers.copy()
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