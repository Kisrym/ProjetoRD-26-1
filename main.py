import webapp # Importamos para poder injetar a referência do dicionário
from webapp import app, socketio, peer_config, config_ready
from peer_conec import keep_alive
from router import message_router
from server import servidor
from cli import cli_loop
import threading

# Dicionário de estado compartilhado
connected_peers = {}

webapp.connected_peers = connected_peers

web_thread = threading.Thread(
    target=lambda: socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True
    ),
    daemon=True
)
web_thread.start()

print("=> Acesse http://localhost:5000 no navegador para configurar o Nome e Namespace.")

config_ready.wait()

name = peer_config['name']
namespace = peer_config['namespace']
peer_id = f"{name}@{namespace}"
print(f"=> Iniciando sistema como {peer_id}")

server_thread = threading.Thread(
    target=servidor,
    args=(),
    daemon=True
)
server_thread.start()

router_thread = threading.Thread(
    target=message_router,
    args=(connected_peers, name, namespace),
    daemon=True
)
router_thread.start()

keepalive_thread = threading.Thread(
    target=keep_alive,
    args=(connected_peers, name, namespace),
    daemon=True
)
keepalive_thread.start()

cli_loop(connected_peers, name, namespace)