from peer_conec import keep_alive
from router import message_router
from server import servidor
from cli import cli_loop
import threading

connected_peers = {}

name = input("Digite seu nome: ")
namespace = input("Digite o namespace: ")
peer_id = f"{name}@{namespace}"


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

cli_loop(connected_peers,name,namespace)