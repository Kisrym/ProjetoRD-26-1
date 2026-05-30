import threading

from server import servidor
from peer_conec import keep_alive
from router import message_router
from cli import cli_loop

connected_peers = {}

name = input("Digite seu nome: ")
namespace = input("Digite o namespace: ")
peer_id = f"{name}@{namespace}"


server_thread = threading.Thread(
    target=servidor,
    args=(connected_peers, name, namespace),
    daemon=True
)

server_thread.start()

router_thread = threading.Thread(
    target=message_router,
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