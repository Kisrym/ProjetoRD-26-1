import threading

from peer_server import servidor
from peer_conec import keep_alive
from cli import cli_loop

connected_peers = {}

server_thread = threading.Thread(
    target=servidor,
    args=(connected_peers,),
    daemon=True
)

keepalive_thread = threading.Thread(
    target=keep_alive,
    args=(connected_peers,),
    daemon=True
)

server_thread.start()

keepalive_thread.start()

cli_loop(connected_peers)