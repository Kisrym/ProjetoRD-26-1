import threading

from webapp import (
    app,
    socketio,
    peer_config,
    config_ready,
    connected_peers
)

from peer_conec import keep_alive
from router import message_router
from server import servidor
from cli import cli_loop
from config import PEER_PORT, WEBAPP_PORT


def start_web_server():
    """Inicia o servidor web de configuração."""

    web_thread = threading.Thread(
        target=lambda: socketio.run(
            app,
            host="0.0.0.0",
            port=WEBAPP_PORT,
            debug=False,
            use_reloader=False,
            allow_unsafe_werkzeug=True
        ),
        daemon=True
    )

    web_thread.start()
    return web_thread

def start_peer_services(name, namespace):
    """Inicia servidor P2P, roteador e keep-alive."""

    server_thread = threading.Thread(
        target=servidor,
        args=(PEER_PORT,),
        daemon=True
    )

    router_thread = threading.Thread(
        target=message_router,
        args=(connected_peers, name, namespace),
        daemon=True
    )

    keepalive_thread = threading.Thread(
        target=keep_alive,
        args=(connected_peers, name, namespace),
        daemon=True
    )

    server_thread.start()
    router_thread.start()
    keepalive_thread.start()

    return server_thread, router_thread, keepalive_thread

def main():
    # Inicia interface web
    start_web_server()

    print("=> Acesse http://localhost:5000 no navegador para configurar o Nome e Namespace.")

    # Aguarda configuração do usuário
    config_ready.wait()

    name = peer_config["name"]
    namespace = peer_config["namespace"]

    peer_id = f"{name}@{namespace}"

    print(f"=> Peer configurado: {peer_id}")

    # Inicia serviços P2P
    start_peer_services(name, namespace)

    # Loop principal
    cli_loop(connected_peers, name, namespace)


if __name__ == "__main__":
    main()