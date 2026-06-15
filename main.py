import asyncio
from hypercorn.config import Config
import hypercorn.asyncio

import webapp
from peer_conec import *
from router import *
from server import *
from cli import *
from config import *


async def start_web_server():
    """Inicia o servidor web Quart/Socket.IO de forma totalmente assíncrona."""
    config = Config()
    config.bind = [f"0.0.0.0:{WEBAPP_PORT}"]

    config.shutdown_timeout = 0.0
    
    async def run_server():
        try:
            await hypercorn.asyncio.serve(webapp.asgi_app, config)

        except Exception as e:
            print(f"[ERRO WEBAPP] Servidor web falhou: {e}")
        
    return asyncio.create_task(run_server())


async def main():
    webapp._loop = asyncio.get_running_loop()

    await start_web_server()

    print(f"=> Acesse http://localhost:{WEBAPP_PORT} no navegador para configurar o Nome e Namespace.")

    await webapp.config_ready.wait()

    name = webapp.peer_config["name"]
    namespace = webapp.peer_config["namespace"]
    peer_id = f"{name}@{namespace}"

    print(f"=> Peer configurado com sucesso: {peer_id}")

    # dispara todas as tarefas 
    try:
        await asyncio.gather(
            servidor(PEER_PORT),
            message_router(webapp.connected_peers, name, namespace),
            peer_connection(webapp.connected_peers, name, namespace),
            cli_loop(webapp.connected_peers, name, namespace)
        )

    except asyncio.CancelledError:
        print("\n=> Encerrando serviços do ecossistema...")


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\n=> Programa interrompido pelo usuário no terminal.")