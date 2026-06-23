import asyncio
from hypercorn.config import Config
import hypercorn.asyncio

import webapp
from peer_conec import *
from router import *
from server import *
from cli import *
from config import *
from rendezvous_connection import discover_loop
from config import *
import argparse

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


async def main(cli_only: bool = False):
    webapp._loop = asyncio.get_running_loop()

    if not cli_only:
        webapp.interceptar_terminal()
        await start_web_server()
        print(f"=> Acesse http://localhost:{WEBAPP_PORT} no navegador para configurar o Nome e Namespace.")
        await webapp.config_ready.wait()
        name = webapp.peer_config["name"]
        namespace = webapp.peer_config["namespace"]

    else:
        print("[MODO TERMINAL] Iniciando sem interface Web.")
        name = PEER_NAME
        namespace = PEER_NAMESPACE

    peer_id = f"{name}@{namespace}"

    success = await register_handler(name, namespace, PEER_PORT)
    if not success:
        print("[REGISTRO] Erro ao registrar-se ao servidor rendezvous")
        exit(-1)

    print(f"=> Peer configurado com sucesso: {peer_id}")

    # dispara todas as tarefas 
    try:
        await asyncio.gather(
            servidor(PEER_PORT),
            discover_loop(name, namespace),
            message_router(name, namespace),
            keep_alive(name, namespace),
            cli_loop(name, namespace)
        )

    except asyncio.CancelledError:
        print("\n=> Encerrando serviços do ecossistema...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", action="store_true", help="Inicia o programa rodando apenas no terminal, sem o WebApp")

    args = parser.parse_args()

    try:
        asyncio.run(main(cli_only=args.cli))

    except KeyboardInterrupt:
        print("\n=> Programa interrompido pelo usuário no terminal.")