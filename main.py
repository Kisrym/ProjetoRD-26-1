import asyncio
from hypercorn.config import Config
import hypercorn.asyncio

import interfaces.web.app as app
from core.connection import *
from core.router import *
from core.server import *
from interfaces.cli import *
from config import *
from core.rendezvous import discover_loop
from config import *
import argparse

__my_name = None
__my_namespace = None
log = logging.getLogger("MAIN")

async def start_web_server():
    """Inicia o servidor web Quart/Socket.IO de forma totalmente assíncrona."""
    config = Config()
    config.bind = [f"0.0.0.0:{WEBAPP_PORT}"]

    config.shutdown_timeout = 0.0
    
    async def run_server():
        try:
            await hypercorn.asyncio.serve(app.asgi_app, config)

        except Exception as e:
            log.error(f"(ERRO WEBAPP) Servidor web falhou: {e}")
        
    return asyncio.create_task(run_server())


async def main(cli_only: bool = False):
    app._loop = asyncio.get_running_loop()

    global __my_name, __my_namespace

    if not cli_only:
        app.interceptar_terminal()
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",
            datefmt="%H:%M:%S"
        )
        await start_web_server()
        print(f"=> Acesse http://localhost:{WEBAPP_PORT} no navegador para configurar o Nome e Namespace.")
        await app.config_ready.wait()
        __my_name = app.peer_config["name"]
        __my_namespace = app.peer_config["namespace"]

    else:
        print("[MODO TERMINAL] Iniciando sem interface Web.")
        __my_name = PEER_NAME
        __my_namespace = PEER_NAMESPACE

    peer_id = f"{__my_name}@{__my_namespace}"

    success, data = await register_handler(__my_name, __my_namespace, PEER_PORT)
    if not success:
        log.error("(REGISTRO) Erro ao registrar-se ao servidor rendezvous")
        exit(-1)

    log.info(f"=> Peer configurado com sucesso: {peer_id}")

    # dispara todas as tarefas 
    try:
        await asyncio.gather(
            register_loop(__my_name, __my_namespace, PEER_PORT), # atualiza o usuário no servidor quando o ttl acaba
            servidor(PEER_PORT),
            discover_loop(__my_name, __my_namespace),
            message_router(__my_name, __my_namespace),
            keep_alive(__my_name, __my_namespace),
            cli_loop(__my_name, __my_namespace)
        )

    except asyncio.CancelledError:
        print("\n=> Encerrando serviços do ecossistema...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", action="store_true", help="Inicia o programa rodando apenas no terminal, sem o WebApp")

    args = parser.parse_args()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main(cli_only=args.cli))

    except KeyboardInterrupt:
        print("\n=> Programa interrompido pelo usuário no terminal...")

        try:
            if __my_name and __my_namespace:
                loop.run_until_complete(close_all_connections(__my_name, __my_namespace))
        
        except Exception as e:
            log.error("(GERAL) Falha ao encerrar:", e)

    finally:
        loop.close()
        log.info("[Processo encerrado]")