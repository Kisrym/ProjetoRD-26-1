import asyncio
import time

from handlers.ping import *
from rendezvous_connection import *
from handlers.hello import *
from config import PEER_PORT

grups_online = {}

def add_grups(connected_peers):
    for peer_id, _ in connected_peers.items():
        if "@" not in peer_id: # ?
            continue

        name, ns = peer_id.split("@", 1)

        if name not in grups_online.get(ns, []):
            grups_online.setdefault(ns, []).append(name)


async def keep_alive(connected_peers, name, namespace):
    timer = 0
    while True:
        if time.time() - timer >= 30: # a cada 30 segundos, atualiza a lista de peers no servidor
            timer = time.time()
            
            peers, err = await discorver_handler()

            if err:
                print("[KEEP-ALIVE] Erro ao descobrir peers:", err)
                return 1
            
            await cadastrar_peers(peers, connected_peers, name, namespace)


        await send_ping_handler(connected_peers, name)
        
        add_grups(connected_peers)

        await asyncio.sleep(1)


async def peer_connection(connected_peers, name, namespace):
    """
    Controla o ciclo de vida do registro no servidor Rendezvous.
    """
    registered = False
    while True:
        if not registered:
            print("[CONEXÃO] Tentando registrar no servidor Rendezvous...")
            
            success = await register_handler(name, namespace, PEER_PORT)

            if success:
                registered = True
                print("[CONEXÃO] Registrado com sucesso!")
            else:
                print("[CONEXÃO] Falha ao registrar. Tentando novamente em 5 segundos...")
                await asyncio.sleep(5)
                continue

        if registered:
            erro = await keep_alive(connected_peers, name, namespace)
            if erro == 1:
                registered = False
                print("[CONEXÃO] Erro no keep-alive, tentando registrar novamente...")
                await asyncio.sleep(2)