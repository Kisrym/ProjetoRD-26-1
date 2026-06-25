import asyncio
import json
import time
import uuid

from core.connection import *
from interfaces.web.app import connected_peers

async def bye_handler(writer: asyncio.StreamWriter, bye_received: dict):
    peer_id = bye_received.get("src")

    if not peer_id:
        print("[BYE] Não foi possível resgatar o peer_id da mensagem de BYE.")
        return
    
    response = {
        "type": "BYE_OK",
        "msg_id": bye_received.get("msg_id"),
        "src": bye_received.get("dst"),
        "dst": peer_id,
        "ttl": 1
    }

    try:
        writer.write((json.dumps(response) + "\n").encode())
        await writer.drain()

    except Exception: pass

    print(f"[CONEXÃO] Peer {peer_id} se despediu (BYE)")
    connected_peers.change_peer_connection_status(peer_id, "DISCONNECTED")
    dados = connected_peers.get(peer_id)

    if dados:
        try:
            dados["writer"].close()
        except: pass
        

async def bye_ok_handler(msg_received: dict):
    peer_id = msg_received.get("peer_id")
    print(f"[CONEXÃO] Confirmação de BYE_OK recebida de {peer_id}")

    if peer_id:
        connected_peers.change_peer_connection_status(peer_id, "DISCONNECTED")