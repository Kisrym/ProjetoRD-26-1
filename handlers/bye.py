import asyncio
import json
import time
import uuid

from core.connection import *
from interfaces.web.app import connected_peers

async def bye_handler(writer: asyncio.StreamWriter, bye_received: dict):
    peer_id = bye_received.get("src")
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
    dados = connected_peers.pop(peer_id, None)

    if dados:
        try:
            dados["writer"].close()
        except: pass
        

async def bye_ok_handler(msg_received: dict):
    print(f"[CONEXÃO] Confirmação de BYE_OK recebida de {msg_received.get("peer_id")}")