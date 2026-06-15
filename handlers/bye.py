import asyncio
import json
import time
import uuid

from peer_conec import *
from server import open_bye

async def send_bye(peer_id, writer: asyncio.StreamWriter, connected_peers, name, namespace):
    payload = {
        "type": "BYE",
        "msg_id": str(uuid.uuid4()),
        "src": f"{name}@{namespace}", # Passar dinâmico
        "dst": peer_id,
        "reason": "Encerrando sessão",
        "ttl": 1
    }

    try:
        writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()

        await asyncio.sleep(0.5) # esperar o envio do pacote

    except Exception:
        pass

    finally:
        try:
            writer.close()
            await writer.wait_closed()

        except Exception: pass

        connected_peers.pop(peer_id, None)

async def bye_handler(writer: asyncio.StreamWriter, bye_received: dict, connected_peers: dict):
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