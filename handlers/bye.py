import asyncio
import json
import time

from peer_conec import *
from server import open_bye

async def bye_handler(writer: asyncio.StreamWriter, connected_peers, msg, name, namespace):
    """
    Trata o recebimento de uma mensagem BYE, removendo o peer e confirmando com BYE_OK.
    """

    peer_id = msg.get("peer_id")
    if peer_id is None:
        return False

    connected_peers.pop(peer_id, None)
    
    response = {
        "type": "BYE_OK",
        "msg_id": msg.get("msg_id"),
        "src": f"{name}@{namespace}",
        "dst": peer_id,
        "ttl": 1
    }
    
    try:
        payload = (json.dumps(response) + "\n").encode()
        writer.write(payload)
        await writer.drain()  # aguarda o envio completo dos dados

    except Exception as e:
        print(f"[BYE] Erro ao enviar BYE_OK para {peer_id}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

    print(f"[BYE] {peer_id} desconectado com sucesso.")
    return True


async def bye_ok_handler(writer: asyncio.StreamWriter, addr, connected_peers, msg):
    """
    Trata o recebimento da confirmação BYE_OK.
    """
    
    peer_id = msg.get("peer_id")
    
    if open_bye.get(peer_id):
        print(f"[BYE_OK] {peer_id} confirmou o fechamento da sessão (BYE).")
        open_bye.pop(peer_id, None)
        
        # ??????????????????????????????
        connected_peers[peer_id] = {
            "peer_id": peer_id,
            "ip": addr[0],
            "port": addr[1],
            "writer": writer,  # Substituiu o "sock" legado
            "last_ping": time.time()
        }

        #
        return True
    else:
        print(f"[BYE_OK] {peer_id} respondeu com BYE_OK sem solicitação prévia.")
        return False