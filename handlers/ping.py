import asyncio
from datetime import datetime, timezone
import json
import time
import uuid

from server import open_ping 

async def send_ping(writer: asyncio.StreamWriter, peer_id):
    """
    Monta a mensagem de PING, envia e aguarda de forma assíncrona o PONG correspondente.
    """
    msg_id = str(uuid.uuid4())
    try:
        event = asyncio.Event()
        open_ping[msg_id] = event

        ping_msg = {
            "type": "PING",
            "msg_id": msg_id,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "ttl": 1
        }
        
        #tempo_envio = time.time()
        
        writer.write((json.dumps(ping_msg) + "\n").encode())
        await writer.drain()

        await asyncio.wait_for(event.wait(), timeout=2.0)
        
        #rtt = (time.time() - tempo_envio) * 1000
        
        # Guarda o histórico de RTT no dicionário do peer para o comando /rtt da CLI
        #if peer_id in connected_peers:
        #    if "rtts" not in connected_peers[peer_id]:
        #        connected_peers[peer_id]["rtts"] = []
        #    connected_peers[peer_id]["rtts"].append(rtt)
        #    if len(connected_peers[peer_id]["rtts"]) > 5:
        #        connected_peers[peer_id]["rtts"].pop(0)

        print(f"[KEEP_ALIVE] PONG de {peer_id} recebido") # | RTT = {rtt:.2f} ms")
            
        return True
        
    except asyncio.TimeoutError:
        print(f"[PING] Timeout de 2.0s esperando PONG do peer {peer_id} (msg_id: {msg_id})")
        return False
    
    except Exception as e:
        print(f"[PING] Erro ao tentar pingar {peer_id}: {e}")
        return False
    
    finally:
        open_ping.pop(msg_id, None)


async def ping_handler(writer: asyncio.StreamWriter, addr, msg):
    """
    Trata o recebimento de uma mensagem PING vinda de outro peer
    """
    response = {
        "type": "PONG",
        "msg_id": msg.get("msg_id"),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "ttl": 1
    }

    try:
        writer.write((json.dumps(response) + "\n").encode())
        await writer.drain()
        print(f"[PING] Respondido PING vindo de {addr}")
        return True
    
    except Exception as e:
        print(f"[PING] Erro ao responder PING para {addr}: {e}")
        return False


async def pong_handler(msg):
    """
    Trata o recebimento de um PONG enviado por um peer remoto.
    """
    msg_id = msg.get("msg_id")
    event = open_ping.get(msg_id)
    
    if event:
        event.set() # acorda o event.wait() lá do send_ping
        return True
    else:
        print(f"[PONG] Recebido PONG do msg_id {msg_id} sem solicitação prévia ou expirado.")
        return False