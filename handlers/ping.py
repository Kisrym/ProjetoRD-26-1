import asyncio
from datetime import datetime, timezone
import json
import time
import uuid

from server import open_ping

async def send_ping_handler(connected_peers, current_peer_id):
    """
    Varre os peers conectados e envia um PING para aqueles que estão há 
    mais de 60 segundos sem atualizar o 'last_ping'.
    """

    for peer_id in list(connected_peers.keys()):
        peer_data = connected_peers.get(peer_id)
        if not peer_data:
            continue

        if time.time() - peer_data["last_ping"] >= 60:
            print(f"[PING-CHECK] {peer_id} está ocioso. Enviando PING...")
            writer = peer_data["writer"]

            success = await send_ping(writer, peer_id)
            
            if success:
                if peer_id in connected_peers:
                    connected_peers[peer_id]["last_ping"] = time.time()

            else:
                print(f"[PING-CHECK] Peer {peer_id} falhou no ping. Removendo...")
                try:
                    writer.close()
                    await writer.wait_closed()

                except Exception:
                    pass

                connected_peers.pop(peer_id, None)


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
        
        writer.write((json.dumps(ping_msg) + "\n").encode())
        await writer.drain()

        try:
            await asyncio.wait_for(event.wait(), timeout=2.0) # aguarda 2 segundos ate o evento completar
            return True
            
        except asyncio.TimeoutError:
            print(f"[PING] Timeout de 2s esperando PONG para a mensagem {msg_id}")
            return False
    
    except Exception as e:
        print(f"[PING] Erro ao tentar pingar {peer_id}: {e}")
        return False
    
    finally:
        open_ping.pop(msg_id, None)


async def ping_handler(writer: asyncio.StreamWriter, addr, msg):
    """
    Trata o recebimento de uma mensagem PING vinda de outro peer, 
    respondendo imediatamente com um PONG.
    """
    response = {
        "type": "PONG",
        "msg_id": msg.get("msg_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
    Trata o recebimento de um PONG. Como apenas libera uma trava em memória,
    esta função pode continuar síncrona.
    """
    msg_id = msg.get("msg_id")
    event = open_ping.get(msg_id)
    
    if event:
        event.set() # ativa o evento assincrono vindo do send_ping
        return True
    
    else:
        print(f"[PONG] Recebido PONG do msg_id {msg_id} sem solicitação prévia (ou expirado).")
        return False