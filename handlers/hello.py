import asyncio
import json
import time

from core.server import open_hello, peer_listener
from interfaces.web.app import connected_peers

async def hand_shake(peer_ip, peer_port, peer_id, name, namespace):
    """
    Abre uma conexão de saída com um peer e realiza o handshake HELLO/HELLO_OK.
    """
    try:
        reader, writer = await asyncio.open_connection(peer_ip, peer_port)

        event = asyncio.Event() # aguarda confirmação do HELLO_OK do peer
        open_hello[peer_id] = event

        addr = (peer_ip, peer_port)
        asyncio.create_task(peer_listener(reader, writer, addr))

        paylaod = {
            "type": "HELLO",
            "peer_id": f"{name}@{namespace}",
            "version": "1.0",
            "features": ["ack", "metrics"],
            "ttl": 1
        }
        
        writer.write((json.dumps(paylaod) + "\n").encode())
        await writer.drain()

        print(f"[HANDSHAKE] Enviado HELLO para {peer_id} em {peer_ip}:{peer_port}")

        try:
            await asyncio.wait_for(event.wait(), timeout=2.0) # espera 2 segundos pro evento disparar
            
            print(f"[HANDSHAKE] Conexão com {peer_id} bem sucedida")
            return writer
            
        except asyncio.TimeoutError:
            print(f"[HANDSHAKE] Timeout esperando HELLO_OK de {peer_id}")
            writer.close()
            await writer.wait_closed()

            return None

    except Exception as e:
        print(f"[HANDSHAKE] Erro no handshake com {peer_id}: {e}")
        return None
    
    finally:
        open_hello.pop(peer_id, None)


async def cadastrar_peers(peers, name, namespace):
    """
    Varre os peers descobertos pelo Rendezvous e inicia handshake com novos conhecidos.
    """
    for peer in peers:
        peer_id = f'{peer["name"]}@{peer["namespace"]}'
        
        # ignora a si mesmo
        if peer_id == f"{name}@{namespace}":
            continue

        connected_peers.registrar_peer(peer)

        p = connected_peers.get(peer_id)
        if p and (p.get("connection_status") == "DISCONNECTED" or p.get("connection_status") == "TRYING_CONNECTION"):
            print(f"[CONEXÃO] Tentando conexão com {peer_id} em {peer['ip']}:{peer['port']}...")
            
            writer = await hand_shake(peer["ip"], peer["port"], peer_id, name, namespace)
            
            if writer:
                connected_peers.change_peer_connection_status(peer_id, "CONNECTED")
                connected_peers.connect_peer(peer_id, writer, time.time(), "inbound")
                                                    # writer armazena o conn


async def hello_handler(writer: asyncio.StreamWriter, addr, msg, name, namespace):
    """
    Trata o recebimento de uma mensagem HELLO vinda de outro peer.
    """
    peer_id = msg.get("peer_id")
    if peer_id is None:
        return False

    connected_peers.change_peer_connection_status(peer_id, "CONNECTED")
    connected_peers.connect_peer(peer_id, writer, time.time(), "outbound")
    
    response = {
        "type": "HELLO_OK",
        "peer_id": f"{name}@{namespace}",
        "version": "1.0",
        "features": ["ack", "metrics"],
        "ttl": 1
    }
    
    try:
        writer.write((json.dumps(response) + "\n").encode())
        await writer.drain()

        print(f"[HELLO] {peer_id} conectado e confirmado.")
        return True
    
    except Exception as e:
        print(f"[HELLO] Erro ao responder HELLO_OK para {peer_id}: {e}")
        return False


async def hello_ok_handler(msg):
    """
    Trata o recebimento de um HELLO_OK liberando a trava do handshake pendente.
    """
    peer_id = msg.get("peer_id")
    event = open_hello.get(peer_id)

    if event:
        event.set() # ativa o evento iniciado lá em hand_shake
        return True
    
    else:        
        print(f"[HELLO_OK] Recebido HELLO_OK de {peer_id} sem solicitação prévia.")
        return False