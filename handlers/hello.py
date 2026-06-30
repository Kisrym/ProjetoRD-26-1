# ==============================================================================
# UNIVERSIDADE DE BRASÍLIA (UnB) - DEPARTAMENTO DE CIÊNCIA DA COMPUTAÇÃO
# REDES DE COMPUTADORES - SEMESTRE: 2026/1 - PROF. MARCOS FAGUNDES CAETANO
# PROJETO FINAL: Chat Peer-to-Peer (P2P) | GRUPO 8
# 
# INTEGRANTES:
#   - Kaio Santos Araújo       (Matrícula: 242009972)
#   - Caio Dias Fleury         (Matrícula: 242009909)
#   - João Paulo Silva Mendes  (Matrícula: 242026187)
# 
# ARQUIVO: handlers/hello.py
# ==============================================================================

import asyncio
import json
import time
import logging

from core.server import open_hello, peer_listener
from interfaces.web.app import connected_peers

log = logging.getLogger("HELLO")

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

        log.info(f"(HANDSHAKE) Enviado HELLO para {peer_id} em {peer_ip}:{peer_port}")

        try:
            await asyncio.wait_for(event.wait(), timeout=2.0) # espera 2 segundos pro evento disparar
            
            log.info(f"(HANDSHAKE) Conexão com {peer_id} bem sucedida")
            return writer
            
        except asyncio.TimeoutError:
            log.error(f"(HANDSHAKE) Timeout esperando HELLO_OK de {peer_id}")
            writer.close()
            await writer.wait_closed()

            return None

    except Exception as e:
        log.error(f"(HANDSHAKE) Erro no handshake com {peer_id}: {e}")
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
            log.info(f"(CONEXÃO) Tentando conexão com {peer_id} em {peer['ip']}:{peer['port']}...")
            
            writer = await hand_shake(peer["ip"], peer["port"], peer_id, name, namespace)
            
            if writer:
                connected_peers.change_peer_connection_status(peer_id, "CONNECTED")
                connected_peers.connect_peer(peer_id, writer, time.time(), "outbound")
                                                    # writer armazena o conn


async def hello_handler(writer: asyncio.StreamWriter, addr, msg, name, namespace):
    """
    Trata o recebimento de uma mensagem HELLO vinda de outro peer.
    """
    peer_id = msg.get("peer_id")
    if peer_id is None:
        return False

    if peer_id not in connected_peers.get_all_peers():
        log.info("[HELLO] Registrando novo peer de conexão inbound...")
        peer_name = peer_id.split("@")[0]
        peer_namespace = peer_id.split("@")[1]

        peer = {
            "peer_id" : peer_id,
            "ip" : addr[0],
            "port" : addr[1],
            "name" : peer_name,
            "namespace" : peer_namespace,
            "ttl" : 3600,
            "expires_in" : 3600,
            "connection_status" : "TRYING_CONNECTION"
        }

        connected_peers.registrar_peer(peer)

    connected_peers.change_peer_connection_status(peer_id, "CONNECTED")
    connected_peers.connect_peer(peer_id, writer, time.time(), "inbound")
    
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

        log.info(f"{peer_id} conectado e confirmado.")
        return True
    
    except Exception as e:
        log.error(f"Erro ao responder HELLO_OK para {peer_id}: {e}")
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
        log.warning(f"(HELLO_OK) Recebido HELLO_OK de {peer_id} sem solicitação prévia.")
        return False