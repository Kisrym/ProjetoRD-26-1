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
# ARQUIVO: interfaces/cli.py
# ==============================================================================

import asyncio
from datetime import datetime, timezone
import json
import uuid
import sys
import logging

from config import ASYNCIO_EVENT_TIMEOUT
from interfaces.web.app import terminal_input_queue, connected_peers, peer_config
from core.connection import grups_online, close_all_connections
from core.server import open_send
from core.connection import try_to_reconnect

log = logging.getLogger("CLI")

async def pub(name, namespace, dst, message):
    """
    Envia uma mensagem de broadcast (PUB) para todos os peers de um grupo específico.
    """
    try:
        log.info(grups_online)
            
        msg_id = str(uuid.uuid4())
        msg = {
            "type": "PUB",
            "msg_id": msg_id,
            "src": f"{name}@{namespace}",
            "dst": dst,
            "payload": message,
            "require_ack": False,
            "ttl": 1
        }
        
        tasks = []
        lista_peers = []

        if dst == '*':
            for sublista in grups_online.values():
                for peer_id in sublista:
                    if peer_id not in lista_peers:
                        lista_peers.append(peer_id)
        else:
            lista_peers = grups_online.get(dst, [])

        for peer_id in lista_peers:
            if peer_id in connected_peers.get_all_peers():
                peer = connected_peers.get(peer_id)
                if not peer or peer.get("connection_status") != "CONNECTED":
                    continue

                writer = peer["writer"]
                
                async def send_payload(w, payload):
                    try:
                        w.write(payload)
                        await w.drain()

                    except Exception:
                        pass

                payload_bytes = (json.dumps(msg) + "\n").encode()
                tasks.append(send_payload(writer, payload_bytes)) # armazena a função para mandar a mensagem para todos os peers conectados de uma vez
        
        if tasks:
            await asyncio.gather(*tasks) # bota em prática a função
            
    except Exception as e:
        log.error("Erro ao enviar mensagem PUB:", e)


async def send(peer_id, writer: asyncio.StreamWriter, name, namespace, message):
    """
    Envia uma mensagem direta (SEND) para um peer e aguarda o ACK assíncronamente.
    """
    msg_id = str(uuid.uuid4())
    try:
        event = asyncio.Event()
        open_send[msg_id] = event

        msg = {
            "type": "SEND",
            "msg_id": msg_id,
            "src": f"{name}@{namespace}",
            "dst": peer_id,
            "payload": message,
            "require_ack": True,
            "ttl": 1
        }

        writer.write((json.dumps(msg) + "\n").encode())
        await writer.drain()

        try:
            await asyncio.wait_for(event.wait(), timeout=ASYNCIO_EVENT_TIMEOUT)
            return True
            
        except asyncio.TimeoutError:
            log.warning(f"Timeout esperando ACK da mensagem {msg_id}")
            return False
            
    except Exception as e:
        log.error(f"Erro no envio para {peer_id}: {e}")
        return False
    
    finally:
        open_send.pop(msg_id, None)


def read_cmd(queue, loop):
    while True:
        sys.stdout.write(">> ")
        sys.stdout.flush()

        line = sys.stdin.readline()
        if not line: break

        loop.call_soon_threadsafe(queue.put_nowait, line)

async def cli_loop(name, namespace):
    log.info(f"Sistema P2P pronto. Logado como: {name}@{namespace}")
    
    if not peer_config.get('name'): #### se ele entrou em modo cmd, ou seja, n configurou
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, read_cmd, terminal_input_queue, loop)

    while True:
        cmd = await terminal_input_queue.get()
        cmd = cmd.strip()

        if not cmd:
            continue
            
        if cmd.startswith("@msg") or cmd.startswith("/msg"):
            partes = cmd.split(" ", 2)
            if len(partes) == 3:
                peer_id = partes[1]
                texto = partes[2]
                
                if peer_id in connected_peers.get_all_peers():
                    peer = connected_peers.get(peer_id)
                    if not peer:
                        log.error("(MSG) Não foi possível resgatar o peer especificado.")
                        continue

                    if peer.get("connection_status") != "CONNECTED":
                        log.error("(MSG) Não é possível enviar mensagem para um peer não conectado.")
                        continue

                    await send(peer_id, peer["writer"], name, namespace, texto)

            continue
            
        elif cmd.startswith("@group_msg") or cmd.startswith("/pub"):
            partes = cmd.split(" ", 2)

            #! AQUI TEM QUE ANALISAR SE O PUB É NO ESCOPO DE GRUPO OU BROADCAST

            if len(partes) == 3:
                dst_namespace = partes[1]
                texto = partes[2]
                await pub(name, namespace, dst_namespace, texto)
                log.info(f"(PUB) Enviando para GRUPO {dst_namespace}: {texto}")
                
            continue
            
        elif cmd.startswith("/peers"):
            log.info("(PEERS) Peers conhecidos:")
            for pid in connected_peers.get_all_peers():
                peer = connected_peers.get(pid)
                if not peer: continue

                log.info(f" - {pid} | {peer.get("ip")}:{peer.get("port")} | Status: {peer.get("connection_status")}")

            continue
            
        elif cmd.startswith("/conn"):
            log.info("Conexões Inbound")
            for item in connected_peers.get_specific_connections('inbound'):
                log.info(item)

            log.info("Conexões outbound")
            for item in connected_peers.get_specific_connections('outbound'):
                log.info(item)

            continue
            
        elif cmd.startswith("/reconnect"):
            tasks = []
            for peer_id in connected_peers.get_all_peers():
                peer = connected_peers.get(peer_id)
                if not peer: continue

                connected_peers.change_peer_connection_status(peer_id, "TRYING_CONNECTION") # FORÇA TODOS A RECONEXÃO
                tasks.append(try_to_reconnect(peer_id, peer.get("ip"), peer.get("port"), name, namespace))

            if tasks:
                await asyncio.gather(*tasks)

            log.info("(RECONNECT) Forçando reconexão com todos os peers...")
            
            continue
        
        elif cmd.startswith("/log"):
            items = cmd.split()
            if len(items) != 2:
                log.error("(LOG) Formato de parâmetros inválido. Utilize DEBUG|INFO|WARNING|ERROR")
            else:
                nivel = items[1].upper()
                nivel_atual = logging.getLogger().level
                if nivel in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                    if nivel != logging.getLevelName(nivel_atual):
                        logging.getLogger().setLevel(getattr(logging, nivel))
                        log.info(f"(LOG) Nível de log alterado para {nivel}")
                    else:
                        log.warning(f"Nível de log já está em {nivel}")
                else:
                    log.error("(LOG) Nível inválido")

        elif cmd.startswith("/rtt"):
            continue
            
        elif cmd.startswith("/quit"):
            log.info("(QUIT) Encerrando o sistema...")

            try:
                await close_all_connections(name, namespace)
                
                current_task = asyncio.current_task()
                all_tasks = [t for t in asyncio.all_tasks() if t is not current_task]
                
                if all_tasks:
                    log.info(f"(QUIT) Cancelando {len(all_tasks)} tarefas secundárias pendentes...")
                    for task in all_tasks:
                        task.cancel()
                    
                    await asyncio.gather(*all_tasks, return_exceptions=True)
                    
            except Exception as e:
                log.error(f"Erro durante o encerramento das conexões: {e}")
            finally:
                log.info("(QUIT) Processo finalizado com sucesso.")
                sys.exit(0)
        
        else:
            log.error("Comando inexistente")