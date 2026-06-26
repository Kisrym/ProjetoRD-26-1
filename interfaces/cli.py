import asyncio
from datetime import datetime, timezone
import json
import uuid
import sys

from interfaces.web.app import terminal_input_queue, connected_peers, peer_config
from core.connection import grups_online, close_all_connections
from core.server import open_send
from core.connection import try_to_reconnect

async def pub(name, namespace, dst, message):
    """
    Envia uma mensagem de broadcast (PUB) para todos os peers de um grupo específico.
    """
    try:
        print(grups_online)
            
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
        print("Erro ao enviar mensagem PUB:", e)


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
            await asyncio.wait_for(event.wait(), timeout=5.0)
            return True
            
        except asyncio.TimeoutError:
            print(f"[CLI] Timeout esperando ACK da mensagem {msg_id}")
            return False
            
    except Exception as e:
        print(f"[CLI] Erro no envio para {peer_id}: {e}")
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
    print(f"Sistema P2P pronto. Logado como: {name}@{namespace}")
    
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
                        print("[CLI] Não foi possível resgatar o peer especificado.")
                        continue

                    if peer.get("connection_status") != "CONNECTED":
                        print("[CLI] Não é possível enviar mensagem para um peer não conectado.")
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
                print(f"[CLI] Enviando para GRUPO {dst_namespace}: {texto}")
                
            continue
            
        elif cmd.startswith("/peers"):
            print("[CLI] Peers conhecidos:")
            for pid in connected_peers.get_all_peers():
                peer = connected_peers.get(pid)
                if not peer: continue

                print(f" - {pid} | {peer.get("ip")}:{peer.get("port")} | Status: {peer.get("connection_status")}")

            continue
            
        elif cmd.startswith("/conn"):
            print("Conexões Inbound")
            for item in connected_peers.get_specific_connections('inbound'):
                print(item)

            print("\nConexões outbound")
            for item in connected_peers.get_specific_connections('outbound'):
                print(item)

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

            print("[CLI] Forçando reconexão com todos os peers...")
            
            continue
            
        elif cmd.startswith("/rtt") or cmd.startswith("/log"):
            continue
            
        if cmd.startswith("/quit"):
            print("Encerrando o sistema...")

            try:
                await close_all_connections(name, namespace)
                
                current_task = asyncio.current_task()
                all_tasks = [t for t in asyncio.all_tasks() if t is not current_task]
                
                if all_tasks:
                    print(f"[QUIT] Cancelando {len(all_tasks)} tarefas secundárias pendentes...")
                    for task in all_tasks:
                        task.cancel()
                    
                    await asyncio.gather(*all_tasks, return_exceptions=True)
                    
            except Exception as e:
                print(f"Erro durante o encerramento das conexões: {e}")
            finally:
                print("[QUIT] Processo finalizado com sucesso.")
                sys.exit(0)