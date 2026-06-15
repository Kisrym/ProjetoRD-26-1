import asyncio
from datetime import datetime, timezone
import json
import uuid
import sys

from webapp import terminal_input_queue
from peer_conec import grups_online
from server import open_send

async def pub(connected_peers, name, namespace, dst, message):
    """
    Envia uma mensagem de broadcast (PUB) para todos os peers de um grupo específico.
    """
    try:
        if dst not in grups_online:
            print(f"[Log] Nenhum peer registrado no grupo {dst}")
            return
            
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
        for peer_id in grups_online.get(dst, []):
            if peer_id in connected_peers:
                writer = connected_peers[peer_id]["writer"]
                
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


async def cli_loop(connected_peers, name, namespace):
    print(f"Sistema P2P pronto. Logado como: {name}@{namespace}")
    
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
                
                if peer_id in connected_peers:
                    await send(peer_id, connected_peers[peer_id]["writer"], name, namespace, texto)

            continue
            
        elif cmd.startswith("@group_msg") or cmd.startswith("/pub"):
            partes = cmd.split(" ", 2)

            #! AQUI TEM QUE ANALISAR SE O PUB É NO ESCOPO DE GRUPO OU BROADCAST

            if len(partes) == 3:
                dst_namespace = partes[1]
                texto = partes[2]
                await pub(connected_peers, name, namespace, dst_namespace, texto)
                print(f"[Log] Enviando para GRUPO {dst_namespace}: {texto}")
                
            continue
            
        elif cmd.startswith("/peers"):
            print("Peers conectados:")
            for pid in connected_peers:
                print(f" - {pid}")

            continue
            
        elif cmd.startswith("/conn"): # REFAZER, N É ISSO QUE ESSA FUNÇÃO DEVERIA FAZER. VIDE https://github.com/mfcaetano/pyp2p-rdv/blob/main/src/docs/RC202502%20-%20PyP2p%20-%20Especificacao%20Trabalho.md#interface-de-usu%C3%A1rio-cli
            for peer_id in connected_peers:
                print(f"Conectado a {peer_id} - Último ping: {connected_peers[peer_id].get('last_ping', 'N/A')}")
            continue
            
        elif cmd.startswith("/reconnect"):
            for peer_id in connected_peers:
                connected_peers[peer_id]["last_ping"] = 0
            print("Forçando reconexão com todos os peers (zerando timestamps de keep-alive)...")
            
            continue
            
        elif cmd.startswith("/rtt") or cmd.startswith("/log"):
            continue
            
        if cmd.startswith("/quit"):
            print("Encerrando o sistema...")

            # FAZER A LIMPA (tirar as tasks do asyncio, loops, encerrar bgl da memoria, etc)
            # ENVIAR BYE para todos os peers conectados
            # UNREGISTER no servidor rendezvous
            # ai sim, sair com exit(0)

            sys.exit(0)
            break