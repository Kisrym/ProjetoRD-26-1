from webapp import terminal_input_queue
import json
import socket
import threading
import time
import uuid
from server import peer_listener

open_send = {}

def send(peer_id,sock,name,namespace,message):
    try:
        msg_id = str(uuid.uuid4())

        event = threading.Event()
        open_send[msg_id] = event

        threading.Thread(
            target=peer_listener,
            args=(sock, peer_id),
            daemon=True
        ).start()   
        msg = {
            "type": "SEND",
            "msg_id": msg_id,
            "src": f"{name}@{namespace}",
            "dst": peer_id,
            "payload": message,
            "require_ack": True,
            "ttl": 1
        }

        sock.sendall((json.dumps(msg) + "\n").encode())

        if event.wait(timeout=5):
            open_send.pop(msg_id, None)
            return sock

        open_send.pop(msg_id, None)
        print("Timeout esperando SEND_OK")
        sock.close()
        return None
    except Exception as e:
        open_send.pop(msg_id, None)
        print("Handshake error:", e)
        return None
    finally:
        open_send.pop(msg_id, None)

def cli_loop(connected_peers,name,namespace):
    print(f"Sistema P2P pronto. Logado como: {name}@{namespace}")
    print("Aguardando comandos...")
    while True:
        cmd = terminal_input_queue.get().strip()
        if not cmd:
            continue
        if cmd.startswith("@msg"):
            partes = cmd.split(" ", 2)
            if len(partes) == 3:
                peer_id = partes[1]
                texto = partes[2]
                
                if peer_id in connected_peers:
                    send(peer_id,connected_peers[peer_id]["sock"],name,namespace,texto)
            continue
        elif cmd.startswith("@group_msg"):
            partes = cmd.split(" ", 2)
            if len(partes) == 3:
                alvo_namespace = partes[1]
                texto = partes[2]
                
                # CHAME SUA FUNÇÃO DE MULTICAST/BROADCAST AQUI
                # Exemplo: broadcast_para_grupo(alvo_namespace, texto, connected_peers)
                print(f"[Log] Enviando para GRUPO {alvo_namespace}: {texto}")
            continue
        if cmd == "sair":
            print("Encerrando o sistema...")
            break

        

