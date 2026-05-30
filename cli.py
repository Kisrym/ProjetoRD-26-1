from webapp import terminal_input_queue
from peer_conec import grups_online
import json
import socket
import threading
import time
import uuid
from server import peer_listener

open_send = {}

def pub (connected_peers,name,namespace,dst,message):
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
        for peer_id in grups_online.get(dst, []):
            if peer_id in connected_peers:
                sock = connected_peers[peer_id]["sock"]
                sock.sendall((json.dumps(msg) + "\n").encode())
    except Exception as e:
        print("Erro ao enviar mensagem PUB:", e)

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
                pub(connected_peers,name,namespace,alvo_namespace,texto)
                print(f"[Log] Enviando para GRUPO {alvo_namespace}: {texto}")
            continue
        elif cmd.startswith("/peers"):
            print("Peers conectados:")
            for pid in connected_peers:
                print(f" - {pid}")  
            continue
        elif cmd.startswith("/msg"):
            partes = cmd.split(" ", 2)
            if len(partes) == 3:
                peer_id = partes[1]
                texto = partes[2]
                
                if peer_id in connected_peers:
                    send(peer_id,connected_peers[peer_id]["sock"],name,namespace,texto)
            continue
        elif cmd.startswith("/pub"):
            partes = cmd.split(" ", 2)
            if len(partes) == 3:
                dst = partes[1]
                texto = partes[2]
                pub(connected_peers,name,namespace,dst,texto)
        elif cmd.startswith("/conn"):
            for peer_id in connected_peers:
                print(f"Conectado a {peer_id} - Último ping: {connected_peers[peer_id].get('last_ping', 'N/A')} ms")
            continue
        elif cmd.startswith("/rtt"):
            print("Calculando RTT para peers conectados...")
            continue
        elif cmd.startswith("/reconnect"):
            for peer in connected_peers:
                peer["last_ping"] = 0
            print("Forçando reconexão com todos os peers...")
        elif cmd.startswith("/log"):
            print("Exibindo logs recentes...")
            continue
        if cmd == "quit":
            print("Encerrando o sistema...")
            break

        

