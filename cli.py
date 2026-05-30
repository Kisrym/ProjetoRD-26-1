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
    while True:
        cmd = input("Digite um comando (view, exit, send): ").strip().upper()
        if cmd == "VIEW":
            print("Peers conectados:")
            for peer_id, info in connected_peers.items():
                print(f'{peer_id} - IP: {info["ip"]}, Porta: {info["port"]}, Último ping: {time.ctime(info["last_ping"])}')
        elif cmd == "EXIT":
            break
        elif cmd == "SEND":
            print("Peers disponíveis:")
            for peer_id, info in connected_peers.items():
                print(f'{peer_id}')

            target = input("Digite o nome do peer de destino (formato name@namespace): ").strip()

            if target not in connected_peers:
                print("Peer não encontrado.")
                continue

            message = input("Digite a mensagem a ser enviada: ").strip()
            send(target,connected_peers[target]["sock"],name,namespace,message)

