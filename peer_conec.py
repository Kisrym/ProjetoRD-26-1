import json
import socket
import time
from datetime import datetime, timezone
import uuid
from rendezvous_connection import discorver_handler, register_handler
from config import PEER_PORT, HOST, PORT


def hand_shake(peer_ip, peer_port,peer_id):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((peer_ip, peer_port))
        hello_msg = {
            "type": "HELLO",
            "peer_id": peer_id,
            "version": "1.0",
            "features": ["ack", "metrics"],
            "ttl": 1
        }
        sock.sendall((json.dumps(hello_msg) + "\n").encode())

        response = sock.recv(4096).decode()
        print("Resposta do peer:", response)
        data = json.loads(response)
        if data.get("type") == "HELLO_OK":
            print(f"Conectado com {peer_ip}:{peer_port}")
            return sock
        sock.close()
        return None
    
    except Exception as e:
        print("Handshake error:", e)
        return False

def ping(sock, peer_id):
    try:
        ping_msg = {
            "type": "PING",
            "msg_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "ttl": 1
        }
        sock.sendall((json.dumps(ping_msg) + "\n").encode())

        response = sock.recv(4096).decode()
        print("Resposta do PING:", response)
        data = json.loads(response)
        if data.get("type") in ["PONG", "PING"]:
            print(f"{peer_id} respondeu ao ping.")
            return True
        else:
            print(f"{peer_id} respondeu com mensagem inesperada.")
            return False
    
    except Exception as e:
        print(f"Erro ping {peer_id}:", e)
        return False

def keep_alive(connected_peers,name,namespace):
    contador = 0
    conectado = False
    while True:
        if not conectado:
            if register_handler(name, namespace, PEER_PORT) == 1:
                conectado = True
            else:
                print("Falha ao registrar. Tente novamente.")
                return
        if conectado and time.time() - contador >= 30:
            peers = discorver_handler()
            contador = time.time()
            if not peers:
                conectado = False
                continue
            for peer in peers:
                peer_id = f'{peer["name"]}@{peer["namespace"]}'
                if peer_id not in connected_peers:
                    print(f"Conectando {peer_id}...")
                    sock = hand_shake(peer["ip"], peer["port"],peer_id)
                    if sock:
                        connected_peers[peer_id] = {
                            "ip": peer["ip"],
                            "port": peer["port"],
                            "sock": sock,
                            "last_ping": time.time()
                        }
                else:   
                    print(f"Pingando {peer_id}...")

                    sock = connected_peers[peer_id]["sock"]

                    if ping(sock, peer_id):
                        connected_peers[peer_id]["last_ping"] = time.time()
                    else:
                        print(f"{peer_id} caiu. Removendo...")
                        sock.close()
                        del connected_peers[peer_id]
                

