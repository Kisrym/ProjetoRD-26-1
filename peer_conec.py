from rendezvous_connection import discorver_handler, register_handler
from config import PEER_PORT, HOST, PORT
from datetime import datetime, timezone
from server import peer_listener
import json
import socket
import time
import threading
import uuid

open_bye={}
open_ping={}
open_send={}
open_hello = {}

def hand_shake(peer_ip, peer_port,peer_id,name,namespace):
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((peer_ip, peer_port))

        event = threading.Event()
        open_hello[peer_id] = event

        threading.Thread(
            target=peer_listener,
            args=(sock, peer_id),
            daemon=True
        ).start()   

        hello_msg = {
            "type": "HELLO",
            "peer_id": f"{name}@{namespace}",
            "version": "1.0",
            "features": ["ack", "metrics"],
            "ttl": 1
        }
        sock.sendall((json.dumps(hello_msg) + "\n").encode())

        if event.wait(timeout=5):
            open_hello.pop(peer_id, None)
            print(f"Registrado com {peer_ip}:{peer_port}")
            return sock

        open_hello.pop(peer_id, None)
        print("Timeout esperando HELLO_OK")
        sock.close()
        return None
    
    except Exception as e:
        open_hello.pop(peer_id, None)
        print("Handshake error:", e)
        return None
    finally:
        open_hello.pop(peer_id, None)

def ping(sock, peer_id):
    msg_id = str(uuid.uuid4())
    try:
        event = threading.Event()
        open_ping[msg_id] = event
        ping_msg = {
            "type": "PING",
            "msg_id": msg_id,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "ttl": 1
        }
        sock.sendall((json.dumps(ping_msg) + "\n").encode())

        if event.wait(timeout=5):
            del open_ping[msg_id]
            return True
        
        open_ping.pop(msg_id, None)
        print("Timeout esperando PONG")
        return False
    
    except Exception as e:
        print(f"Erro ping {peer_id}:", e)
        open_ping.pop(msg_id, None)
        return False

def keep_alive(connected_peers,name,namespace):
    contador = 0
    registrado = False
    while True:
        if not registrado:
            if register_handler(name, namespace, PEER_PORT):
                registrado = True
            else:
                return
        if registrado and time.time() - contador >= 60:
            peers = discorver_handler()
            for peer in peers:
                if peer["name"] == name and peer["namespace"] == namespace:
                    MEU_IP = peer["ip"]
            contador = time.time()
            if not peers:
                registrado = False
                continue
            for peer in peers:
                if peer["ip"] == MEU_IP:
                    continue
                peer_id = f'{peer["name"]}@{peer["namespace"]}'
                if peer_id not in connected_peers:
                    print(f"Descobrindo {peer_id} em {peer['ip']}:{peer['port']}...")
                    sock = hand_shake(peer["ip"], peer["port"],peer_id,name,namespace)
                    if sock:
                        connected_peers[peer_id] = {
                            "peer_id": peer_id,
                            "ip": peer["ip"],
                            "port": peer["port"],
                            "sock": sock,
                            "last_ping": time.time()
                        }
        if registrado:
            for peer_id in list(connected_peers.keys()):
                if time.time() - connected_peers[peer_id]["last_ping"] >= 60:
                    print(f"Pingando {peer_id}...")
                    sock = connected_peers[peer_id]["sock"]
                    if ping(sock, peer_id):
                        connected_peers[peer_id]["last_ping"] = time.time()
                    else:
                        sock.close()
                        del connected_peers[peer_id]
                

