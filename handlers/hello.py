import threading
import socket

from peer_conec import open_hello
import json
import time

from server import peer_listener

def hand_shake(peer_ip, peer_port,peer_id,name,namespace):
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
        print(f"Enviado HELLO para {peer_id} em {peer_ip}:{peer_port}")
        if event.wait(timeout=2):
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
    
def cadastrar_peers(peers, connected_peers,name,namespace):
    for peer in peers:
        #if peer["name"] == name and peer["namespace"] == namespace:
        #    continue
        peer_id = f'{peer["name"]}@{peer["namespace"]}'
        if peer_id not in connected_peers:
            print(f"Descobrindo {peer_id} em {peer['ip']}:{peer['port']}...")
            sock = hand_shake(peer["ip"], peer["port"],peer_id,name,namespace)
            if sock:
                print(f"Conectado a {peer_id}")
                connected_peers[peer_id] = {
                    "peer_id": peer_id,
                    "ip": peer["ip"],
                    "port": peer["port"],
                    "sock": sock,
                    "last_ping": time.time()
                }


def hello_handler(conn, addr, connected_peers, msg, name, namespace):
    peer_id = msg.get("peer_id")
    if peer_id is None:
        return False
    connected_peers[peer_id] = {
        "peer_id": peer_id,
        "ip": addr[0],
        "port": addr[1],
        "sock": conn,
        "last_ping": time.time()
    }
    response = {
        "type": "HELLO_OK",
        "peer_id": f"{name}@{namespace}",
        "version": "1.0",
        "features": ["ack", "metrics"],
        "ttl": 1
    }
    conn.sendall((json.dumps(response) + "\n").encode())
    print(f"[HELLO] {peer_id} conectado")
    return True

def hello_ok_handler(msg):
    peer_id = msg.get("peer_id")

    event = open_hello.get(peer_id)

    if event:
        event.set()
        return True
    else:        
        print(f"Recebido HELLO_OK de {peer_id} sem solicitação prévia.")
        return False