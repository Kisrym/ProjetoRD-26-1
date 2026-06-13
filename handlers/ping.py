from datetime import datetime, timezone
import threading
import uuid
from peer_conec import open_ping
import json
import time

def send_ping_handler(connected_peers, peer_id):
    for peer_id in list(connected_peers.keys()):
        if time.time() - connected_peers[peer_id]["last_ping"] >= 60:
            print(f"Pingando {peer_id}...")
            sock = connected_peers[peer_id]["sock"]
            if send_ping(sock, peer_id):
                connected_peers[peer_id]["last_ping"] = time.time()
            else:
                sock.close()
                del connected_peers[peer_id]

def send_ping(sock, peer_id):
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

        if event.wait(timeout=2):
            del open_ping[msg_id]
            return True
        
        open_ping.pop(msg_id, None)
        print("Timeout esperando PONG")
        return False
    
    except Exception as e:
        print(f"Erro ping {peer_id}:", e)
        open_ping.pop(msg_id, None)
        return False

def ping_handler(conn, addr, msg):
    response = {
        "type": "PONG",
        "msg_id": msg.get("msg_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ttl": 1
    }
    conn.sendall((json.dumps(response) + "\n").encode())
    print(f"Respondido PING de {addr}")
    return True

def pong_handler(msg):
    msg_id = msg.get("msg_id")
    if msg_id in open_ping:
        open_ping[msg_id].set()
        return True
    else:
        print(f"Recebido PONG sem solicitação prévia.")
        return False