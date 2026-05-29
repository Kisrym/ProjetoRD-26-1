import socket
import time
import json
import threading
from datetime import datetime, timezone

def hello_OK_handler(conn, connected_peers, msg, name, namespace):
    peer_id = msg.get("peer_id")
    if not connected_peers.get(peer_id):
        connected_peers[peer_id] = {
            "peer_id": peer_id,
            "ip": msg.get("ip"),
            "port": msg.get("port"),
            "sock": conn,
            "last_ping": time.time()
        }
    else:
        connected_peers[peer_id] = {
            "last_ping": time.time(),
        }

    response = {
        "type": "HELLO_OK",
        "peer_id": name + "@" + namespace,
        "version": "1.0",
        "features": ["ack", "metrics"],
        "ttl": 1
    }

    conn.sendall((json.dumps(response) + "\n").encode())
    return True

def pong_handler(conn, connected_peers, msg, name, namespace):
    peer_id = msg.get("peer_id")
    if not connected_peers.get(peer_id):
        connected_peers[peer_id] = {
            "peer_id": peer_id,
            "ip": msg.get("ip"),
            "port": msg.get("port"),
            "sock": conn,
            "last_ping": time.time()
        }
    else:
        connected_peers[peer_id] = {
            "last_ping": time.time(),
        }

    response = {
        "type": "PONG",
        "msg_id": msg.get("msg_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ttl": 1
    }

    conn.sendall((json.dumps(response) + "\n").encode())
    return True

def ack_handler(conn, connected_peers, msg, name, namespace):
    peer_id = msg.get("peer_id")
    if not connected_peers.get(peer_id):
        connected_peers[peer_id] = {
            "peer_id": peer_id,
            "ip": msg.get("ip"),
            "port": msg.get("port"),
            "sock": conn,
            "last_ping": time.time()
        }
    else:
        connected_peers[peer_id] = {
            "last_ping": time.time(),
        }

    ack = {
        "type": "ACK",
        "msg_id": msg.get("msg_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ttl": 1
    }

    conn.sendall((json.dumps(ack) + "\n").encode())
    return True

def handle_client(conn, addr, connected_peers, name, namespace):
    print(f"[CONEXÃO] {addr}")

    buffer = ""

    try:
        while True:
            data = conn.recv(4096).decode()
            if not data:
                break
            buffer += data
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip():
                    continue
                msg = json.loads(line)
                msg_type = msg.get("type")

                if msg_type == "HELLO":
                    hello_OK_handler(conn,connected_peers, msg, name, namespace)
                elif msg_type == "PING":
                    pong_handler(conn,connected_peers, msg, name, namespace)
                elif msg_type == "SEND":
                    ack_handler(conn,connected_peers, msg, name, namespace)

    except Exception as e:
        print("Erro conexão:", e)


def servidor(connected_peers, name, namespace, host="0.0.0.0", port=4000):

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()

    print(f"[SERVIDOR] escutando em {host}:{port}")

    while True:
        conn, addr = server.accept()

        thread = threading.Thread(
            target=handle_client,
            args=(conn, addr, connected_peers, name, namespace),
            daemon=True
        )

        thread.start()