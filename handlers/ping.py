from datetime import datetime, timezone
from peer_conec import open_ping
import json
import time

def ping_handler(conn, addr, connected_peers, msg, name, namespace):
    response = {
        "type": "PONG",
        "msg_id": msg.get("msg_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ttl": 1
    }
    conn.sendall((json.dumps(response) + "\n").encode())
    return True

def pong_handler(msg):
    msg_id = msg.get("msg_id")
    if msg_id in open_ping:
        open_ping[msg_id].set()
        return True
    else:
        print(f"Recebido PONG sem solicitação prévia.")
        return False