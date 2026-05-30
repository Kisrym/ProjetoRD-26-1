from datetime import datetime, timezone
from peer_conec import open_send
import json
import time

def ack_handler(conn,addr, connected_peers, msg, name, namespace):
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

def ack_handler(conn,addr, connected_peers, msg, name, namespace):
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