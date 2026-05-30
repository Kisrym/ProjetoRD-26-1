from peer_conec import open_hello
import json
import time

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