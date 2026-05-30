from peer_conec import open_bye
import json
import time

def bye_handler(conn, connected_peers, msg, name, namespace):
    peer_id = msg.get("peer_id")
    if peer_id is None:
        return False
    connected_peers.pop(peer_id, None)
    response = {
        "type": "BYE_OK",
        "msg_id": msg.get("msg_id"),
        "src": f"{name}@{namespace}",
        "dst": peer_id,
        "ttl": 1
    }
    conn.sendall((json.dumps(response) + "\n").encode())
    conn.close()
    print(f"[BYE] {peer_id} desconectado")
    return True

def bye_ok_handler(conn, addr, connected_peers, msg):
    peer_id = msg.get("peer_id")
    if open_bye.get(peer_id):
        print(f"{peer_id} confirmou o BYE.")
        open_bye.pop(peer_id, None)
        connected_peers[peer_id] = {
            "peer_id": peer_id,
            "ip": addr[0],
            "port": addr[1],
            "sock": conn,
            "last_ping": time.time()
        }
        return True
    else:
        print(f"{peer_id} respondeu com BYE_OK sem solicitação prévia.")
        return False