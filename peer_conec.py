import socket
import time
from rendezvous_connection import discorver_handler, register_handler


def hand_shake(peer_ip, peer_port):
    hello_msg = {
        "type": "HELLO",
        "peer_id": f"{name}@{namespace}",
        "version": "1.0",
        "features": ["ack", "metrics"],
        "ttl": 1
    }
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((peer_ip, peer_port))
        sock.sendall((json.dumps(hello_msg) + "\n").encode())

        response = sock.recv(4096).decode()
        print("Resposta do peer:", response)
        data = json.loads(response)
        sock.close()
        if data.get("type") == "HELLO_OK":
            print(f"Conectado com {peer_ip}:{peer_port}")
            return True
        else:
            sock.close()
            return False
    except Exception as e:
        return False

def ping(peer_ip, peer_port):
    ping_msg = {
        "type": "PING",
        "msg_id": "uuid",
        "timestamp": str(time.time()),
        "ttl": 1
    }
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((peer_ip, peer_port))
        sock.sendall((json.dumps(ping_msg) + "\n").encode())

        response = sock.recv(4096).decode()
        data = json.loads(response)
        sock.close()
        if data.get("type") == "PONG":
            return True
        else:
            return False
    except Exception as e:
        return False

def keep_alive(connected_peers):
    contador = 0
    while True:
        if conectado == False :
            if register_handler(name, namespace, PEER_PORT) == 1:
                conectado = True
            else:
                print("Falha ao registrar. Tente novamente.")
                return 0
        if conectado == True and time.time() - contador >= 30:
            peers = discorver_handler()
            contador = time.time()
            if peers == []:
                conectado = False
            elif peers != []:
                for peer in peers:
                    peer_id = f'{peer["name"]}@{peer["namespace"]}'
                    if peer_id not in connected_peers:
                        print(f"Conectando {peer_id}...")
                        if hand_shake(peer["ip"], peer["port"]):
                            connected_peers[peer_id] = {
                                "ip": peer["ip"],
                                "port": peer["port"],
                                "last_ping": time.time()
                            }
                    else:   
                        print(f"Pingando {peer_id}...")
                        if ping(peer["ip"], peer["port"]):
                            connected_peers[peer_id]["last_ping"] = time.time()
                        else:
                            print(f"{peer_id} não respondeu. Removendo dos conectados.")
                            del connected_peers[peer_id]
                

