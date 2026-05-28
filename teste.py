import socket
import json
import time

with open("config.json", "r") as f:
    config = json.load(f)

HOST = config["host"]
PORT = config["port"]
PEER_PORT = config["peer-port"]

def register(name, namespace, peer_port):
    Rendezvous = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    Rendezvous.connect((HOST, PORT))
    msg = {
        "type": "REGISTER",
        "namespace": namespace,
        "name": name,
        "port": peer_port,
        "ttl": 3600
    }
    Rendezvous.sendall((json.dumps(msg) + "\n").encode())
    response = Rendezvous.recv(4096)
    print("Resposta do servidor:", response.decode())
    data = json.loads(response)
    if data["status"] == "ERROR":
        if data["message"] == "bad_name":
            print("Nome Invalido. deve conter entre 1 e 63 caracteres")
        elif data["message"] == "bad_namespace":
            print("Namespace Invalido. deve conter entre 1 e 63 caracteres.")
        elif data["message"] == "bad_ttl":
            print("TTL Invalido. deve ser um inteiro entre 1 e 86400.")
        elif data["message"] == "bad_port":
            print("Porta Invalida. deve ser um inteiro entre 1 e 65535.")
        Rendezvous.close()
        return 0
    elif data["status"] == "OK":     
        print("Registrado com sucesso!")
        Rendezvous.close()
        return 1
    else:
        print("Resposta desconhecida do servidor.")
        Rendezvous.close()
        return 0

def discover(namespace=None):
    Rendezvous = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    Rendezvous.connect((HOST, PORT))
    msg = {
        "type": "DISCOVER"
    }

    if namespace is not None:
        msg["namespace"] = namespace

    Rendezvous.sendall((json.dumps(msg) + "\n").encode())

    response = Rendezvous.recv(4096).decode()
    data = json.loads(response)

    if data["status"] == "ERROR":

        if data["message"] == "peer_not_registered":
            print("Peer não registrado.")

        return []

    elif data["status"] == "OK":
        peers = data["peers"]
        return peers

    else:
        print("Resposta desconhecida do servidor.")
        return []




name = "vitor"##input("Digite o nome do peer: ").strip()
namespace = "andre"##input("Digite o namespace do peer: ").strip()
connected_peers = {}
conectado = False
tentativas = 0
contador = 0
while True:
    if conectado == False :
        if register(name, namespace, PEER_PORT) == 1:
            conectado = True
            tentativas = 0
        else:
            print("Falha ao registrar. Tente novamente.")
            tentativas += 1
            if tentativas >= 5:
                print("Número máximo de tentativas atingido. Encerrando.")
                break
    if conectado == True and time.time() - contador >= 30:
        peers = discover()
        if peers == []:
            print("Peer não encontrado. Registrando novamente.")
            contador = 0
            conectado = False
        elif peers != []:
            print("Peer encontrado. Mantendo registro.")
            for peer in peers:
                peer_id = f'{peer["name"]}@{peer["namespace"]}'
                if peer_id not in connected_peers:
                    print(f"Conectando {peer_id}...")
                    hello_msg = {
                        "type": "HELLO",
                        "peer_id": f"{name}@{namespace}",
                        "version": "1.0",
                        "features": ["ack", "metrics"],
                        "ttl": 1
                    }
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((peer["ip"], peer["port"]))
                        sock.sendall((json.dumps(hello_msg) + "\n").encode())

                        response = sock.recv(4096).decode()
                        print("Resposta do peer:", response)
                        data = json.loads(response)
                        sock.close()
                        if data.get("type") == "HELLO_OK":
                            print(f"Conectado com {peer_id}")

                            connected_peers[peer_id] = {
                                "ip": peer["ip"],
                                "port": peer["port"],
                                "last_ping": time.time()
                            }
                        else:
                            sock.close()
                    except Exception as e:
                        continue 
                else:   
                    print(f"Pingando {peer_id}...")
                    ping_msg = {
                        "type": "PING",
                        "msg_id": "uuid",
                        "timestamp": str(time.time()),
                        "ttl": 1
                    }

                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((connected_peers[peer_id]["ip"], connected_peers[peer_id]["port"]))
                        sock.sendall((json.dumps(ping_msg) + "\n").encode())

                        response = sock.recv(4096).decode()
                        data = json.loads(response)
                        sock.close()
                        if data.get("type") == "PONG":
                            connected_peers[peer_id]["last_ping"] = time.time()
                            print(f"PONG recebido de {peer_id}")

                    except Exception as e:
                        print(f"Peer caiu: {peer_id}")
                        del connected_peers[peer_id]

            contador = time.time()
        

    cmd = input("Digite um comando (view, exit): ").strip().upper()
    
    if cmd == "VIEW":
        print("Peers conectados:")
        for peer_id, info in connected_peers.items():
            print(f'{peer_id} - IP: {info["ip"]}, Porta: {info["port"]}, Último ping: {time.ctime(info["last_ping"])}')
    elif cmd == "EXIT":
        break
    elif cmd == "SEND":
        print("Peers disponíveis:")
        for peer_id, info in connected_peers.items():
            print(f'{peer_id}')

        target = input("Digite o nome do peer de destino (formato name@namespace): ").strip()

        if target not in connected_peers:
            print("Peer não encontrado.")
            continue

        message = input("Digite a mensagem a ser enviada: ").strip()
        Peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        Peer.connect((connected_peers[target]["ip"], connected_peers[target]["port"]))
        '''msg = {
            "type": "SEND",
            "msg_id": "uuid",
            "src": name + "@" + namespace,
            "dst": target,
            "payload": message,
            "require_ack": True,
            "ttl": 1
        }'''
        Peer.sendall((json.dumps(msg) + "\n").encode())
        response = Peer.recv(4096)
        print ("Resposta do servidor:", response.decode())
        data = json.loads(response)
        if data["type"] == "ACK":
            print("Mensagem entregue com sucesso!")
        else:
            print("Falha ao entregar a mensagem.")
    

