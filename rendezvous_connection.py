import socket
import json
import time

def register_handler(name, namespace, peer_port):
    contador = 0
    while contador < 3:
        if register(name, namespace, peer_port) == 1:
            return True
        else:
            contador += 1
            print(f"Tentativa {contador} de registro falhou. Tentando novamente em 5 segundos...")
            time.sleep(5)
    print("Falha ao registrar o peer após 3 tentativas.")
    return False

def register(name, namespace, peer_port, ttl=3600):
    Rendezvous = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    Rendezvous.connect((HOST, PORT))
    msg = {
        "type": "REGISTER",
        "namespace": namespace,
        "name": name,
        "port": peer_port,
        "ttl": ttl
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

def discorver_handler(namespace=None):
    contador = 0
    while contador < 3:
        peers = discover(namespace)
        if peers:
            print("Peers encontrados:")
            return peers
        else:
            contador += 1
            print(f"Tentativa {contador} de descoberta falhou. Tentando novamente em 5 segundos...")
            time.sleep(5)
    print("Falha ao descobrir peers após 3 tentativas.")
    return []

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
        if data["message"] == "bad_namespace":
            print("Namespace Invalido. deve conter entre 1 e 63 caracteres.")
        if data["message"] == "peer_not_registered":
            print("Peer não registrado.")
        return []

    elif data["status"] == "OK":
        peers = data["peers"]
        return peers

    else:
        print("Resposta desconhecida do servidor.")
        return []