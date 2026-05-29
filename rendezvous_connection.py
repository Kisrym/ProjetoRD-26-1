import socket
import json

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