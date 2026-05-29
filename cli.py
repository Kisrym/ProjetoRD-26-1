import json
import socket
import time


def cli_loop(connected_peers):
    while True:
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

