import json
import socket
import time
import uuid


def cli_loop(connected_peers,name,namespace):
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
            Peer = connected_peers[target]["sock"]
            msg = {
                "type": "SEND",
                "msg_id": str(uuid.uuid4()),
                "src": name + "@" + namespace,
                "dst": target,
                "payload": message,
                "require_ack": True,
                "ttl": 1
            }
            Peer.sendall((json.dumps(msg) + "\n").encode())
            response = Peer.recv(4096).decode()
            messages = response.strip().split("\n")
            ack_received = False

            for raw_msg in messages:
                if raw_msg.strip():
                    data = json.loads(raw_msg)
                    print("Mensagem:", data)

                    if data.get("type") == "ACK":
                        ack_received = True

            print("Resposta bruta:", response)

            if ack_received:
                print("Mensagem entregue com sucesso!")
            else:
                print("Falha ao entregar a mensagem.")

