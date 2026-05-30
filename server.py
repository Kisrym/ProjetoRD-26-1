import socket
import json
import threading
from queue import Queue

message_queue = Queue()

def peer_listener(conn, addr):
    buffer = ""

    try:
        while True:
            data = conn.recv(4096).decode()
            if not data:
                break

            buffer += data

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)

                if not line.strip():
                    continue

                msg = json.loads(line)

                message_queue.put({
                    "conn": conn,
                    "addr": addr,
                    "msg": msg
                })

    except Exception as e:
        print("Erro listener:", e)

    finally:
        conn.close()

def servidor(host="0.0.0.0", port=4000):
    print(f"[SERVIDOR] escutando em {host}:{port}")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen()
    print(f"[SERVIDOR] escutando em {host}:{port}")

    while True:

        conn, addr = server.accept()
        print(f"[NOVA CONEXÃO] {addr}")
        listener_thread = threading.Thread(
            target=peer_listener,
            args=(conn, addr),
            daemon=True
        )

        listener_thread.start()