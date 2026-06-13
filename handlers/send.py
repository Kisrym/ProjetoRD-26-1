from datetime import datetime, timezone
from cli import open_send
import json
from webapp import enviar_para_chat_web

def send_handler(conn, msg):
    remetente = msg.get('src')
    conteudo = msg.get('payload')

    if remetente and conteudo:
        enviar_para_chat_web(remetente, conteudo)

    print(f"\n[MENSAGEM] {msg.get('src')} -> {msg.get('dst')}")
    print(msg.get("payload"))

    ack = {
        "type": "ACK",
        "msg_id": msg.get("msg_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ttl": 1
    }

    conn.sendall((json.dumps(ack) + "\n").encode())

    return True

def ack_handler(msg):

    msg_id = msg.get("msg_id")

    event = open_send.get(msg_id)

    if event:
        print(f"Recebido ACK para msg_id: {msg_id}")
        event.set()
        return True

    print("Recebido ACK sem solicitação prévia.")
    return False

def pub_handler(msg):
    dst = msg.get("dst", "").replace("#", "")
    conteudo = msg.get('payload')
    if dst and conteudo:
        enviar_para_chat_web(dst, conteudo)
        print(f"\n[MENSAGEM] {msg.get('src')} -> {msg.get('dst')}")
        print(msg.get("payload"))
        return True
    