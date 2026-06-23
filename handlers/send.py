import asyncio
from datetime import datetime, timezone
import json

from interfaces.cli import open_send
from interfaces.web.app import enviar_para_chat_web


async def send_handler(writer: asyncio.StreamWriter, msg):
    """
    Trata o recebimento de uma mensagem direta (SEND), exibe na tela/web 
    e responde com um ACK.
    """
    remetente = msg.get('src')
    conteudo = msg.get('payload')

    if remetente and conteudo:
        await enviar_para_chat_web(remetente, conteudo)

    print(f"\n[MENSAGEM] {msg.get('src')} -> {msg.get('dst')}")
    print(msg.get("payload"))

    ack = {
        "type": "ACK",
        "msg_id": msg.get("msg_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ttl": 1
    }

    try:
        writer.write((json.dumps(ack) + "\n").encode())
        await writer.drain()

    except Exception as e:
        print(f"[SEND-HANDLER] Erro ao enviar ACK: {e}")

    return True


async def ack_handler(msg):
    """
    Trata a chegada de um ACK, liberando a trava da CLI que enviou a mensagem.
    """
    msg_id = msg.get("msg_id")
    event = open_send.get(msg_id)

    if event:
        print(f"[ACK] Recebido ACK para msg_id: {msg_id}")
        event.set()
        return True

    print("[ACK] Recebido ACK sem solicitação prévia (ou já expirado).")
    return False


async def pub_handler(msg):
    """
    Trata mensagens de publicação em canais/grupos (PUB).
    """
    src = msg.get("src", "Desconhecido")
    dst = msg.get("dst", "*")
    payload = msg.get("payload", "")
    
    id_grupo_visual = dst if (dst == "*" or dst.startswith("#")) else f"#{dst}"

    if dst and payload:
        print(f"\n[MENSAGEM PUB] {msg.get('src')} -> {msg.get('dst')}")
        print(msg.get("payload"))
        await enviar_para_chat_web(target_id=id_grupo_visual, mensagem=payload)
        return True
    
    return False