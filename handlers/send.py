# ==============================================================================
# UNIVERSIDADE DE BRASÍLIA (UnB) - DEPARTAMENTO DE CIÊNCIA DA COMPUTAÇÃO
# REDES DE COMPUTADORES - SEMESTRE: 2026/1 - PROF. MARCOS FAGUNDES CAETANO
# PROJETO FINAL: Chat Peer-to-Peer (P2P) | GRUPO 8
# 
# INTEGRANTES:
#   - Kaio Santos Araújo       (Matrícula: 242009972)
#   - Caio Dias Fleury         (Matrícula: 242009909)
#   - João Paulo Silva Mendes  (Matrícula: 242026187)
# 
# ARQUIVO: handlers/send.py
# ==============================================================================

import asyncio
from datetime import datetime, timezone
import json
import logging

from interfaces.cli import open_send
from interfaces.web.app import enviar_para_chat_web

log = logging.getLogger("SEND")

async def send_handler(writer: asyncio.StreamWriter, msg):
    """
    Trata o recebimento de uma mensagem direta (SEND), exibe na tela/web 
    e responde com um ACK.
    """
    remetente = msg.get('src')
    conteudo = msg.get('payload')

    if remetente and conteudo:
        await enviar_para_chat_web(remetente, conteudo)

    log.info(f"(MENSAGEM) {msg.get('src')} -> {msg.get('dst')}")
    log.info(msg.get("payload"))

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
        log.error(f"(SEND-HANDLER) Erro ao enviar ACK: {e}")

    return True


async def ack_handler(msg):
    """
    Trata a chegada de um ACK, liberando a trava da CLI que enviou a mensagem.
    """
    msg_id = msg.get("msg_id")
    event = open_send.get(msg_id)

    if event:
        log.info(f"(ACK) Recebido ACK para msg_id: {msg_id}")
        event.set()
        return True

    log.warning("(ACK) Recebido ACK sem solicitação prévia (ou já expirado).")
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
        log.info(f"\n(MENSAGEM PUB) {msg.get('src')} -> {msg.get('dst')}")
        log.info(msg.get("payload"))
        await enviar_para_chat_web(target_id=id_grupo_visual, mensagem=payload)
        return True
    
    return False