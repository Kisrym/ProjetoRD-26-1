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
# ARQUIVO: core/server.py
# ==============================================================================

import asyncio
import json
import logging
from interfaces.web.app import connected_peers

log = logging.getLogger("SERVIDOR")

message_queue = asyncio.Queue() # a queue nativa é bloqueante

open_bye = {}
open_ping = {}
open_send = {}
open_hello = {}

async def peer_listener(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, addr):
    """
    Escuta mensagens de um peer específico usando Streams assíncronas.
    """
    buffer = ""
    try:
        while True:
            data_bytes = await reader.read(4096)
            if not data_bytes:
                break # conexão pedida pelo peer

            data = data_bytes.decode()
            buffer += data

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)

                if not line.strip():
                    continue

                try:
                    msg = json.loads(line)
                    
                    await message_queue.put({
                        "writer": writer, # funciona como o conn do socket
                        "addr": addr,
                        "msg": msg
                    })
                except json.JSONDecodeError:
                    log.error(f"Erro ao decodificar JSON de {addr}")

    except Exception as e:
        log.error(f"Erro no listener do peer {addr}: {e}")

    finally:
        log.info(f"Conexão encerrada com {addr}")
        writer.close()
        await writer.wait_closed()


async def servidor(port, host="0.0.0.0"):
    """
    Inicia o servidor TCP assíncrono.
    """
    log.info(f"Iniciando em {host}:{port}...")

    async def handle_client(reader, writer):
        addr = writer.get_extra_info('peername')
        log.info(f"(NOVA CONEXÃO) {addr}")
        
        # cria uma task em background para escutar o cliente novo
        asyncio.create_task(peer_listener(reader, writer, addr))

    server = await asyncio.start_server(handle_client, host, port)

    log.info(f"Escutando em {host}:{port}")

    # roda até ser interrompido
    async with server:
        await server.serve_forever()