import asyncio
import json

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
                    print(f"[SERVIDOR] Erro ao decodificar JSON de {addr}")

    except Exception as e:
        print(f"[SERVIDOR] Erro no listener do peer {addr}: {e}")

    finally:
        print(f"[SERVIDOR] Conexão encerrada com {addr}")
        writer.close()
        await writer.wait_closed()


async def servidor(port, host="0.0.0.0"):
    """
    Inicia o servidor TCP assíncrono.
    """
    print(f"[SERVIDOR] Iniciando em {host}:{port}...")

    async def handle_client(reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"[NOVA CONEXÃO ASSÍNCRONA] {addr}")
        
        # cria uma task em background para escutar o cliente novo
        asyncio.create_task(peer_listener(reader, writer, addr))

    server = await asyncio.start_server(handle_client, host, port)

    print(f"[SERVIDOR] Escutando em {host}:{port}")

    # roda até ser interrompido
    async with server:
        await server.serve_forever()