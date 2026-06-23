import asyncio
from core.server import message_queue

from handlers.hello import *
from handlers.ping import *
from handlers.send import *
from handlers.bye import *

async def message_router(name, namespace):
    print("[ROTEADOR] Inicializado e aguardando mensagens...")
    
    while True:
        event = await message_queue.get()

        writer = event["writer"] # funciona como o conn
        addr = event["addr"]
        msg = event["msg"]

        msg_type = msg.get("type")

        print(f"[ROTEADOR] Processando: {msg_type} de {addr}")

        try:
            if msg_type == "HELLO":
                await hello_handler(writer, addr, msg, name, namespace)

            elif msg_type == "HELLO_OK":
                await hello_ok_handler(msg)

            elif msg_type == "PING":
                await ping_handler(writer, addr, msg)

            elif msg_type == "PONG":
                await pong_handler(msg)

            elif msg_type == "PUB":
                await pub_handler(msg)

            elif msg_type == "SEND":
                await send_handler(writer, msg)

            elif msg_type == "ACK":
                await ack_handler(msg)

            elif msg_type == "BYE":
                await bye_handler(writer, msg)

            elif msg_type == "BYE_OK":
                await bye_ok_handler(msg)

        except Exception as e:
            print(f"[ROTEADOR] Erro ao processar mensagem {msg_type}: {e}")
        
        finally:
            message_queue.task_done()