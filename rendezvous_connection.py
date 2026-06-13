import asyncio
import json
from config import *


async def register_handler(name, namespace, peer_port):
    """
    Tenta registrar o peer no servidor Rendezvous até 3 vezes com espaçamento assíncrono.
    """
    contador = 0
    while contador < RDZV_RECONNECT_TRIES:
        status = await register(name, namespace, peer_port)
        if status == 1:
            return True
        
        else:
            contador += 1
            print(f"[RENDEZVOUS] Tentativa {contador}/{RDZV_RECONNECT_TRIES} de registro falhou. Tentando novamente em 5 segundos...")
            await asyncio.sleep(5)
            
    print(f"[RENDEZVOUS] Falha ao registrar o peer após {RDZV_RECONNECT_TRIES} tentativas.")
    return False


async def register(name, namespace, peer_port, ttl=3600):
    """
    Abre uma conexão temporária, envia o comando REGISTER e trata a resposta.
    """
    try:
        reader, writer = await asyncio.open_connection(HOST, PORT)
        
        msg = {
            "type": "REGISTER",
            "namespace": namespace,
            "name": name,
            "port": peer_port,
            "ttl": ttl
        }
        
        writer.write((json.dumps(msg) + "\n").encode())
        await writer.drain()
        
        response_bytes = await reader.read(4096)
        if not response_bytes:
            print("[RENDEZVOUS] Servidor fechou a conexão sem responder.")
            return 0
            
        response = response_bytes.decode()
        print("[RENDEZVOUS] Resposta do servidor:", response)
        data = json.loads(response)
        
        writer.close()
        await writer.wait_closed()
        
        if data.get("status") == "ERROR":
            msg_err = data.get("message")

            if msg_err == "bad_name":
                print("[RENDEZVOUS] Nome Inválido. Deve conter entre 1 e 63 caracteres.")

            elif msg_err == "bad_namespace":
                print("[RENDEZVOUS] Namespace Inválivdo. Deve conter entre 1 e 63 caracteres.")

            elif msg_err == "bad_ttl":
                print("[RENDEZVOUS] TTL Inválido. Deve ser um inteiro entre 1 e 86400.")

            elif msg_err == "bad_port":
                print("[RENDEZVOUS] Porta Inválida. Deve ser um inteiro entre 1 e 65535.")

            return 0
            
        elif data.get("status") == "OK":     
            print("[RENDEZVOUS] Registrado com sucesso!")
            return 1
        
        else:
            print("[RENDEZVOUS] Resposta desconhecida do servidor.")
            return 0

    except Exception as e:
        print(f"[RENDEZVOUS] Erro de conexão ao registrar: {e}")
        return 0


async def discorver_handler(namespace=None):
    """
    Tenta descobrir peers no servidor Rendezvous até 3 vezes.
    """
    contador = 0
    while contador < RDZV_DISCOVER_TRIES:
        peers = await discover(namespace)

        if peers:
            print("[RENDEZVOUS] Peers encontrados:")

            for peer in peers:
                print(f" - {peer['name']}@{peer['namespace']}:{peer['port']}")

            return peers, 0
        
        else:
            contador += 1
            print(f"[RENDEZVOUS] Tentativa {contador}/{RDZV_DISCOVER_TRIES} de descoberta falhou. Tentando novamente em 5 segundos...")
            await asyncio.sleep(5)
            
    print("[RENDEZVOUS] Falha ao descobrir peers após 3 tentativas.")
    return [], 1


async def discover(namespace=None):
    """
    Abre uma conexão temporária, envia o comando DISCOVER e extrai a lista de peers.
    """
    try:
        reader, writer = await asyncio.open_connection(HOST, PORT)
        
        msg = {"type": "DISCOVER"}
        if namespace is not None:
            msg["namespace"] = namespace

        writer.write((json.dumps(msg) + "\n").encode())
        await writer.drain()

        response_bytes = await reader.read(4096)
        if not response_bytes:
            return []
            
        response = response_bytes.decode()
        data = json.loads(response)

        writer.close()
        await writer.wait_closed()

        if data.get("status") == "ERROR":
            msg_err = data.get("message")

            if msg_err == "bad_namespace":
                print("[RENDEZVOUS] Namespace Inválido. Deve conter entre 1 e 63 caracteres.")

            if msg_err == "peer_not_registered":
                print("[RENDEZVOUS] Peer não registrado.")

            return []

        elif data.get("status") == "OK":
            return data.get("peers", [])
        
        else:
            print("[RENDEZVOUS] Resposta desconhecida do servidor.")
            return []
            
    except Exception as e:
        print(f"[RENDEZVOUS] Erro de conexão ao descobrir peers: {e}")
        return []