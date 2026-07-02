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
# ARQUIVO: core/rendezvous.py
# ==============================================================================

import asyncio
import json
from config import *
from handlers.hello import cadastrar_peers
from config import PEER_TTL
import logging
from interfaces.web.app import interceptar_terminal, connected_peers

log = logging.getLogger("RENDEZVOUS")

async def register_loop(name, namespace, peer_port):
    while True:
        success, data = await register_handler(name, namespace, peer_port)

        if success and data is not None:
            await asyncio.sleep(data.get("ttl") * 0.8) # vai renovar o registro antes de acabar o ttl

        else:
            log.error("(REGISTER) Erro ao tentar registrar-se ao servidor. Tentando novamente...")
            await asyncio.sleep(RDZV_REGISTER_ATTEMPT_INTERVAL)

async def register_handler(name, namespace, peer_port):
    """
    Tenta registrar o peer no servidor Rendezvous até 3 vezes com espaçamento assíncrono.
    """
    contador = 0
    while contador < RDZV_RECONNECT_TRIES:
        status, data = await register(name, namespace, peer_port)
        if status == 1:
            return True, data
        
        else:
            contador += 1
            log.warning(f"Tentativa {contador}/{RDZV_RECONNECT_TRIES} de registro falhou. Tentando novamente em 5 segundos...")
            await asyncio.sleep(RDZV_REGISTER_ATTEMPT_INTERVAL)
            
    log.error(f"Falha ao registrar o peer após {RDZV_RECONNECT_TRIES} tentativas.")
    return False, None


async def register(name, namespace, peer_port):
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
            "ttl": PEER_TTL
        }
        
        writer.write((json.dumps(msg) + "\n").encode())
        await writer.drain()
        
        response_bytes = await reader.read(4096)
        if not response_bytes:
            log.error("Servidor fechou a conexão sem responder.")
            return 0, None
            
        response = response_bytes.decode()
        data = json.loads(response)
        
        writer.close()
        await writer.wait_closed()
        
        if data.get("status") == "ERROR":
            msg_err = data.get("message")

            if msg_err == "bad_name":
                log.error("Nome Inválido. Deve conter entre 1 e 63 caracteres.")

            elif msg_err == "bad_namespace":
                log.error("Namespace Inválivdo. Deve conter entre 1 e 63 caracteres.")

            elif msg_err == "bad_ttl":
                log.error("TTL Inválido. Deve ser um inteiro entre 1 e 86400.")

            elif msg_err == "bad_port":
                log.error("Porta Inválida. Deve ser um inteiro entre 1 e 65535.")

            return 0, None
            
        elif data.get("status") == "OK":     
            log.info("Registrado com sucesso!")
            return 1, data
        
        else:
            log.warning("Resposta desconhecida do servidor.")
            return 0, None

    except Exception as e:
        log.error(f"Erro de conexão ao registrar: {e}")
        return 0, None


async def unregister(name, namespace, port):
    command = {
        "name" : name,
        "type" : "UNREGISTER",
        "namespace" : namespace,
        "port" : port
    }

    reader, writer = await asyncio.open_connection(HOST, PORT)

    try:
        payload = (json.dumps(command) + "\n").encode('utf-8')
        writer.write(payload)
        await writer.drain() # da flush no buffer de write; na pratica, espera os dados entrarem na rede de fato

        response = await reader.readline()
        data = json.loads(response.decode("utf-8"))
        
        if data.get("status") == 'ERROR':
            if data.get('message') == "ad_port (abc)":
                log.error("(UNREGISTER) Port inválido ou está fora do intervalo [1, 65535]")

            elif data.get('message') == "bad_namespace":
                log.error("(UNREGISTER) Namespace Inválivdo. Deve conter entre 1 e 63 caracteres.")

            elif data.get('message') == "peer_not_registered":
                log.error("(UNREGISTER) Peer não registrado no rendezvous")

            elif data.get('message') == "namespace_required":
                log.error("(UNREGISTER) Campo obrigatório 'namespace' ausente")

            elif data.get('message') == "peer_credentials_do_not_match":
                log.error("(UNREGISTER) Credenciais de usuário não correspondem ao banco de dados")

            return 0
        
        return 1
    
    except Exception as e:
        log.error("(UNREGISTER) Erro ao retirar registro:", e)
        return None
    
    finally:
        writer.close()
        await writer.wait_closed()

async def discover_loop(name: str, peer_namespace: str, namespace = None):
    while True:
        peers, err = await discorver_handler(name, peer_namespace)

        if err:
            log.error("Erro ao descobrir peers:", err)

        else:
            await cadastrar_peers(peers, name, peer_namespace)

        await asyncio.sleep(RDZV_DISCOVER_INTERVAL)

async def discorver_handler(my_name: str, my_namespace:str, namespace=None):
    """
    Tenta descobrir peers no servidor Rendezvous até 3 vezes.
    """
    contador = 0
    while contador < RDZV_DISCOVER_TRIES:
        peers = await discover(namespace)

        if peers:
            log.info("Peers encontrados:")

            for peer in peers:
                peer_id = f"{peer['name']}@{peer['namespace']}"
                
                if peer_id == f"{my_name}@{my_namespace}":
                    continue

                if peer_id in connected_peers.get_all_peers():
                    log.info(f" - {peer['name']}@{peer['namespace']}:{peer['port']}")

                else:
                    log.info(f" - {peer['name']}@{peer['namespace']}:{peer['port']} (NEW PEER)")

            return peers, 0
        
        else:
            contador += 1
            log.warning(f"Tentativa {contador}/{RDZV_DISCOVER_TRIES} de descoberta falhou. Tentando novamente em 5 segundos...")
            await asyncio.sleep(RDZV_REDISCOVER_INTERVAL)
            
    log.error("Falha ao descobrir peers após 3 tentativas.")
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
                log.error("Namespace Inválido. Deve conter entre 1 e 63 caracteres.")

            if msg_err == "peer_not_registered":
                log.error("Peer não registrado.")

            return []

        elif data.get("status") == "OK":
            return data.get("peers", [])
        
        else:
            log.warning("Resposta desconhecida do servidor.")
            return []
            
    except Exception as e:
        log.error(f"Erro de conexão ao descobrir peers: {e}")
        return []