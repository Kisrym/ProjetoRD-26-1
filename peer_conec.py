import asyncio
import time

from handlers.ping import *
from rendezvous_connection import *
from handlers.hello import hand_shake, cadastrar_peers
from config import PEER_PORT, PING_INTERVAL, PEER_RECONNECT_TRIES

grups_online = {}

def add_grups(connected_peers):
    for peer_id, _ in connected_peers.items():
        if "@" not in peer_id: # ?
            continue

        name, ns = peer_id.split("@", 1)

        if peer_id not in grups_online.get(f"#{ns}", []):
            grups_online.setdefault(f"#{ns}", []).append(peer_id)


async def keep_alive(connected_peers, name, namespace):
    timer = 0

    while True:
        if time.time() - timer >= 30: # a cada 30 segundos, atualiza a lista de peers no servidor
            timer = time.time()        
            peers, err = await discorver_handler()

            if err:
                print("[KEEP-ALIVE] Erro ao descobrir peers:", err)

            else:
                await cadastrar_peers(peers, connected_peers, name, namespace)


        now = time.time()
        peer_ids = list(connected_peers.keys())

        for pid in peer_ids:
            dados = connected_peers.get(pid)
            if not dados:
                continue

            if now - dados.get("last_ping", 0) >= PING_INTERVAL: # depois de PING_INTERVAL segundos envia um ping dnv
                writer = dados["writer"]
                ip = dados["ip"]
                port = dados["port"]

                print(f"[PING] {pid} ocioso, enviando PING...")

                success = await send_ping(writer, pid, connected_peers)

                if success:
                    dados["last_ping"] = time.time()

                else:
                    print(f"[KEEP_ALIVE] Detectada queda de {pid}. Removendo da tabela...")

                    # remove o socket desse peer
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass

                    connected_peers.pop(pid, None)

                    asyncio.create_task(
                        try_to_reconnect(pid, ip, port, connected_peers, name, namespace)
                    )
        
        add_grups(connected_peers)
        await asyncio.sleep(1)


async def peer_connection(connected_peers, name, namespace):
    """
    Controla o ciclo de vida do registro no servidor Rendezvous.
    """
    registered = False
    while True:
        if not registered:
            print("[CONEXÃO] Tentando registrar no servidor Rendezvous...")
            
            success = await register_handler(name, namespace, PEER_PORT)

            if success:
                registered = True
                print("[CONEXÃO] Registrado com sucesso!")
            else:
                print("[CONEXÃO] Falha ao registrar. Tentando novamente em 5 segundos...")
                await asyncio.sleep(5)
                continue

        if registered:
            erro = await keep_alive(connected_peers, name, namespace)
            if erro == 1:
                registered = False
                print("[CONEXÃO] Erro no keep-alive, tentando registrar novamente...")
                await asyncio.sleep(2)

async def try_to_reconnect(peer_id, ip, port, connected_peers, name, namespace):
    tries = 1
    timeout = 2.0

    print(f"[RECONEXÃO] Iniciando rotina de reconexão com {peer_id}")

    while tries <= PEER_RECONNECT_TRIES:
        # se o peer se conectou com o server, para a tentativa
        if peer_id in connected_peers:
            print(f"[RECONEXÃO] {peer_id} já se reconectou")
            return True
        
        try:
            writer = await hand_shake(ip, port, peer_id, name, namespace)

            if writer is not None:
                print(f"[RECONEXÃO] Sucesso! {peer_id} está conectado novamente.")

                connected_peers[peer_id] = {
                    "writer": writer,
                    "ip": ip,
                    "port": port,
                    "last_ping": time.time()
                }

                return True
            
        except Exception:
            pass

        if tries < PEER_RECONNECT_TRIES:
            await asyncio.sleep(timeout)
            timeout *= 2 # backoff exponencial
        
        tries += 1

    print(f"[RECONEXÃO] Esgotadas as {PEER_RECONNECT_TRIES} tentativas.")
    return False

async def close_all_connections(connected_peers, name, namespace):
    print("\n[QUIT] Iniciando encerramento...")

    bye_msg = {
        "type": "BYE",
        "msg_id": str(uuid.uuid4()),
        "src": f"{name}@{namespace}",
        "ttl": 1
    }
    payload_bytes = (json.dumps(bye_msg) + "\n").encode()

    tasks = []
    peers_ativos = list(connected_peers.items())
    
    if peers_ativos:
        print(f"[QUIT] Avisando {len(peers_ativos)} peers sobre a saida")
        
        for peer_id, dados in peers_ativos:
            writer = dados.get("writer")
            
            async def send_bye_and_close(w, pid):
                try:
                    w.write(payload_bytes)
                    await w.drain()
                    w.close()
                    await w.wait_closed()
                    print(f"[QUIT] Conexão com {pid} encerrada com sucesso.")

                except Exception:
                    pass

            tasks.append(send_bye_and_close(writer, peer_id))
        
        await asyncio.gather(*tasks, return_exceptions=True)

    connected_peers.clear()

    try:
        success = await unregister(name, namespace, PEER_PORT)

        if success:
            print("[QUIT] Desregistrado do servidor Rendezvous com sucesso.")
        else:
            print("[QUIT] Falha ao desregistrar do servidor Rendezvous.")
            
    except NameError:
        print("[QUIT] Alerta: unregister_handler não encontrado. Pulando desregistro central.")

    except Exception as e:
        print(f"[QUIT] Erro ao comunicar saída para o Rendezvous: {e}")

    print("[QUIT] Sistema P2P finalizado de forma limpa.")