from handlers.ping import send_ping_handler
from rendezvous_connection import discorver_handler, register_handler
from handlers.hello import cadastrar_peers
from config import PEER_PORT
import time
grups_online={}
def add_grups(connected_peers):
    for peer_id in connected_peers.items():
        if "@" not in peer_id:
            continue

        name, ns = peer_id.split("@", 1)

        if name not in grups_online.get(ns, []):
            grups_online.setdefault(ns, []).append(name)

def keep_alive(connected_peers,name,namespace):
    timer = 0
    while True:
        if time.time() - timer >= 30:
            timer = time.time()
            peers, err = discorver_handler()
            if err:
                print("Erro ao descobrir peers:", err)
                return 1
            cadastrar_peers(peers, connected_peers,name,namespace)

        send_ping_handler(connected_peers, name)
        
        add_grups(connected_peers)

        time.sleep(1)
                

def peer_connection(connected_peers,name, namespace):
    Registered = False
    while True:
        if not Registered:
            if register_handler(name, namespace, PEER_PORT):
                Registered = True
            else:
                return
        if Registered:
            erro = keep_alive(connected_peers,name, namespace)
            if erro == 1:
                Registered = False
                print("Erro no keep-alive, tentando registrar novamente...")