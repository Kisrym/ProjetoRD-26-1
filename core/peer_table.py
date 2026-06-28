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
# ARQUIVO: core/peer_table.py
# ==============================================================================

import asyncio
import logging

log = logging.getLogger("PEER TABLE")

class PeerTable:
    def __init__(self):
        self.peers = {}

    def registrar_peer(self, peer: dict):
        peer_id = f"{peer.get('name')}@{peer.get('namespace')}"

        if peer_id in self.peers: return

        self.peers[peer_id] = {
            "peer_id" : peer_id,
            "ip" : peer.get("ip"),
            "port" : peer.get("port"),
            "name" : peer.get("name"),
            "namespace" : peer.get("namespace"),
            "ttl" : peer.get("ttl"),
            "expires_in" : peer.get("expires_in"),
            "connection_status" : "TRYING_CONNECTION"
        }

    def connect_peer(self, peer_id: str, writer: asyncio.StreamWriter, last_ping: float, direction: str):
        if peer_id not in self.peers:
            log.error("Peer não está registrado na tabela.")

        self.peers[peer_id]["writer"] = writer
        self.peers[peer_id]["last_ping"] = last_ping
        self.peers[peer_id]["direction"] = direction

    def change_peer_connection_status(self, peer_id: str, status: str):
        if status not in ["CONNECTED", "TRYING_CONNECTION", "DISCONNECTED"]:
            log.error("Status inválido.")
            return
        
        self.peers[peer_id]['connection_status'] = status

    def change_peer_connection_direction(self, peer_id: str, direction: str):
        if direction != "inbound" and direction != "outbound":
            log.error("Direção inválida.")
            return
        
        self.peers[peer_id]['direction'] = direction

    def get_all_peers(self):
        return self.peers
    
    def get_specific_connections(self, connection_type: str):
        lista = []
        for key, value in self.peers.items():
            if value.get("connection_status") == "CONNECTED" and value.get("direction") == connection_type:
                lista.append(key)

        return lista


    def items(self):
        return self.peers.items()
    
    def keys(self):
        return self.peers.keys()
    
    def get(self, key: str):
        return self.peers.get(key)
    
