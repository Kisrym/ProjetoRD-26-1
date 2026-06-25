import asyncio

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
            print("[PEER_TABLE] Peer não está registrado na tabela.")

        self.peers[peer_id]["writer"] = writer
        self.peers[peer_id]["last_ping"] = last_ping
        self.peers[peer_id]["direction"] = direction

    def change_peer_connection_status(self, peer_id: str, status: str):
        if status not in ["CONNECTED", "TRYING_CONNECTION", "DISCONNECTED"]:
            print("[PEER_TABLE] Status inválido.")
            return
        
        self.peers[peer_id]['connection_status'] = status

    def change_peer_connection_direction(self, peer_id: str, direction: str):
        if direction != "inbound" and direction != "outbound":
            print("[PEER_TABLE] Direção inválida.")
            return
        
        self.peers[peer_id]['direction'] = direction

    def get_all_peers(self):
        return self.peers
    
    def get_specific_connections(self, connection_type: str):
        lista = []
        for key, value in self.peers.items():
            if value.get("direction") == connection_type:
                lista.append(key)

        return lista


    def items(self):
        return self.peers.items()
    
    def keys(self):
        return self.peers.keys()
    
    def get(self, key: str):
        return self.peers.get(key)
    
