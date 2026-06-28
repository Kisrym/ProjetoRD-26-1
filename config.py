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
# ARQUIVO: config.py
# ==============================================================================

import json

with open("config.json", "r") as f:
    config = json.load(f)

HOST = config["host"]
PORT = config["port"]

PEER_NAME = config["name"]
PEER_NAMESPACE = config["namespace"]
PEER_PORT = config["peer_port"]
PEER_TTL = config["ttl"]

WEBAPP_PORT = config["webapp_port"]

PEER_RECONNECT_TRIES = config["max_peer_reconnect_attempts"]
RDZV_RECONNECT_TRIES = config["max_rdzv_reconnect_attempts"]
RDZV_DISCOVER_TRIES = config["max_rdzv_discover_attempts"]
PING_INTERVAL = config["ping_interval_seconds"]