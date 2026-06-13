import json

with open("config.json", "r") as f:
    config = json.load(f)

HOST = config["host"]
PORT = config["port"]
PEER_PORT = config["peer_port"]
WEBAPP_PORT = config["webapp_port"]
PEER_RECONNECT_TRIES = config["max_peer_reconnect_attempts"]
RDZV_RECONNECT_TRIES = config["max_rdzv_reconnect_attempts"]
RDZV_DISCOVER_TRIES = config["max_rdzv_discover_attempts"]