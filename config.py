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