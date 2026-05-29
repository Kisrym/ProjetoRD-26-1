import json

with open("config.json", "r") as f:
    config = json.load(f)

HOST = config["host"]
PORT = config["port"]
PEER_PORT = config["peer_port"]