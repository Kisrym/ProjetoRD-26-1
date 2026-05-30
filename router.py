from server import message_queue
from handlers import hello_handler, hello_ok_handler, pong_handler, pong_received_handler, send_handler, ack_received_handler, bye_handler, bye_ok_handler

def message_router(connected_peers, name, namespace):
    while True:
        event = message_queue.get()

        conn = event["conn"]
        addr = event["addr"]
        msg = event["msg"]

        msg_type = msg.get("type")

        print("Recebido:", msg)

        if msg_type == "HELLO":
            hello_handler(conn, addr, connected_peers, msg, name, namespace)

        if msg_type == "HELLO_OK":
            hello_ok_handler(conn, addr, connected_peers, msg)

        elif msg_type == "PING":
            pong_handler(conn, addr, connected_peers, msg, name, namespace)

        elif msg_type == "PONG":
            pong_received_handler(conn, addr, connected_peers, msg)

        elif msg_type == "SEND":
            send_handler(conn, addr, connected_peers, msg, name, namespace)

        elif msg_type == "ACK":
            ack_received_handler(conn, addr, connected_peers, msg)

        elif msg_type == "BYE":
            bye_handler(conn, connected_peers, msg, name, namespace)

        elif msg_type == "BYE_OK":
            bye_ok_handler(conn, addr, connected_peers, msg)