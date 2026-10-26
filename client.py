"""
CP372 Programming Assignment - Client
Authors: Richey Zhang, Ahmed Khalid

Connects to the server, receives its name, then lets the user type commands:
    any text        -> server echoes it back with " ACK"
    status          -> server's cache of clients
    list            -> files in the server's repository
    get <filename>  -> download a file into the "downloads" folder
    exit            -> close the connection
"""

import os
import socket

HOST = "localhost"
PORT = 12345
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")


def receive_reply(reader):
    """Read one framed reply. Returns (type, payload_bytes, filename or None)."""
    header = reader.readline().decode().strip()
    if not header:
        raise ConnectionError("Server closed the connection")
    parts = header.split(" ", 2)          # e.g. ["FILE", "1234", "notes.txt"]
    reply_type, length = parts[0], int(parts[1])
    filename = parts[2] if len(parts) > 2 else None
    payload = reader.read(length)         # blocks until exactly `length` bytes arrive
    if len(payload) < length:
        raise ConnectionError("Connection lost in the middle of a reply")
    return reply_type, payload, filename


def save_file(filename, data):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    path = os.path.join(DOWNLOAD_DIR, filename)
    with open(path, "wb") as f:
        f.write(data)
    print(f"Downloaded {filename} ({len(data)} bytes) -> {path}")


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
    except ConnectionRefusedError:
        print("Could not connect. Is the server running?")
        return
    reader = sock.makefile("rb")

    try:
        # The first reply is either our name or "Server full".
        reply_type, payload, _ = receive_reply(reader)
        if reply_type == "ERROR":
            print(payload.decode())
            return
        name = payload.decode().split()[-1]       # "WELCOME Client01" -> "Client01"
        print(f"Connected. You are {name}")
        sock.sendall(f"HELLO {name}\n".encode())  # tell the server our name

        print("Commands: <any text>, status, list, get <filename>, exit")
        while True:
            message = input(f"{name}> ").strip()
            if not message:
                continue
            sock.sendall((message + "\n").encode())

            reply_type, payload, filename = receive_reply(reader)
            if reply_type == "FILE":
                save_file(filename, payload)
            elif reply_type == "ERROR":
                print("Error:", payload.decode())
            else:
                print(payload.decode())

            if message.lower() == "exit":
                break
    except (ConnectionError, OSError) as e:
        print("Disconnected:", e)
    except (KeyboardInterrupt, EOFError):
        print("\nClosing connection")
    finally:
        reader.close()
        sock.close()


if __name__ == "__main__":
    main()