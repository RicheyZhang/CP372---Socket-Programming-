"""
CP372 Programming Assignment - Server
Authors: Richey Zhang, Ahmed Khalid

A multi-threaded TCP chat/file server.
  - Accepts up to MAX_CLIENTS clients at a time; extra clients are refused.
  - Names clients Client01, Client02, ... in the order they are accepted.
  - Echoes messages back with " ACK", reports its cache on "status",
    lists its repository on "list", and streams files on "get <filename>".

Reply format (server -> client), used for every reply:
    <TYPE> <length>\n<exactly length bytes of payload>
    TYPE is TEXT, FILE or ERROR. For FILE the header is
    FILE <length> <filename>\n so the client knows what to save it as.
"""

import os
import socket
import threading
from datetime import datetime

HOST = "localhost"
PORT = 12345
MAX_CLIENTS = 3
REPO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "repo")

# Shared state, used by every client thread. Always access it while holding `lock`.
lock = threading.Lock()
active_clients = 0      # clients currently connected
next_client_number = 1  # number for the next accepted client
cache = {}              # name -> {"address": ..., "accepted": ..., "finished": ...}


def now():
    """Current date and time as a readable string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def send_reply(conn, reply_type, payload, filename=None):
    """Send one framed reply: a header line, then exactly len(payload) bytes."""
    if isinstance(payload, str):
        payload = payload.encode()
    if filename is None:
        header = f"{reply_type} {len(payload)}\n"
    else:
        header = f"{reply_type} {len(payload)} {filename}\n"
    conn.sendall(header.encode() + payload)


def format_cache():
    """Build the text sent back for the "status" command."""
    with lock:
        lines = [f"Active clients: {active_clients}/{MAX_CLIENTS}",
                 f"{'Name':<10}{'Address':<24}{'Accepted':<22}Finished"]
        for name, info in cache.items():
            address = f"{info['address'][0]}:{info['address'][1]}"
            finished = info["finished"] or "(still connected)"
            lines.append(f"{name:<10}{address:<24}{info['accepted']:<22}{finished}")
    return "\n".join(lines)


def list_repo():
    """Return the names of the files in the repository, sorted."""
    return sorted(f for f in os.listdir(REPO_DIR)
                  if os.path.isfile(os.path.join(REPO_DIR, f)))


def send_file(conn, filename):
    """Stream a repository file to the client, or send an ERROR reply."""
    # basename() strips folder parts so a client can't ask for "../server.py"
    safe_name = os.path.basename(filename)
    path = os.path.join(REPO_DIR, safe_name)
    if not safe_name or not os.path.isfile(path):
        send_reply(conn, "ERROR", f"File '{filename}' not found. Type 'list' to see available files.")
        return
    size = os.path.getsize(path)
    conn.sendall(f"FILE {size} {safe_name}\n".encode())   # header first
    with open(path, "rb") as f:
        while True:                       # then stream the file in 64 KB chunks
            chunk = f.read(64 * 1024)
            if not chunk:
                break
            conn.sendall(chunk)
    print(f"Sent {safe_name} ({size} bytes)")


def handle_client(conn, address, name):
    """Runs in its own thread: talks to one client until it exits or disconnects."""
    global active_clients
    reader = conn.makefile("rb")  # lets us read one line at a time
    try:
        # Step 1: tell the client its name, then wait for it to send the name back.
        send_reply(conn, "TEXT", f"WELCOME {name}")
        hello = reader.readline().decode().strip()
        print(f"{name} connected from {address[0]}:{address[1]} and says: {hello}")

        # Step 2: answer commands until the client exits.
        while True:
            line = reader.readline()
            if not line:                      # client closed without "exit"
                print(f"{name} disconnected unexpectedly")
                break
            message = line.decode().strip()
            if not message:
                continue
            command = message.lower()

            if command == "exit":
                send_reply(conn, "TEXT", f"Goodbye {name}")
                print(f"{name} sent exit")
                break
            elif command == "status":
                send_reply(conn, "TEXT", format_cache())
            elif command == "list":
                files = list_repo()
                text = "Files in repository:\n" + "\n".join(files) if files else "Repository is empty."
                send_reply(conn, "TEXT", text)
            elif command == "get" or command.startswith("get "):
                filename = message[3:].strip()   # keep the original capitalization
                if filename:
                    send_file(conn, filename)
                else:
                    send_reply(conn, "ERROR", "Usage: get <filename>")
            else:
                print(f"{name}: {message}")
                send_reply(conn, "TEXT", f"{message} ACK")
    except (ConnectionError, OSError):
        print(f"{name} connection lost")
    finally:
        # Runs no matter how the loop ended: record the finish time and free the slot.
        with lock:
            cache[name]["finished"] = now()
            active_clients -= 1
            count = active_clients
        reader.close()
        conn.close()
        print(f"{name} closed. Active clients: {count}/{MAX_CLIENTS}")


def main():
    global active_clients, next_client_number
    os.makedirs(REPO_DIR, exist_ok=True)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Lets the server restart right away on the same port after being stopped
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    print(f"Server listening on {HOST}:{PORT} (max {MAX_CLIENTS} clients)")
    print(f"Repository folder: {REPO_DIR}")

    try:
        while True:
            conn, address = server.accept()

            # Decide under the lock so two clients arriving together can't both take the last slot.
            with lock:
                if active_clients >= MAX_CLIENTS:
                    name = None
                else:
                    name = f"Client{next_client_number:02d}"
                    next_client_number += 1
                    active_clients += 1
                    cache[name] = {"address": address, "accepted": now(), "finished": None}

            if name is None:
                print(f"Refused {address[0]}:{address[1]}: server full")
                send_reply(conn, "ERROR", "Server full. Try again later.")
                conn.close()
                continue

            threading.Thread(target=handle_client, args=(conn, address, name), daemon=True).start()
    except KeyboardInterrupt:
        print("\nServer shutting down")
    finally:
        server.close()


if __name__ == "__main__":
    main()