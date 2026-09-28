import socket
import sys


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 manager.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])

    # Create a UDP socket
    manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind the manager to its port
    manager_socket.bind(("", port))

    print(f"Manager listening on UDP port {port}")

    # Store registered peers
    peers = {}

    while True:
        data, address = manager_socket.recvfrom(4096)

        message = data.decode()
        print(f"Received from {address}: {message}")

        # Split the message into pieces
        parts = message.split()

        # Handle register command
        if parts[0] == "register":
            peer_name = parts[1]
            peer_ip = parts[2]
            manager_port = int(parts[3])
            peer_port = int(parts[4])

            # Store the peer's information
            peers[peer_name] = {
                "ip": peer_ip,
                "manager_port": manager_port,
                "peer_port": peer_port,
                "state": "Free"
            }

            print(f"Registered peer: {peer_name}")
            print(f"  IP: {peer_ip}")
            print(f"  Manager port: {manager_port}")
            print(f"  Peer port: {peer_port}")
            print(f"  State: Free")

        elif parts[0] == "setup-dht":
            leader_name = parts[1]
            n = int(parts[2])
            year = int(parts[3])

            print(f"Setting up DHT with leader: {leader_name}")
            print(f"Number of peers: {n}")
            print(f"Year: {year}")


if __name__ == "__main__":
    main()