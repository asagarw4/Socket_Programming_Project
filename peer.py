import socket
import sys


def main():
    if len(sys.argv) != 5:
        print("Usage: python3 peer.py <peer-name> <peer-port> <manager-ip> <manager-port>")
        sys.exit(1)

    peer_name = sys.argv[1]
    peer_port = int(sys.argv[2])
    manager_ip = sys.argv[3]
    manager_port = int(sys.argv[4])

    # Create the peer's UDP socket
    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind the peer to its assigned port
    peer_socket.bind(("", peer_port))

    print(f"Peer {peer_name} listening on port {peer_port}")
    print(f"Manager: {manager_ip}:{manager_port}")

    # Send register command to manager
    register_message = (
        f"register {peer_name} 127.0.0.1 {manager_port} {peer_port}"
    )

    peer_socket.sendto(
        register_message.encode(),
        (manager_ip, manager_port)
    )

    print(f"Sent: {register_message}")

    # Keep the peer running
    while True:
        data, address = peer_socket.recvfrom(4096)

        message = data.decode()

        print(f"Received from {address}: {message}")


if __name__ == "__main__":
    main()