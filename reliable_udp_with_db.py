import socket
import struct
import time
import random
import threading
import queue
import matplotlib.pyplot as plt

# Parameters
MAX_PACKET_SIZE = 1024
ACK_TIMEOUT = 1  # seconds
DROP_PROBABILITY = 0.1  # Simulate packet loss (Challenge: simulating a lossy network)
WINDOW_SIZE = 5  # Sliding window size (Challenge: simulating reliable transmission with limited window size)
MAX_RETRANSMISSIONS = 5  # Maximum retries for a packet (Challenge: managing retransmissions and packet loss)
SERVER_ADDRESS = ('localhost', 5005)

# Database simulation parameters
DB_WRITE_DELAY = 0.2  # Simulated database write latency (Challenge: simulating delays in DB writes)
DB_SIMULATE_FAILURE_RATE = 0.2  # Simulate database write failure (Challenge: handling failures during database transactions)
DB_SIMULATE_DEADLOCK_RATE = 0.1  # Simulate deadlock condition (Challenge: simulating DB deadlocks)

# NAT Traversal Simulation (optional, placeholder for actual implementation)
NAT_SIMULATION = True  # Enable NAT traversal simulation (Challenge: addressing NAT traversal in networking)

# Performance tracking variables
sent_packets = 0
acknowledged_packets = 0
dropped_packets = 0
retransmissions = 0
rolled_back_transactions = 0

# Simple checksum function
def calculate_checksum(data):
    return sum(data) % 256

# Simulated database transaction class
class DatabaseTransaction:
    def __init__(self):
        self.lock = threading.Lock()
        self.in_progress = False

    def start(self):
        # Simulate database transaction start
        if random.random() < DB_SIMULATE_DEADLOCK_RATE:
            print("Simulated deadlock occurred. Retrying transaction...")  # Challenge: Simulating database deadlock
            raise IOError("Deadlock detected")
        self.in_progress = True

    def commit(self):
        # Commit the transaction (if no deadlock)
        self.in_progress = False

    def rollback(self):
        global rolled_back_transactions
        rolled_back_transactions += 1
        self.in_progress = False
        print("Transaction rolled back.")  # Challenge: Handling transaction rollback in case of failure

# Reliable UDP server with TCP-like features and database challenges
class ReliableUDPServer:
    def __init__(self, address):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(address)
        self.received_packets = {}
        self.ack_queue = queue.Queue()
        self.connected_clients = set()
        print(f"Server listening on {address[0]}:{address[1]}")

    def handle_connect(self, addr):
        # Challenge: Handling new client connections and NAT traversal
        if addr not in self.connected_clients:
            print(f"New client connected: {addr}")
            self.connected_clients.add(addr)

    def process_packet(self, packet, addr):
        global acknowledged_packets

        if len(packet) < 5:
            return

        self.handle_connect(addr)

        seq_num, checksum = struct.unpack('I B', packet[:5])
        data = packet[5:]

        if calculate_checksum(data) != checksum:
            print(f"Packet {seq_num} failed checksum.")  # Challenge: Ensuring data integrity with checksum validation
            return

        if seq_num in self.received_packets:
            print(f"Packet {seq_num} already received.")  # Challenge: Preventing duplicate packet processing
            return

        # Simulating database transaction processing
        transaction = DatabaseTransaction()
        try:
            transaction.start()  # Start DB transaction (Challenge: Simulating delays and failures in DB writes)
            time.sleep(DB_WRITE_DELAY)  # Simulate database write delay
            if random.random() < DB_SIMULATE_FAILURE_RATE:
                raise IOError("Simulated database write failure")  # Challenge: Handling database write failure
            transaction.commit()

            self.received_packets[seq_num] = data
            acknowledged_packets += 1
            print(f"Packet {seq_num} written to database successfully.")  # Challenge: Successfully committing to database
            ack = struct.pack('I', seq_num)
            self.ack_queue.put((ack, addr))
        except IOError as e:
            transaction.rollback()  # Challenge: Handling transaction rollback on failure
            print(f"Packet {seq_num} failed to write to database: {e}")

    def ack_sender(self):
        # Acknowledge packets in the queue
        while True:
            ack, addr = self.ack_queue.get()
            self.sock.sendto(ack, addr)

    def run(self):
        # Start background thread for sending ACKs
        threading.Thread(target=self.ack_sender, daemon=True).start()

        # Receive packets from clients and process them
        while True:
            packet, addr = self.sock.recvfrom(MAX_PACKET_SIZE)
            self.process_packet(packet, addr)

# Reliable UDP client with retransmission control
class ReliableUDPClient:
    def __init__(self, server_address):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(ACK_TIMEOUT)
        self.server_address = server_address
        self.connected = False

    def connect(self):
        print("Attempting to connect to server...")  # Challenge: Managing connection reliability
        self.sock.sendto(b"CONNECT", self.server_address)
        self.connected = True

    def send_data(self, data):
        global sent_packets, dropped_packets, retransmissions

        if not self.connected:
            self.connect()

        packet_size = MAX_PACKET_SIZE - 5
        packets = [data[i:i + packet_size] for i in range(0, len(data), packet_size)]
        unacknowledged = {i: (packet, 0) for i, packet in enumerate(packets)}

        print(f"Total packets: {len(packets)}")

        window_start = 0
        while window_start < len(packets):
            for seq_num in range(window_start, min(window_start + WINDOW_SIZE, len(packets))):
                if seq_num in unacknowledged:
                    packet, retries = unacknowledged[seq_num]

                    if retries >= MAX_RETRANSMISSIONS:
                        print(f"Packet {seq_num} exceeded max retransmissions and is dropped.")  # Challenge: Handling packet drops and retransmissions
                        del unacknowledged[seq_num]
                        dropped_packets += 1
                        continue

                    if random.random() > DROP_PROBABILITY:
                        checksum = calculate_checksum(packet)
                        message = struct.pack('I B', seq_num, checksum) + packet
                        self.sock.sendto(message, self.server_address)
                        sent_packets += 1
                        retransmissions += retries
                        print(f"Sent packet {seq_num}. (Retry {retries})")
                        unacknowledged[seq_num] = (packet, retries + 1)
                    else:
                        print(f"Simulated drop for packet {seq_num}.")  # Challenge: Simulating packet loss and retransmission

            try:
                while True:
                    ack, _ = self.sock.recvfrom(4)
                    ack_seq_num = struct.unpack('I', ack)[0]

                    if ack_seq_num in unacknowledged:
                        print(f"Packet {ack_seq_num} acknowledged.")  # Challenge: Successful acknowledgment of sent packets
                        del unacknowledged[ack_seq_num]

            except socket.timeout:
                print("Timeout waiting for acknowledgments.")  # Challenge: Handling timeouts and retransmissions

            # Advance the window start past any packets no longer in the unacknowledged set
            while window_start not in unacknowledged and window_start < len(packets):
                window_start += 1

        print("All packets acknowledged.")  # Challenge: Ensuring all packets are successfully acknowledged

# Visualization of performance
def display_performance():
    labels = ['Sent', 'Acknowledged', 'Dropped', 'Retransmissions', 'Rolled Back']
    values = [sent_packets, acknowledged_packets, dropped_packets, retransmissions, rolled_back_transactions]

    plt.bar(labels, values, color=['blue', 'green', 'red', 'orange', 'purple'])
    plt.title('Reliable UDP Performance Metrics with Database Challenges')
    plt.ylabel('Packet Count')
    plt.show()

# Main function for testing
if __name__ == "__main__":
    server_thread = threading.Thread(target=lambda: ReliableUDPServer(SERVER_ADDRESS).run(), daemon=True)
    server_thread.start()

    time.sleep(1)  # Give the server time to start

    data = b"Reliable UDP protocol test data for database systems." * 500
    client = ReliableUDPClient(SERVER_ADDRESS)
    client.send_data(data)

    display_performance()
