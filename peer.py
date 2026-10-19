import sys
from socket import *
import threading
import json
import random
import time
from pathlib import Path
import pandas as pd

class Peer:
    def __init__(self, manager_addr):
        self.manager_addr = manager_addr
        self.m_socket = socket(AF_INET, SOCK_DGRAM)
        self.p_socket = socket(AF_INET, SOCK_DGRAM)
        self.m_handlers = {"register": self.register, "setup-dht": self.setup_dht, "dht-complete": self.dht_complete}
        self.p_handlers = {"setid": self.setid}
        self.m_seq = random.randrange(2**15)
        self.p_seq = random.randrange(2**15)
        
        #personal info
        self.ipv4 = None
        self.mport = None
        self.pport = None
        self.dht_id = -1   #-1 if and only if not DHT_member
        self.dht_status = None #Free, Leader, Member, Joining, Leaving

        #DHT meta related info
        self.DHT_setup = False
        self.DHT_num_members = None   
        self.DHT_year = None
        self.DHT_memberlist = None #The complete list of three tuples

        #DHT managment info
        self.DHT_rn_addr = None   #right neighbour addr
        self.DHT_rn_name = None
        self.DHT_size = None
        self.DHT_s = None #smallest prime greater than 2 * l
        self.localDHT = {}  #stores a local DHT as a dict, keyed by postion

        #mport-thread info
        self.mport_thread = None
        self.mport_timeout = 100 #default setting, changed according to task before sendto
        self.mport_trials = 5 #default setting, changed according to task beofre sendto
        self.mport_msg_sent = None
        self.mport_msg_received = None
        self.mport_communication_status = None  # "ERROR", "TIMEOUT", "DONE"
        self.mport_resents = 0

        #pport-thread info
        self.pport_thread = None
        self.pport_recv_response = []
        self.pport_requests= {}
        self.pport_timeout = 100 #default setting, changed according to task before sendto (managed inside check_ACK)
        self.pport_trials = 5 #default setting, changed according to task beofre sendto (managed inside check_ACK)
        self.pport_communication_status = None  # "ERROR", "TIMEOUT", "DONE"

    def recv_from_manager(self):  #On main thread version, with max_trials to prevent program hanging
        self.m_socket.settimeout(self.mport_timeout)
        trials_left = self.mport_trials

        while trials_left > 0:
            try: #Try and see if anything received
                msg_recv, manager_addr = self.m_socket.recvfrom(1000)
            except TimeoutError: #Timed out, resend and continue to try receive
                self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)          
                tries_left = tries_left - 1
                continue

            if (manager_addr != self.manager_addr): #check if msg was sent by manager
                print('Uknown sender msg received at m-port, discarding msg, listening for manager')
                continue

            try:  #Something recevied from manager, check if json convertible
                msg_recv = json.loads((msg_recv).decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError):
                print('error caused by JSON conversion')
                self.mport_communication_status = "ERROR"
                return

            #JSON convertible, check if same seq
            if msg_recv[self.m_seq] < self.mport_msg_sent["seq"]:
                continue #Older msg status code received, ignore
            if msg_recv[self.m_seq] > self.mport_msg_sent["seq"]:
                print(f'Return message from manager missed, latest message: {msg_recv}')
                return
            #seq check passed, correct msg received, wrap up
            self.mport_msg_received = msg_recv 
            self.mport_communication_status = 'DONE'
            self.m_socket.settimeout(None)
            return
        
        #break loop only occurs if no msg received within timeout limit even after max_trials
        print('Max wait time limit reached')
        self.mport_communication_status = 'TIMEOUT' 
        return

    def register(self, CLI_list):
        if not len(CLI_list) == 5:
            print('register needs four command line parameters')
            return
        self.name = CLI_list[1]
        self.ipv4 = CLI_list[2]
        self.mport = int(CLI_list[3])
        self.pport =int(CLI_list[4])

        self.mport_msg_sent = {"seq":self.m_seq, "command": "register", "peername":self.name, "ipv4":self.ipv4, "mport":self.mport, "pport":self.pport}
        self.m_seq = self.m_seq + 1
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)

        self.recv_from_manager()        
        status_com = self.mport_communication_status
        if status_com == "ERROR": #TO DO: later handle case for packet corruption
            print('Errorsome message received from manager, check syntax before resending')
            return
        if  status_com == "TIMEOUT":
            print(f'Manager not responding after  and {self.mport_timeout} timeout and {self.mport_trials} max_trials, so stopped listening')
            return
        if status_com == "DONE":
            status_code = self.mport_msg_received["status_code"]
            if status_code == "SUCCESS": 
                self.dht_status = "Free"
                self.pport_thread = threading.Thread(target=self.listen_pport, daemon=True) #general peer starts listening at pport
                self.pport_thread.start()
            print(f'Status code of {status_code} received from the manager in response to register command')
            return
        print('Unknown status communication code')
        return


    def setupdht(self, CLI_list):
        if not len(CLI_list) == 4:
            print('setup-dht needs three command line parameters')
            return
        peer_name = CLI_list[1]
        self.DHT_num_members = CLI_list[2]
        self.DHT_year = int(CLI_list[3])

        if not peer_name == self.name: #Refuse to make an irrelevant request, confusing state
            print(f'My name is {self.name} in the manager register!')
            return

        if not (self.DHT_year >= 1950 and self.DHT_year <= 2019):
            print('Year given out of bounds, pls enter between 1950 and 2019')
            return

        #Temporary if
        if not self.DHT_num_members < 20:
            print("Uptil 250 members are allowed, but restrict till 20 for prototype")
            return
        
        self.mport_msg_sent = {"seq":self.m_seq, "command": "setupdht", "peername": self.name, "n": self.DHT_num_members, "year": self.DHT_year}
        self.m_seq = self.m_seq + 1
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)
        
        self.mport_timeout = 100
        self.mport_trials = 5
        self.recv_from_manager()        
        status_com = self.mport_communication_status

        if status_com == "ERROR": #TO DO: later handle case for packet corruption
            print('Errorsome message received from manager, check syntax before resending')
            return
        if status_com == "TIMEOUT":
            print(f'Manager not responding after  and {self.pport_timeout} timeout and {self.pport_trials} max_trials, so stopped listening')
            return
        if status_com == "DONE": 
            status_code = self.mport_msg_received["status_code"]
            print(f'Status code of {status_code} received from the manager in response to register command')
            if not status_code == "SUCCESS":
                return
            else:
                self.dht_status = "Leader"
                memberlist = self.mport_msg_received["memberlist"] 
                if not memberlist: #Check existence of member list
                    print('Member list missing from setupdht response')
                    return
                self.DHT_memberlist = [tuple(item) for item in memberlist]

                n = len(self.DHT_memberlist)
                if not n == self.DHT_num_members: #check size of member list
                    print("ALL DHT members not received")
                    return
                
                if not self.DHT_memberlist[0] == (self.name, self.ipv4, self.pport): #check if leader tuple is correctly set
                    print(f"Member list first tuple incorrect: {self.DHT_memberlist[0]}")
                    return
                self.dht_id = 0
                self.set_peer_ids()
                self.DHT_data(self.DHT_num_members, self.DHT_year)
            return
        print('Unknown status communication code')
        return

    def is_prime(self, n):
        if n < 2:
            return False
        i = 2
        while i * i <= n:
            if n % i == 0:
                return False
        i += 1
        return True
    
    def next_prime_after(self, x):
        n = x + 1
        while not self.is_prime(n):
            n += 1
        return n

    def DHT_data(self, n, year):
        path = Path.home() / "Downloads" / "details-1996.csv"
        df = pd.read_csv(path)
        records = df.values.tolist()

        l = len(df)
        s = self.next_prime_after(2*l)
        for event_id in df["event_id"].values:
            pos = int(event_id) % s
            peer_id = pos % n



        
        







        
        

    def check_response_peer(self, requests, sleep_time, trials): #Note, requests is a list of request, so if a single msg sent, pass: [(msg_sent, peer_addr)]
        trials_left = trials
        while trials_left > 0 and requests:
            time.sleep(sleep_time)
            for response in self.pport_recv_response:
                res_seq = response[0]["seq"] 
                res_addr = response[1]
                for request in requests:
                    req_seq = request[0]["seq"]
                    req_addr = request[1]
                
                    if (req_seq, req_addr) == (res_seq, res_addr): #peer_addr just as an additional check
                        print(f"response {response} received")
                        self.pport_recv_response.remove(response) #Ideally, should delete all duplicate req, but costlier
                        requests.remove(request)

            trials_left = trials_left - 1
            print(f'No response after waiting for {sleep_time}s, resending request, trial {trials - trials_left}')
            for request in requests: #resend the responses for which no ACK was received
                self.p_socket.sendto((json.dumps(request[0])).encode('utf-8'), request[1])
        return requests #Return the list of requests whose ACKs were not received

    def listen_pport(self): #called only after register command handled successfully
        self.p_socket.bind(self.ipv4, self.pport)
        while True:
            msg_recv, peer_addr = self.p_socket.recvfrom(1000)
            try:  #Something recevied, check if json convertible
                msg_recv = json.loads((msg_recv).decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError):
                print('error caused by JSON conversion')
                self.pport_communication_status = "ERROR"
                continue

            format = msg_recv['format']
            if (format == 'ACK' or format == 'response'):
                response = (msg_recv, peer_addr)
                if len(self.pport_recv_response) > 20:
                    del self.pport_recv_response[0]
                self.pport_recv_response.append(response)
                continue
            if not format=='request':
                print('Unknown format msg received at pport')
                continue

            request = (msg_recv, peer_addr)
            if request in self.pport_requests:
                print('Duplicate request found, resending response')
                self.p_socket.sendto((json.dumps(self.pport_requests.get(request))).encode('utf-8'), peer_addr)
                continue

            command = msg_recv['command']
            if not command:
                print('Command field missing from request recevied at pport')
                continue

            handler = self.p_handlers[command]
            if not handler:
                print('INVALID command field found in request recevied at pport')
                continue

            response = handler(request) #response should be in json
            if len(self.pport_requests) > 20:
                self.pport_requests.popitem()
            self.pport_requests.update(request=response)

    def set_peer_ids(self): #called by setupdht, main-thread
        requests = []
        for i,v in enumerate(self.DHT_memberlist):
            if i == 0: #no need for leader to send id to itself
                continue
            peer_id = i
            peer_name = v[0]
            peer_addr = (v[1], v[2])
            
            p_seq = self.p_seq
            msg_to_send = {"format": "request", "command": "setid", "id":peer_id, "name":peer_name, "seq":p_seq, "memberlist":self.DHT_memberlist} #peer name passed as additional check
            requests.append((msg_to_send, peer_addr))
            self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), peer_addr)
            self.p_seq = self.p_seq + 1

        requests_missingACK = self.check_response_peer(requests, 50, 3)
        for request in requests_missingACK:
            print(f'ACK for request {request} to member id {request[0]["id"]} not received, pls recheck')
        return

    def setid(self, request): #called by listen_pport
        if self.dht_id == 0:
            print('I am leader, I am not supposed to receive a setid request!')
            return
        (msg_recv, leader_addr) = request
        self.dht_id = msg_recv["id"]
        name = msg_recv["name"]
        seq = msg_recv["seq"]
        self.DHT_memberlist = msg_recv["memberlist"]

        if not self.dht_id or not name or not seq or not self.DHT_memberlist:
            print('missing fields found in setid request, pls check')
            return
        if not name==self.name:
            print(f'My name is {self.name}, not {name}! Incorrect setid request received')
            return
        
        rn_v = self.DHT_memberlist[self.dht_id+1]
        self.DHT_rn_name = rn_v[0]
        self.DHT_rn_addr = (rn_v[1], rn_v[2])
        ACK_msg = {"format":"response", "seq":seq}
        self.p_socket.sendto((json.dumps(ACK_msg)).encode('utf-8'), leader_addr)
        return
        
    def read_CLI(self):
        while True:
            try:
                print("Peer process started. Please enter the message to be sent to the manager")
                CLI_instr = input("peer> ") #returns on a newline
            except(EOFError):
                print('EOF encountered, exiting CLI mode')
                break #exit current stdin read attempt if EOF is encountered

            CLI_list = CLI_instr.split()
            if not CLI_list: #If just \n was entered and list is []
                continue

            command = CLI_list[0]
            handler = self.handlers.get(command)
            if not handler:
                print(f"Invalid command: {command} was passed")
                continue
            handler(CLI_list)
            #command handled, relevant output printed to stdout
            #Assumption: To discontinue, user will print Ctrl+D

def main(): #module level function (outside Peer class)
    if not len(sys.argv) == 3:
        print("Atleast 2 command line parameters expected!")
        sys.exit(1)
    manager_addr = (sys.argv[1], int(sys.argv[2]))
    peer = Peer(manager_addr) #make an instance of peer
    peer.read_CLI()

        
            
