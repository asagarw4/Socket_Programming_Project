import sys
from socket import *
import threading
import json
import random
import time
from pathlib import Path
import pandas as pd
import copy

class Peer:
    def __init__(self, manager_addr):
        self.manager_addr = manager_addr
        self.m_socket = socket(AF_INET, SOCK_DGRAM)
        self.p_socket = socket(AF_INET, SOCK_DGRAM)
        self.m_handlers = {"register": self.register, "setup-dht": self.setupdht, "query-dht": self.querydht, "join-dht": self.joindht, "leave-dht": self.leavedht, "teardown-dht": self.teardowndht, "deregister":self.deregister}
        self.p_handlers = {"setid": self.setid, "store": self.store, "find-event":self.find_event, "reset-id":self.resetid, "teardown":self.teardown, "rebuild-dht":self.start_rebuild_thread}
        self.m_seq = random.randrange(2**15)
        self.p_seq = random.randrange(2**15)
        
        #personal info
        self.registration_attempted = False
        self.name = None
        self.ipv4 = None
        self.mport = None
        self.pport = None
        self.dht_id = -1   #-1 if and only if not DHT_member
        self.dht_status = None #Free, Leader, InDHT, Joining, Leaving

        #DHT meta related info
        self.DHT_setup = False
        self.DHT_num_members = None   
        self.DHT_year = None
        self.DHT_memberlist = None #The complete list of three tuples

        #DHT managment info
        self.DHT_rn_addr = None   #right neighbour addr
        self.DHT_rn_name = None
        self.DHT_size = None #smallest prime greater than 2 * l
        self.localDHT = {}  #stores a local DHT as a dict, keyed by postion

        #mport-thread info
        self.mport_thread = None
        self.mport_timeout = 10 #default setting, changed according to task before sendto
        self.mport_trials = 5 #default setting, changed according to task beofre sendto
        self.mport_msg_sent = None
        self.mport_msg_received = None
        self.mport_communication_status = None  # "ERROR", "TIMEOUT", "DONE"
        self.mport_resents = 0

        #pport-thread info
        self.pport_thread = None
        self.pport_recv_response = []
        self.pport_requests= {}
        self.pport_timeout = 7 #default setting, changed according to task before sendto (managed inside check_ACK)
        self.pport_trials = 5 #default setting, changed according to task beofre sendto (managed inside check_ACK)
        self.pport_communication_status = None  # "ERROR", "TIMEOUT", "DONE"

    def recv_from_manager(self):  #On main thread version, with max_trials to prevent program hanging
        self.m_socket.settimeout(self.mport_timeout)
        trials_left = self.mport_trials
        self.mport_communication_status = None
        while trials_left > 0:
            try: #Try and see if anything received
                msg_recv, manager_addr = self.m_socket.recvfrom(1000)
            except timeout: #Timed out, resend and continue to try receive
                self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)          
                trials_left = trials_left - 1
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
            if msg_recv["seq"] < self.mport_msg_sent["seq"]:
                continue #Older msg status code received, ignore
            if msg_recv["seq"] > self.mport_msg_sent["seq"]:
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
        name = CLI_list[1]
        ipv4 = CLI_list[2]
        mport = int(CLI_list[3])
        pport =int(CLI_list[4])

        self.mport_msg_sent = {"seq":self.m_seq, "command": "register", "peername":name, "ipv4":ipv4, "mport":mport, "pport":pport}
        self.m_seq = self.m_seq + 1

        if not self.registration_attempted: #If register retrying occurs, peer will still send to manager from the original mport
            self.m_socket.bind(("", mport)) 
            self.registration_attempted = True
        else:
            print('registration already attempted, sticking to the original mport to send the message to manager!') #keeping mport same, not rebinding

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
            if status_code == "SUCCESS":  #aligning state accordingly
                self.name = name
                self.ipv4 = ipv4
                self.mport = mport
                self.pport = pport
                self.dht_status = "Free"
                self.p_socket.bind((self.ipv4, self.pport))
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
        try:
            n = int(CLI_list[2])
            year = int( CLI_list[3])
        except ValueError:
            print('n and year must be integer values! Wrong usage, try again')
            return

        self.DHT_num_members = n
        self.DHT_year = year
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
        
        self.mport_msg_sent = {"seq":self.m_seq, "command": "setup-dht", "peername": self.name, "n": self.DHT_num_members, "year": self.DHT_year}
        self.m_seq = self.m_seq + 1
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)
        
        self.mport_timeout = 5
        self.mport_trials = 5
        self.recv_from_manager()        
        status_com = self.mport_communication_status

        if status_com == "ERROR": #TO DO: later handle case for packet corruption
            print('Errorsome message received from manager, check syntax before resending')
            return
        if status_com == "TIMEOUT":
            print(f'Manager not responding after {self.mport_timeout} timeout and {self.mport_trials} max_trials, so stopped listening')
            return
        if status_com == "DONE": 
            status_code = self.mport_msg_received["status_code"]
            print(f'Status code of {status_code} received from the manager in response to setup-dht command')
            if not status_code == "SUCCESS":
                return
            else:
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
                
                self.dht_status = "Leader"
                self.dht_id = 0
                rn_tuple = self.DHT_memberlist[1]
                self.DHT_rn_name = rn_tuple[0]
                self.DHT_rn_addr = (rn_tuple[1], int(rn_tuple[2]))

                path = Path(__file__).parent / f"details-{self.DHT_year}.csv"

                try:
                    df = pd.read_csv(path, dtype=str, keep_default_na=False)  
                except Exception as e:
                    print('Exception was thrown while opening file, quit and try again')    
                self.DHT_size = self.calc_s(len(df))

                self.set_peer_ids() #send memberlist, set id on each member

                NACK_peers = self.DHT_data(df) #load and distribute dataset using store command
                if NACK_peers:
                    print('All peer ACKs not received, but for testing going ahead with dht-complete')
                self.dhtcomplete()

    def calc_s(self, l):
        s = 2*l
        is_prime = False
        while not is_prime:
            i = 2
            s = s+1 #incrementally check for factors, starting 2l+1
            is_prime = True
            while i*i <= s:
                if s%i == 0:
                    is_prime = False
                    break
                i = i+1
        return s

    def DHT_data(self, df):
        s = self.DHT_size
        n = self.DHT_num_members
        global_DHT = {i: {} for i in range(n)}

        #storing all entries from df in global_DHT, keyed by id, and then by pos
        for row in df.itertuples(index=False):
            entry = list(row)
            pos = int(row.EVENT_ID) % s
            peer_id = pos % n

            if not global_DHT.get(peer_id):
                global_DHT.update({peer_id: {pos: [entry]}})
            elif not global_DHT[peer_id].get(pos):
                global_DHT[peer_id].update({pos: [entry]})
            else:
                global_DHT[peer_id][pos].append(entry)

        #storing the id 0 entries in the leader's local_DHT
        self.localDHT.update(global_DHT[0])

        #To avoid IP fragmentation, we set send 5 pos slots per message
        #To avoid congesting the network, we sleep for 2 milliseconds every 5 messages
        #and for 10 milliseconds after every 100 messages
        self.pport_recv_response.clear() #clear responses to make room for ACKs (~1350) from peer
        msgs_per_slot = 3
        batch_size = 10
        NACK_peers = []
        #sending (sorted by id) list of pos
        for i in range(1, n):
            pos_list = list(global_DHT[i].items()) #storing (pos, entry) tuples in a list
            p = 0
            requests = []
            while p < len(pos_list):
                v = self.DHT_memberlist[i]
                msg_sent = {"format":"request", "command": "store", "id":i, "seq": self.p_seq, "data":[]}
                msg_sent.update(data = pos_list[p:p+msgs_per_slot]) #data = [(pos1, entry1), (pos2, entry2)]
                requests.append((msg_sent, self.DHT_rn_addr))
                self.p_socket.sendto((json.dumps(msg_sent)).encode('utf-8'), self.DHT_rn_addr)
                self.p_seq = self.p_seq+1
                p = p+msgs_per_slot
                
                if (p // msgs_per_slot) % batch_size == 0:
                    time.sleep(0.001) #to avoid bursts of packets
            
            sleep_time = 2*(n-i) #wait more for 1, as ACK will be sent through right neghbours
            trials = 5
            time.sleep(4)
            print(f'PEER {i} RECORDS----------------------')
            requests, responses = self.check_response_peer("store", requests, sleep_time, trials) #Needs ACK for each individual store msg, resends the unacked msg after timeout
            if requests:
                print(f'The following {requests} were not received by peer {i}, after {sleep_time} timeouts on each of {trials} trials')
                NACK_peers.append(i)
            self.pport_recv_response.clear() #clear responses to make room for ACKs (~1350) from peer
        return NACK_peers

    def store(self, request):
         #request arrived at target node/peer id
        data = request[0]["data"]
        for pos, entries in data: #Note: lack of error checking done here
            pos = int(pos) #ensuring that pos is int, so can be directly used as key during query
            self.localDHT.update({pos: entries}) #Example {pos: [['5553873', 'KENTUCKY', '1996', 'May', 'Tornado', 'C', 'JEFFERSON', '0', '0', '0', '0', '', '0', 'F1']]}
                #pos is a key to a list of (clashing) entries, and each entry is itself a list
        response_msg = {"format":"response", "command": "store", "seq":request[0]["seq"], "id":self.dht_id} #new command for p2p, denotes ACK
        self.p_socket.sendto((json.dumps(response_msg)).encode('utf-8'), self.DHT_rn_addr)

    def dhtcomplete(self):
        self.mport_msg_sent = {"command": "dht-complete", "peername": self.name, "seq":self.m_seq}
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'),self.manager_addr)
        self.m_seq = self.m_seq + 1
        self.recv_from_manager()            
        status_com = self.mport_communication_status
        if status_com == "ERROR": #TO DO: later handle case for packet corruption
            print('Errorsome message received from manager, check syntax before resending')
        elif  status_com == "TIMEOUT":
            print(f'Manager not responding after  and {self.mport_timeout} timeout and {self.mport_trials} max_trials, so stopped listening')
            return
        elif status_com == "DONE":
            status_code = self.mport_msg_received["status_code"]
            if status_code == "SUCCESS": 
                self.DHT_setup = True
            print(f'Status code of {status_code} received from the manager in response to dht-complete command')
        else:        
            print('Unknown status communication code')
        return
        
    #return a list of impending requests, and if ret_res is True, returns respones to requests that were satisfied 
    def check_response_peer(self, command, requests, sleep_time, trials, ret_res=False): #Note, requests is a list of request, so if a single msg sent, pass: [(msg_sent, peer_addr)]
        trials_left = trials
        responses = []
        while trials_left >= 0 and requests:
            time.sleep(sleep_time)
            for response in self.pport_recv_response.copy():
                res_seq = response[0]["seq"] 
                if response[0]["command"] != command:
                    continue

                for request in requests:
                    req_seq = request[0]["seq"]

                    if req_seq == res_seq:
                        self.pport_recv_response.remove(response) #Ideally, should delete all duplicate req, but costlier
                        requests.remove(request)
                        if ret_res == True:
                            print(f"response {response} to command {command} received")
                            responses.append(response)
                        break #response matched with a request, move onto next response
                            
            trials_left = trials_left - 1
            if (trials_left < 0):
                break
            if requests:
                print(f'No response after waiting for {sleep_time}s, resending request after trial {trials - trials_left}')
                for request in requests: #resend the requests for which no response was received
                    self.p_socket.sendto((json.dumps(request[0])).encode('utf-8'), request[1])

        if requests:
            print(f'No response after waiting for {sleep_time}s, for all {trials}')
        else:
            print(f'All responses to requests for command {command} received')
        return requests, responses #Return the list of requests whose ACKs were not received

    def listen_pport(self): #called only after register command handled successfully
        while True:
            msg_recv, peer_addr = self.p_socket.recvfrom(65535)
            try:  #Something recevied, check if json convertible
                msg_recv = json.loads((msg_recv).decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError):
                print('error caused by JSON conversion')
                self.pport_communication_status = "ERROR"
                continue

            try: #for debudding, as p-thread is separate from main-thread
                self.handle_pport_msg((msg_recv, peer_addr))
            except Exception as e:
                print(f'Exception {e} was thrown while handling message {msg_recv} from {peer_addr}')

    def handle_pport_msg(self, req_or_resp):
        (msg_recv, peer_addr) = req_or_resp
        
        format = msg_recv.get('format')
        command = msg_recv['command']

        if not command or not format:
            print('Command field or Format field missing from request recevied at pport')
            return

        if format == 'response':
            response = (msg_recv, peer_addr)
            if command=="store" and self.dht_id != 0:
                self.p_socket.sendto((json.dumps(msg_recv)).encode('utf-8'), self.DHT_rn_addr)
    
            elif command!="store" and [self.ipv4, self.pport] != msg_recv["d"]:
                self.p_socket.sendto((json.dumps(msg_recv)).encode('utf-8'), self.DHT_rn_addr)
                    
            else:
                if len(self.pport_recv_response) > 5000:
                    del self.pport_recv_response[0]
                self.pport_recv_response.append(response)
            return                    
        
        if not format=='request':
            print('Unknown format msg received at pport')
            return

        request = (msg_recv, peer_addr)
        request_hash = (msg_recv['seq'], peer_addr)

        if command == 'store' and self.dht_id != msg_recv['id']:
            self.p_socket.sendto((json.dumps(msg_recv)).encode('utf-8'), self.DHT_rn_addr)
            return
        if command != 'store' and [self.ipv4, self.pport] != msg_recv['d']:
            self.p_socket.sendto((json.dumps(msg_recv)).encode('utf-8'), self.DHT_rn_addr)
            return

        if request_hash in self.pport_requests:
            print('Duplicate request found, resending response')
            self.p_socket.sendto((json.dumps(self.pport_requests[request_hash])).encode('utf-8'), peer_addr)
            return

        handler = self.p_handlers.get(command)
        if not handler:
            print('INVALID command field found in request recevied at pport')
            return

        response = handler(request) #response should be in json, and just the msg
        if len(self.pport_requests) > 20:
            del self.pport_requests[next(iter(self.pport_requests))] #delete oldest request
        if response:
            self.pport_requests.update({request_hash: response})

    def set_peer_ids(self): #called by setupdht, main-thread
        requests = []
        for i,v in enumerate(self.DHT_memberlist):
            if i == 0: #no need for leader to send id to itself
                continue
            peer_id = i
            peer_name = v[0]
            peer_addr = (v[1], v[2])
            
            p_seq = self.p_seq
            msg_to_send = {"format": "request", "command": "setid", "id":peer_id, "name":peer_name, "seq":p_seq, "d":peer_addr, "memberlist":self.DHT_memberlist, "hash_size":self.DHT_size, "year":self.DHT_year} #peer name passed as additional check
            requests.append((msg_to_send, peer_addr))
            self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), peer_addr)
            self.p_seq = self.p_seq + 1

        requests_missingACK, responses = self.check_response_peer("setid", requests, 2, 3)
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
        self.DHT_num_members = len(self.DHT_memberlist)
        self.DHT_size = msg_recv["hash_size"]
        self.DHT_year = msg_recv["year"]

        if not self.dht_id or not name or not seq or not self.DHT_memberlist or not self.DHT_size or not self.DHT_year:
            print('missing fields found in setid request, pls check')
            return
        if not name==self.name:
            print(f'My name is {self.name}, not {name}! Incorrect setid request received')
            return
        
        rn_v = self.DHT_memberlist[(self.dht_id+1)%self.DHT_num_members]
        self.DHT_rn_name = rn_v[0]
        self.DHT_rn_addr = (rn_v[1], rn_v[2])
        ACK_msg = {"format":"response", "command":"setid", "seq":seq, "d":leader_addr}
        self.p_socket.sendto((json.dumps(ACK_msg)).encode('utf-8'), leader_addr)
        return

    def querydht(self, CLI_list): #running on main thread
        if not len(CLI_list) == 3:
            print('querydht needs 2 command line parameters')
            return
        name = CLI_list[1]
        if not name == self.name:
            print(f'My name is {self.name} in the manager register!')
            return
        try:
            event_id = int(CLI_list[2])
        except ValueError:
            print('event-id must be an integer value! Wrong usage, try again')
            return
        
        self.mport_msg_sent = {"seq":self.m_seq, "command": "query-dht", "peername":self.name, "event-id":event_id}
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)
        self.m_seq = self.m_seq + 1

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
            print(f'Status code of {status_code} received from the manager in response to query command')
            if status_code == "SUCCESS": 
                S_peer_tuple = self.mport_msg_received["peer"]
                S_peer_addr = (S_peer_tuple[1], S_peer_tuple[2])
                msg_to_send = {"format":"request", "command":"find-event", "seq":self.p_seq, "event-id":event_id, "id-seq":[], "Saddr": (self.ipv4, self.pport), "d":S_peer_addr} #Saadr: sending peer's address
                self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), S_peer_addr)
                self.p_seq = self.p_seq + 1
                print(f'find-event request sent by querer {self.name} to peer {S_peer_tuple[0]}')
                requests, responses = self.check_response_peer("find-event", [(msg_to_send, S_peer_addr)], 5, 1, ret_res=True)
                if requests or not responses:
                    print('No query response not receive check pport connections and try again')
                    return
                if len(responses) > 1:
                    print('Error in check_p_handler functionality, more than one response recevied')
                response = responses[0]
                if not response[0]["status_code"] == "SUCCESS":
                    print(f"Storm event {event_id} not found in the DHT.")
                else:
                    entry = response[0]['entry']
                    cols = [
    "EVENT_ID", "STATE", "YEAR", "MONTH_NAME", "EVENT_TYPE",
    "CZ_TYPE", "CZ_NAME", "INJURIES_DIRECT", "INJURIES_INDIRECT",
    "DEATHS_DIRECT", "DEATHS_INDIRECT", "DAMAGE_PROPERTY",
    "DAMAGE_CROPS", "TOR_F_SCALE",]
                    if len(cols) != len(entry):
                        print("Not ALL fields in entry received")
                    for i in range(len(cols)):
                        print(f'{cols[i]}: {entry[i]}\n')

    def find_event(self, request): #running on p-thread
        msg_recv, peer_addr = request
        event_id = int(msg_recv["event-id"])
        Saddr = tuple(msg_recv["Saddr"])
        id_seq = msg_recv["id-seq"] #list of ids of nodes visited during query
        seq = msg_recv["seq"] #current seq-id at pport
        target_entry = None
        n = self.DHT_num_members

        if not event_id or not Saddr or not seq:
            print(f'fields are missing from find-event query message at peer {self.name}, with id {self.dht_id}')
            return

        if not id_seq: #If this is the starting node
            id_seq.append(self.dht_id)

        pos = event_id % self.DHT_size
        peer_id = pos % self.DHT_num_members

        if len(id_seq) > 0 or (self.dht_id == peer_id):
            print(f'query received at random target node {self.dht_id}, checking if entry stored at calculated pos')
            entries = self.localDHT.get(pos)

            if entries:
                for entry in entries: #check if any entries at pos correspond to the query event-id
                    if int(entry[0]) == event_id:
                        target_entry = entry
                        break
                
            if target_entry:    
                msg_to_send = {"format":"response", "status_code":"SUCCESS", "command":"find-event", "seq":seq, "id-seq":id_seq, "d":Saddr, "entry":target_entry} #Saadr: sending peer's address
                self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), Saddr)
                print(f'query entry found at id {self.dht_id}, sending success msg {msg_to_send} to querer at address {Saddr}')
                return
            
        if len(id_seq) >= n:
            print(f"Storm event {event_id} not found in the DHT.")
            msg_to_send = {"format":"response", "status_code":"FAILURE", "command":"find-event", "seq":seq, "id-seq":id_seq, "d":Saddr}
            self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), Saddr)
            return
            
        I = set(range(n)) - set(id_seq)
        next = random.choice(tuple(I))
        id_seq.append(next)

        next_tuple = self.DHT_memberlist[next]
        next_name = next_tuple[0]
        next_addr = (next_tuple[1], next_tuple[2])
        
        msg_to_send = copy.deepcopy(msg_recv)
        msg_to_send.update({"id-seq":id_seq})
        msg_to_send.update(d=next_addr)
        print(f'Forwarding query to random node id {next} with name {next_name}')
        self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), next_addr)
        return

    def joindht(self, CLI_list): #running on main thread
        if not len(CLI_list) == 2:
            print('join-dht needs 1 command line parameters')
            return
        name = CLI_list[1]
        if not name == self.name:
            print(f'My name is {self.name} in the manager register!')
            return
        self.mport_msg_sent = {"seq":self.m_seq, "command": "join-dht", "peername":self.name}
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)
        self.m_seq = self.m_seq + 1

        self.recv_from_manager()        
        status_com = self.mport_communication_status
        if status_com != "DONE":
            print('Error in communication with manager, quitting')
            return
        status_code = self.mport_msg_received["status_code"]
        print(f'Status code of {status_code} received from the manager in response to join-dht command')
        if status_code != "SUCCESS": 
            print('Failed to join DHT, quitting')
            return

        rn_tuple = self.mport_msg_received["peer"]
        self.DHT_rn_name = rn_tuple[0]
        self.DHT_rn_addr = (rn_tuple[1], rn_tuple[2])

        msg_recv = self.initiate_resetid(op="join")
        if not msg_recv or  not msg_recv["status_code"] == "SUCCESS":
            print('Error in resetid functionality, restart')
            return
        self.DHT_setup = True
        self.dht_status = "InDHT"
        self.dht_id = msg_recv["id"]
        self.DHT_year = msg_recv["year"]
        self.DHT_size = msg_recv["hash_size"]
        self.DHT_memberlist = msg_recv["memberlist"]
        self.DHT_num_members = len(self.DHT_memberlist)
        if not self.initiate_teardown(self.DHT_num_members-2, op="join"):
            print('No response returned by teardown, unsure whether operation was completed')
            return
        self.initiate_rebuild()

#Issue resestid
    def initiate_resetid(self, op): #main thread helper
        msg_to_send = {"format":"request", "command":"reset-id", "seq":self.p_seq, "id":0, "op":"", "S_state": (self.name, self.ipv4, self.pport), "d":self.DHT_rn_addr}
        if op=="join":
            msg_to_send.update(op="join")
        elif op=="leave":
            msg_to_send.update(op="leave")
        else:
            print('Unknown op')
            return
        self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), self.DHT_rn_addr)
        self.p_seq = self.p_seq + 1
        print(f'resetid request sent by {op}er {self.name} to peer {self.DHT_rn_name}')
        requests, responses = self.check_response_peer("reset-id", [(msg_to_send, self.DHT_rn_addr)], 5, 0, ret_res=True)
        if requests or not responses:
            print('No resetid response not receive check pport connections and try again')
            return
        if len(responses) > 1:
            print('Error in check_p_handler functionality, more than one response recevied')
        response = responses[0]
        return response[0]


#Issue teardown 
    def initiate_teardown(self, stop, op): #main thread helper
        msg_to_send = {"format":"request", "command":"teardown", "seq":self.p_seq, "stop":stop, "op":op, 'Saddr':(self.ipv4, self.pport), "d":self.DHT_rn_addr} 
        self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), self.DHT_rn_addr)
        self.p_seq = self.p_seq + 1
        print(f'teardown request sent by {self.name} to peer {self.DHT_rn_name}')
        requests, responses = self.check_response_peer("teardown", [(msg_to_send, self.DHT_rn_addr)], 5, 1, ret_res=True)
        if requests or not responses:
            print('No teardown response received, check pport connections and try again')
            return
        if len(responses) > 1:
            print('Error in check_p_handler functionality, more than one response recevied')
        response = responses[0]
        return response[0]

#Issue rebuild-dht
    def initiate_rebuild(self): #main thread helper
        msg_to_send = {"format":"request", "command":"rebuild-dht", "seq":self.p_seq, "d":self.DHT_rn_addr} 
        self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), self.DHT_rn_addr)
        self.p_seq = self.p_seq + 1
        print(f'rebuild-dht request sent by {self.name} to peer {self.DHT_rn_name}')
        n = self.DHT_num_members
        stime = n*(n+1)
        requests, responses = self.check_response_peer("rebuild-dht", [(msg_to_send, self.DHT_rn_addr)], stime+50, 0, ret_res=True)
        if requests or not responses:
            print('No rebuild response received, check pport connections and try again')
            return
        if len(responses) > 1:
            print('Error in check_p_handler functionality, more than one response recevied')
        status_code = responses[0][0]["status_code"]
        if not status_code == "SUCCESS":
            print('Rebuild of dht unsuccessful, quitting')
            return
        self.mport_msg_sent = {"command": "dht-rebuilt", "peername": self.name, "leadername": self.DHT_rn_name, "seq":self.m_seq}
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'),self.manager_addr)
        self.m_seq = self.m_seq + 1
        self.recv_from_manager()            
        status_com = self.mport_communication_status
        if status_com != "DONE": #TO DO: later handle case for packet corruption
            print('Error in communication with manager, quitting')
            return
        status_code = self.mport_msg_received["status_code"]
        print(f'Status code of {status_code} received from the manager in response to dht-rebuilt command')

    def leavedht(self, CLI_list): #running on main thread
        if not len(CLI_list) == 2:
            print('join-dht needs 1 command line parameters')
            return
        name = CLI_list[1]
        if not name == self.name:
            print(f'My name is {self.name} in the manager register!')
            return
        self.mport_msg_sent = {"seq":self.m_seq, "command": "leave-dht", "peername":self.name}
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)
        self.m_seq = self.m_seq + 1

        self.recv_from_manager()        
        status_com = self.mport_communication_status
        if status_com != "DONE":
            print('Error in communication with manager, quitting')
            return
        status_code = self.mport_msg_received["status_code"]
        print(f'Status code of {status_code} received from the manager in response to leave-dht command')
        if status_code != "SUCCESS": 
            print('Failed to leave DHT, quitting')
            return

        msg_recv = self.initiate_resetid(op="leave")
        if not msg_recv or not msg_recv["status_code"] == "SUCCESS":
            print('Error in resetid functionality, restart')
            return
        self.dht_status = "Free"
        self.dht_id = -1
        if not self.initiate_teardown(self.DHT_num_members-2, op="leave"):
            print('No response returned by teardown, unsure whether operation was completed')
            return
#Reset state of leaving peer, leaving it with knowledge of only rn_addr, rn_name, manager
        self.localDHT.clear()
        self.DHT_num_members = self.DHT_num_members - 1
        self.initiate_rebuild()
        self.DHT_setup = None #Only members know about DHT state
        self.DHT_memberlist = None
        self.DHT_size = None
        self.DHT_year = None
        self.DHT_num_members = None 
        self.DHT_rn_addr = None   #right neighbour addr
        self.DHT_rn_name = None

    def resetid(self, request): #running on p-thread
        #needs an id to specify which id to set self.dht_id to
        #re-orders it's DHT_memberlist according to id reset
        #if operation=="join", adds the (name, ipv4, pport) tuple in field S_state to member_list
        #if operation=="leave", remove the tuple from member_list
        #if self.dht_id is n-1, and op is "join", adds joiner as rn, send 'SUCCESS' to joiner along with DHT_size, year and re-ordered member-list
        #if self.dht_id is n-1, and op is "leave", adds node id 0 in reordered list as rn and send 'SUCCESS' to leaver
        #if self.dht-id is 0, sets self.dht_status as 'Leader'
        msg_recv, peer_addr = request
        S_state = tuple(msg_recv["S_state"]) #state of joiner or leaver
        id = int(msg_recv["id"]) #id to set
        seq = msg_recv["seq"] 
        op = msg_recv["op"] #join or leave
        n = self.DHT_num_members

        #reorder member list
        leader_id = self.dht_id - id
        if self.dht_id == 0:
            self.dht_status = 'InDHT'
        if leader_id < 0:
            leader_id = n + leader_id
        
        memberlist = self.DHT_memberlist[leader_id:]
        memberlist.extend(self.DHT_memberlist[:leader_id])
        self.DHT_memberlist = memberlist
        
        #append to or delete from memberlist
        if op == "join":
            self.DHT_memberlist.append(S_state)
            self.DHT_num_members = n + 1
            self.dht_id = id
            if id == n-1: #if I am supposed to be joiner's right neighbour
                self.DHT_rn_name = S_state[0]
                self.DHT_rn_addr = (S_state[1], S_state[2])
                msg_to_send = {"format":"response", "status_code":"SUCCESS", "command":"reset-id", "seq":seq, "id":n, "d":self.DHT_rn_addr, "s":(self.ipv4, self.pport), "memberlist":self.DHT_memberlist, "year":self.DHT_year, "hash_size":self.DHT_size} #Saadr: sending peer's address
                self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), self.DHT_rn_addr)
                print(f'resetid completed at id {self.dht_id}, sending success msg {msg_to_send} to joiner {self.DHT_rn_name} at address {self.DHT_rn_addr}')
                return
        if op == "leave":
            leaving_peer_state = self.DHT_memberlist.pop() #left neighbour becomes last element 
            self.DHT_num_members = self.DHT_num_members - 1
            self.dht_id = id
            if id == n-2: #arrived at the leaving peer's left neighbour
                leader_state = self.DHT_memberlist[0]
                self.DHT_rn_name = leader_state[0]
                self.DHT_rn_addr = (leader_state[1], leader_state[2])
                leaving_peer_addr = (leaving_peer_state[1], leaving_peer_state[2])
                msg_to_send = {"format":"response", "status_code":"SUCCESS", "command":"reset-id", "seq":seq, "d":leaving_peer_addr, "s":(self.ipv4, self.pport)}
                self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), leaving_peer_addr)
                print(f'resetid completed at id {self.dht_id}, sending success msg to leaver {leaving_peer_state[0]} at address {leaving_peer_addr}')
                return
            
        if id == 0:
            self.dht_status = "Leader"
        id = id + 1
        msg_recv.update(id=id)
        msg_recv.update(d=self.DHT_rn_addr)
        self.p_socket.sendto((json.dumps(msg_recv).encode('utf-8')), self.DHT_rn_addr)
        print(f'resetid {id-1} completed at id {self.dht_id}, forwarding resetid {id} to peer {self.DHT_rn_name} at address {self.DHT_rn_addr}')

    def teardown(self, request):
        msg_recv, peer_addr = request
        self.localDHT.clear()
        seq = msg_recv["seq"]
        stop = int(msg_recv["stop"])
        op = msg_recv["op"]
        Saddr = tuple(msg_recv["Saddr"])
        if self.dht_id == stop:
            msg_to_send = {"format":"response", "status_code":"SUCCESS", "command":"teardown", "seq":seq, "d":Saddr, "s":(self.ipv4, self.pport)}
            self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), Saddr)
            print(f'teardown completed at id {self.dht_id}, sending success msg to leaver at address {Saddr}')
        else:
            msg_recv.update(d=self.DHT_rn_addr)
            self.p_socket.sendto((json.dumps(msg_recv).encode('utf-8')), self.DHT_rn_addr)
            print(f'teardown completed at id {self.dht_id}, forwarding teardown to peer {self.DHT_rn_name} at address {self.DHT_rn_addr}')

        if op == "teardown":
            self.dht_status = "Free"
            self.dht_id = -1
            self.DHT_setup = None #Only members know about DHT state
            self.DHT_memberlist = None
            self.DHT_size = None
            self.DHT_year = None
            self.DHT_num_members = None 
            self.DHT_rn_addr = None   #right neighbour addr
            self.DHT_rn_name = None

    def start_rebuild_thread(self, request):
        self.rebuild_thread = threading.Thread(target=self.rebuilddht, args=(request,), daemon=True)
        self.rebuild_thread.start()

    def rebuilddht(self, request):
        msg_recv, peer_addr = request
        seq = msg_recv["seq"]
        if self.dht_id != 0:
            print('I am not leader, I cannot handle this command request!')
            return
        msg_to_send = {"format":"response", "command": "rebuild-dht", "seq":seq, "status_code":"", "d":peer_addr}
        path = Path(__file__).parent / f"details-{self.DHT_year}.csv"
        df = pd.read_csv(path, dtype=str, keep_default_na=False)             
        self.DHT_size = self.calc_s(len(df))
        NACK_peers = self.DHT_data(df) #load and distribute dataset using store command
        if NACK_peers:
            print('All peer ACKs not received, so DHT not fully REBUILT')
            msg_to_send.update(status_code="FAILURE")
            self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), peer_addr)
        else:
            print('Signalling sender of completion')
            msg_to_send.update(status_code="SUCCESS")
            self.p_socket.sendto((json.dumps(msg_to_send).encode('utf-8')), peer_addr)

    def teardowndht(self, CLI_list): #main thread
        if not len(CLI_list) == 2:
            print('teardown-dht needs 1 command line parameters')
            return
        name = CLI_list[1]
        if not name == self.name:
            print(f'My name is {self.name} in the manager register!')
            return
        self.mport_msg_sent = {"seq":self.m_seq, "command": "teardown-dht", "peername":self.name}
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)
        self.m_seq = self.m_seq + 1
        
        self.recv_from_manager()        
        status_com = self.mport_communication_status
        if status_com != "DONE":
            print('Error in communication with manager, quitting')
            return
        status_code = self.mport_msg_received["status_code"]
        print(f'Status code of {status_code} received from the manager in response to teardown-dht command')
        if status_code != "SUCCESS": 
            print('Failed to teardown DHT, quitting')
            return
        if not self.initiate_teardown(self.DHT_num_members-1, op="teardown"):
            print('No response returned by teardown, unsure whether operation was completed')
            return
        self.localDHT.clear()
        self.dht_status = "Free"
        self.dht_id = -1
        self.DHT_setup = None #Only members know about DHT state
        self.DHT_memberlist = None
        self.DHT_size = None
        self.DHT_year = None
        self.DHT_num_members = None 
        self.DHT_rn_addr = None   #right neighbour addr
        self.DHT_rn_name = None

        self.mport_msg_sent = {"command": "teardown-complete", "peername": self.name, "seq":self.m_seq}
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'),self.manager_addr)
        self.m_seq = self.m_seq + 1
        self.recv_from_manager()            
        status_com = self.mport_communication_status
        if status_com != "DONE": #TO DO: later handle case for packet corruption
            print('Error in communication with manager, quitting')
            return
        status_code = self.mport_msg_received["status_code"]
        print(f'Status code of {status_code} received from the manager in response to teardown-complete command')

    def deregister(self, CLI_list): #on main thread
        if not len(CLI_list) == 2:
            print('teardown-dht needs 1 command line parameters')
            return
        name = CLI_list[1]
        if not name == self.name:
            print(f'My name is {self.name} in the manager register!')
            return
        self.mport_msg_sent = {"seq":self.m_seq, "command": "deregister", "peername":self.name}
        self.m_socket.sendto((json.dumps(self.mport_msg_sent)).encode('utf-8'), self.manager_addr)
        self.m_seq = self.m_seq + 1
        
        self.recv_from_manager()        
        status_com = self.mport_communication_status
        if status_com != "DONE":
            print('Error in communication with manager, quitting')
            return
        status_code = self.mport_msg_received["status_code"]
        print(f'Status code of {status_code} received from the manager in response to deregister command')
        if status_code != "SUCCESS":
            print('Failed to deregister, staying')
        else:
            print('deregistration successful, shutting down process')
            sys.exit(0)
       
    def read_CLI(self):
        print("Peer process started. Please enter the message to be sent to the manager")
        while True:
            try:
                CLI_instr = input("peer> ") #returns on a newline
            except(EOFError):
                print('EOF encountered, exiting CLI mode')
                break #exit current stdin read attempt if EOF is encountered

            CLI_list = CLI_instr.split()
            if not CLI_list: #If just \n was entered and list is []
                continue

            command = CLI_list[0]
            handler = self.m_handlers.get(command)
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

if __name__ == "__main__":
    main()



            



            

            
