from socket import*
import sys
import json
import random

class Manager:
    def __init__(self, port):
        self.listening_port = int(port)
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.handlers = {"register": self.register, "setup-dht": self.setupdht, "dht-complete": self.dhtcomplete, "query-dht": self.querydht, "join-dht": self.joindht, "dht-rebuilt": self.dhtrebuilt, "leave-dht": self.leavedht, "teardown-dht": self.teardowndht, "teardown-complete": self.teardowncomplete, "deregister":self.deregister}
        
        #Important DHT state info
        self.DHT_setup = "TO DO"
        self.peer_joining = None
        self.peer_leaving = None
        self.tearing_dht = False
        
        #DHT meta info
        self.leader_name = ""
        self.leader_addr = ""
        self.peer_state = {} # keys are peernames, values are lists [state, IPV4 addr, m-port, p-port]
        self.DHT_members = [] #list to be sent to the leader during setup-dht
        self.DHT_num_members = 0
        self.DHT_year = None

        #manager port tracking
        self.requests = {}
        self.clientAddr = ""
        self.msg_recv = None #latest msg received at listening port
        self.msg_to_send = None #latest message to be sent
        self.mseq = -1

    def register(self):
        peer_name = self.msg_recv.get("peername")
        ipv4 = self.msg_recv.get("ipv4") #string object, "100.10.10.1" or host name
        mport = self.msg_recv.get("mport")
        pport = self.msg_recv.get("pport")

        self.msg_to_send = {"status_code": "", "seq":self.mseq}
    #check if all the fields are present
        if not peer_name or not ipv4 or not mport or not pport: 
            print('required register msg fields not present')
            self.msg_to_send.update(status_code="INVALID")
            self.send()
            return
    #ensure mport and pport are integers
        mport = int(mport)
        pport = int(pport)
    
    #ensure uniqueness of peer name
        if (self.peer_state.get(peer_name)):  
            self.msg_to_send.update(status_code="DUPLICATE")
            print('PEER ALREADY REGISTERED!')
            self.send()
            return

    #ensure uniqueness of pport and mport
        for v in self.peer_state.values():
            if v[2] == mport or v[3] == pport:
                self.msg_to_send.update(status_code="DUPLICATE")
                print('PORT ALREADY IN USE!')
                self.send()
                return

        #So, status:v[0], ipv4_byte_object:v[1], mport:v[2], pport:[v3]
        self.peer_state.update({peer_name: ["Free", ipv4, mport, pport]})
        self.msg_to_send.update(status_code="SUCCESS")
        print(f'command handled, returning status code of SUCCESS')  
        self.send()

    def setupdht(self):
        name = self.msg_recv.get("peername")
        n = self.msg_recv.get("n")
        year = self.msg_recv.get("year")

        self.msg_to_send = {"status_code": "", "seq":self.mseq}
        #check if all the fields are present
        if not name or not n or not year: 
            self.msg_to_send.update(status_code="INVALID")
            print('required setup-dht msg fields not present')
            self.send()
            return
    
        state = self.peer_state.get(name)
        if not state:
            self.msg_to_send.update(status_code="FAILURE")
            print('Leader name not registered!') 
            self.send() 
            return
    
        if n < 3:
            self.msg_to_send.update(status_code="FAILURE")
            print('minimum size of DHT is 3!')
            self.send()
            return
        
        if len(self.peer_state) < n:
            self.msg_to_send.update(status_code="FAILURE")
            print('not enough peers have registered yet!')
            self.send()
            return
        
        if self.DHT_setup != "TO DO":
            self.msg_to_send.update(status_code="DUPLICATE") #Meant to indicate that FILURE was caused by DHT already having bee nsetup 
            print('DHT already setup or is in progress!')  
            self.send()
            return

        if not (state[1], state[2]) == self.clientAddr:
            self.msg_to_send.update(status_code="INVALID")
            print('Ambiguity in leader address, registration record does not match setupdht clientAddr!')
            self.send()
            return

        #All checks passed, Manage leader state, addr, info
        self.leader_name = name
        self.leader_addr = (state[1], state[2])
        state[0] = "Leader" #change status from 'Free' to 'Leader'
        self.peer_state.update({name: state}) #Updating leader status

        self.DHT_members = [] #ensuring setup is new
        self.DHT_members.append((name, state[1], state[3])) #adding leader tuple to list
        free_peers = [name for name,v in self.peer_state.items() if name != self.leader_name and v[0]=="Free"] 
        #Form the DHT_members 3-tuple list
        member_name_list = random.sample(free_peers, n-1) 
        print(member_name_list)
        for name in member_name_list:
            v = self.peer_state.get(name)
            print(v)
            v[0] = 'InDHT'
            self.peer_state.update({name: v}) #change member state to InDHT
            self.DHT_members.append((name, v[1], v[3])) #appending chosen member tuples to DHT_members list

        self.DHT_num_members = len(self.DHT_members)
        self.DHT_setup = "IN PROGRESS"
        self.msg_to_send.update(status_code='SUCCESS')
        self.msg_to_send.update(memberlist=self.DHT_members[:20])  #sending only first 20 members.
        print(f'setupdht command handled, returning status code of SUCCESS')  
        self.send()
        
    def dhtcomplete(self):
        name = self.msg_recv.get("peername")
        self.msg_to_send = {"status_code": "", "seq":self.mseq}
        if self.clientAddr != self.leader_addr:
            print("dht-complete recevied from peer other than manager, usage issue, ignore command")
            return
        if name != self.leader_name:
            self.msg_to_send.update(status_code="FAILURE")
            print('dht-complete needs leader name!')
            self.send()
            return
        self.DHT_setup = "DONE"
        self.msg_to_send.update(status_code="SUCCESS")
        print('dht-complete received with leader-name, sending SUCCESS msg to leader')
        self.send()

    def querydht(self):
        name = self.msg_recv.get("peername")
        self.msg_to_send = {"status_code": "", "seq":self.mseq}
        
        if not self.DHT_setup == "DONE":
            print('DHT setup not completed, cannot support query!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return

        peer_state = self.peer_state.get(name)
        if not peer_state:
            print('Peer making query is not registered!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return

        peer_status = peer_state[0]
        if not peer_status == 'Free':
            print('Peer making query is not Free!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()  
            return 

        self.msg_to_send.update(status_code="SUCCESS")
        S_id = random.randrange(0, len(self.DHT_members))
        self.msg_to_send.update(peer=self.DHT_members[S_id])
        self.send()

    def joindht(self):
        self.peer_joining = None
        name = self.msg_recv.get("peername")
        self.msg_to_send = {"status_code": "", "seq":self.mseq}
        
        if not self.DHT_setup == "DONE":
            print('DHT setup not completed, cannot support join!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return

        peer_state = self.peer_state.get(name)
        if not peer_state:
            print('Peer making join request is not registered!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return

        peer_status = peer_state[0]
        if not peer_status == 'Free':
            print('Peer making join request is not Free!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()  
            return 

        self.peer_joining = name
        self.msg_to_send.update(status_code="SUCCESS")
        S_id = random.randrange(0, len(self.DHT_members))
        self.msg_to_send.update(peer=self.DHT_members[S_id]) 
        #sends a random peer's (name, ipv4, pport) which joiner can make it's right neighbour 
        #it can then send reset and teardown commands to this peer 
        self.send()

    def leavedht(self):
        self.peer_leaving = None
        name = self.msg_recv.get("peername")
        self.msg_to_send = {"status_code": "", "seq":self.mseq}
        
        if not self.DHT_setup == "DONE":
            print('DHT setup not completed, cannot support leave!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return

        peer_state = self.peer_state.get(name)
        if not peer_state:
            print('Peer making leave request is not registered!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return

        peer_status = peer_state[0]
        if peer_status == 'Free':
            print('Peer making leave request is not maintaining DHT!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()  
            return 

        self.peer_leaving = name
        self.msg_to_send.update(status_code="SUCCESS")
        self.send()     

 
    def dhtrebuilt(self):
        print('dht-rebuilt command received')
        name = self.msg_recv.get("peername")
        leader_name = self.msg_recv.get("leadername")

        if not name or not leader_name:
            print('Fields missing from dht-rebuilt, ignoring msg')
            return

        self.msg_to_send = {"status_code": "", "seq":self.mseq}
        leader_state = self.peer_state.get(leader_name)
        peer_state = self.peer_state.get(name)
        if not leader_state or not peer_state:
            print(f'leadername {leader_name} or peername {name} sent is not registered! sending failure')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return

        leader_id = -1
        for i,v in enumerate(self.DHT_members):
            if v[0] == leader_name:
                leader_id = i
                break
        if (leader_id < 0):
            print(f'leadername {leader_name} is not part of DHT! sending failure')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return

        past_leader_state = self.peer_state.get(self.leader_name)
        past_leader_state[0] = 'InDHT'
        self.peer_state.update({self.leader_name : past_leader_state})
        if name == self.peer_joining:
            peer_state[0] = 'InDHT'
            self.peer_state.update({name: peer_state})
            peer_tuple = (name, peer_state[1], peer_state[3])

            memberlist = self.DHT_members[leader_id:] #reordering memberlist
            memberlist.extend(self.DHT_members[:leader_id])
            memberlist.append(peer_tuple) #appending joint peer to member list
            self.DHT_members = memberlist
            self.DHT_num_members = self.DHT_num_members + 1

            self.leader_name = leader_name
            self.leader_addr = (leader_state[1], leader_state[2])
            leader_state[0] = 'Leader'
            self.peer_state.update({leader_name: leader_state})
            print('peer join successfully completed, clearing out blockade')
            self.peer_joining = None
        
        elif name == self.peer_leaving:
            peer_state[0] = 'Free'
            self.peer_state.update({name: peer_state})
            peer_tuple = (name, peer_state[1], peer_state[3])

            memberlist = self.DHT_members[leader_id:] #reordering memberlist
            memberlist.extend(self.DHT_members[:leader_id])
            del_member = memberlist.pop() #removing leaving member
            if del_member != peer_tuple:
                print('CAUTION: leader_name not selected as expected, wrong member deleted!')
            self.DHT_members = memberlist
            self.DHT_num_members = self.DHT_num_members - 1

            self.leader_name = leader_name
            self.leader_addr = (leader_state[1], leader_state[2])
            leader_state[0] = 'Leader'
            self.peer_state.update({leader_name: leader_state})
            print('peer join successfully completed, clearing out blockade')
            self.peer_leaving = None
        else:
            print('peername received is neither leaving nor joining!')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return
        
        self.DHT_setup = "DONE"
        self.msg_to_send.update(status_code="SUCCESS")
        print('dht-rebuilt received with leader-name, sending SUCCESS msg to leader')
        self.send()

    def teardowndht(self):
        peername = self.msg_recv.get('peername')
        self.msg_to_send = {"status_code": "", "seq":self.mseq}
        if not peername == self.leader_name:
            print(f'peername {peername} sent is not Leader! sending failure')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return
        self.tearing_dht = True
        self.msg_to_send.update(status_code="SUCCESS")
        print('teardown-dht received with leader-name, sending SUCCESS msg to leader')
        self.send()
        
    def teardowncomplete(self):
        peername = self.msg_recv.get('peername')
        self.msg_to_send = {"status_code": "", "seq":self.mseq}
        if not peername == self.leader_name:
            print(f'peername {peername} sent is not Leader! sending failure')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return
        self.tearing_dht = False #releases blockade
        self.DHT_setup = "TO DO"#ensures that all the remaining meta info about dht is N/A

        for name, v in self.peer_state.items(): #Updating every registered member to Free
            v[0] = 'Free'
            self.peer_state.update({name: v})

        #clearing out other DHT meta info
        self.DHT_members = [] 
        self.DHT_num_members = 0
        self.DHT_year = None
        self.leader_name = None
        self.leader_addr = None

        self.msg_to_send.update(status_code="SUCCESS")
        print('teardown-complete received with leader-name, sending SUCCESS msg to leader')
        self.send()        

    def deregister(self):
        peername = self.msg_recv.get('peername')
        self.msg_to_send = {"status_code": "", "seq":self.mseq}
        peer_state = self.peer_state.get(peername)
        if not peer_state:
            print(f'peername {peername} sent is not registered! sending failure')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return
        if not peer_state[0] == 'Free':
            print('Peer need to be free to allow deregistration! sending failure')
            self.msg_to_send.update(status_code="FAILURE")
            self.send()
            return
        del self.peer_state[peername]
        self.msg_to_send.update(status_code="SUCCESS")
        print('deregister received with leader-name, sending SUCCESS msg to leader')
        self.send()           

    def send(self):
        print(f'sending msg: {self.msg_to_send} to {self.clientAddr}')
        self.socket.sendto((json.dumps(self.msg_to_send)).encode('utf-8'), self.clientAddr)
        #As manager neveinitiates the request, it does not update the seq number
        #it is merely responding with the same seq number
def main():
    manager = Manager(sys.argv[1])
    manager.socket.bind(('', manager.listening_port))
    while True:
        msg, clientAddr = manager.socket.recvfrom(1000)

        try: #For debugging, incase it is JSON conversion that causes problems
            manager.msg_recv = json.loads(msg.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError):
            print(f'error caused by JSON conversio of message: {msg}')
            continue #ignore corrupt msg

        manager.clientAddr = clientAddr
        seq = manager.msg_recv.get("seq")
        command = manager.msg_recv.get("command")

        #ignore msg if no seq number or no command
        if seq is None or not command:
            print('Either sequence number or command field is missing')
            continue #Handle duplicate requests by resending response [value of request key in manager.requests dict]
       
        request = (manager.msg_recv, manager.clientAddr)
        request_hash = (seq, manager.clientAddr)
        if request_hash in manager.requests:
            print("Duplicate msg received, sending status_code again")
            manager.msg_to_send = manager.requests[request_hash] 
            manager.send()
            continue   

        manager.mseq = seq
        manager.msg_to_send = {"status_code": "", "seq":manager.mseq}
        #Manager waiting for dht-complete after setup-dht
        if manager.DHT_setup == "IN PROGRESS":
            if clientAddr == manager.leader_addr:  #event that designed nack for member_list
                if command == "setup-dht":
                    print('setupdht duplicate request not handled properly by manager or resend was corrupted')
                    continue
            else:
                print(f'Sending FAILURE to request {request} received while DHT_Setup IN PROGRESS') #any other request                                                  
                manager.msg_to_send.update(status_code="FAILURE")
                manager.send()
                continue

        if (manager.peer_joining or manager.peer_leaving) and command != 'dht-rebuilt': 
            #trusting duplicate message handling here, can implement extra guard if need be
            print(f'Sending FAILURE to request {request} received while DHT_Setup IN PROGRESS') #any other request                                                  
            manager.msg_to_send.update(status_code="FAILURE")
            manager.send()
            continue

        if  manager.tearing_dht == True and command != "teardown-complete":
            #trusting duplicate message handling here, can implement extra guard if need be
            print(f'Sending FAILURE to request {request} received while DHT is being torn down') #any other request                                                  
            manager.msg_to_send.update(status_code="FAILURE")
            manager.send()
            continue

        handler = manager.handlers.get(command)
        if not handler:
            print('command passed does not exist')
            manager.msg_to_send.update(status_code="INVALID")
            manager.send()
            continue
        handler()  

        if len(manager.requests) >= 10:  #maximum ten old msgs to be stored of duplicate check
            del manager.requests[next(iter(manager.requests))]
        manager.requests.update({request_hash: manager.msg_to_send}) #buffer to allow server to recognize duplicate request
        
if __name__ == "__main__":
    main()


        

