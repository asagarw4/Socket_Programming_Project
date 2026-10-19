from socket import*
import os, sys
import pandas as pd
import numpy as np
import json
import random
import math
import threading

listening_port = sys.argv[1]
socket = socket(AF_INET, SOCK_DGRAM)
socket.bind('', listening_port)

DHT_setup = "TO DO"
Leader = ""
peer_state = {} # keys are peernames, values are tuples (state, IPV4 addr, m-port, p-port)
DHT_members = [] #list to be sent to the leader during setup-dht

requests_received = {}

def register(msg, peer_state):
    peer_name = msg.get("peername")
    ipv4 = msg.get("ipv4") #string object, "100.10.10.1" or host name
    mport = msg.get("mport")
    pport = msg.get("pport")

    #check if all the fields are present
    if not peer_name or not ipv4 or not mport or not pport: 
        print('required register msg fields not present')
        msg_to_send = {"status_code": "INVALID"}
        return msg_to_send

    #ensure uniqueness of peer name
    if (peer_state.get(peer_name)):  
        msg_to_send = {"status_code": "DUPLICATE"}
        print('PEER ALREADY REGISTERED!')
        return msg_to_send

    #ensure uniqueness of pport and mport
    for i,v in enumerate(peer_state):
        if v[2] == "mport" or v[3] == "pport":
            msg_to_send = {"status_code": "DUPLICATE"}
            print('PORT ALREADY IN USE!')
            return msg_to_send

    #So, status:v[0], ipv4_byte_object:v[1], mport:v[2], pport:[v3]
    peer_state.update(peer_name=("Free", ipv4, mport, pport))
    msg_to_send = {"status_code": "SUCCESS"}
    return msg_to_send

def setupdht(msg):
    leader_name = msg.get("peername")
    n = msg.get("n")
    year = msg.get("year")

    #check if all the fields are present
    if not leader_name or not n or not year: 
        msg_to_send = {"status_code": "INVALID"}
        print('required setup-dhtmsg fields not present')
        return msg_to_send
    
    leader_state = peer_state.get(leader_name)
    if not leader_state:
        msg_to_send = {"status_code": "FAILURE"}
        print('Leader name not registered!')  
        return msg_to_send
    
    if n < 3:
        msg_to_send = {"status_code": "FAILURE"}
        print('minimum size of DHT is 3!')
        return msg_to_send
    
    if len(peer_state) < n:
        msg_to_send = {"status_code": "FAILURE"}
        print('not enough peers have registered yet!')
        return msg_to_send

    if DHT_setup:
        msg_to_send = {"status_code": "DUPLICATE"} #Meant to indicate that FILURE was caused by DHT already having bee nsetup 
        print('DHT already setup!')
        return msg_to_send       

    #Manage leader state, addr, info
    leader_state = peer_state.get(leader_name) 
    leader_addr = (leader_state[1], leader_state[2]) #(ipv4, m-PORT)
    leader_state[0] = "Leader"                   #change status from 'Free' to 'Leader'
    peer_state.update(leader_name=leader_state) #Updating leader status
    DHT_members.append((leader_name, leader_state[1], leader_state[3])) #adding leader tuple to list
    
    #Form the DHT_members 3-tuple list
    member_name_list = random.sample(list(peer_state), n-1)
    for name in member_name_list:
        v = peer_state.get(name)
        DHT_members.append((name, v[1], v[3]))

    global DHT_setup
    DHT_setup = "IN PROGRESS"
    msg_to_send = {"status_code": 'SUCCESS', "memberlist": DHT_members[:20]} #sending first 20 members.
    return msg_to_send

def dhtcomplete(msg):
    leader_name = msg.get("peername")
    leader_state = DHT_members[0]
    if leader_name == leader_state[0]:
        global DHT_setup
        DHT_setup = "Done"
        msg_to_send = {"status_code": "SUCCESS"}
    else:
        msg_to_send = {"status_code": "FAILURE"}
        print('dht-complete needs leader name!')
    return msg_to_send
    

handlers = {"register": register, "setup-dht": setupdht, "dht-complete": dhtcomplete}
setupdht_request = None

while True:
    message, clientAddress = socket.recvfrom(1000)

    try: #For debugging, incase it is JSON conversion that causes problems
        msg_json = json.loads(message.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError):
        print(f'error caused by JSON conversio of message: {message}')
        status_code = "CORRUPTION" #msg corrupted, ignore
        continue

    request = (message, clientAddress)
    if request in list(requests_received):  #Handle duplicate requests by resending status_code [value of key]
        print("Duplicate msg received, sending status_code again")
        socket.sendto(requests_received[request], clientAddress)
        continue   


    seq = msg_json.get("seq")
    command = msg_json.get("command")
    if not seq or not command:
            print('Sequence number is missing')
            status_code = "CORRUPTION" 
            continue #ignore msg if no seq number or no command
    
    
    if DHT_setup == "IN PROGRESS":  #NManager waiting for dht-complete after setup-dht
        leader_name = DHT_members[0][0]
        leader_state = peer_state[leader_name]
        leader_address = (leader_state[1], leader_state[2])
           
        if clientAddress == leader_address and command == "setupdht":  #event that designed nack for member_list
            print('setupdht duplicate request not handled properly by manager or resend was corrupted')
        elif clientAddress == leader_address and command == "dhtcomplete":
            peer_name = msg_json.get("peername")
            if not peer_name == leader_name:
                print('Invalid dht-complete request made by leader, peer-name not leader-name')
            else:
                print(f'Sending FAILURE to request f{request} received while DHT_Setup IN PROGRESS') #any other request                                                  
                msg_to_send = {"status_code": "FAILURE", "seq":seq}
                socket.sendto((json.dumps(msg_to_send)).encode('utf-8'), clientAddress)
            continue
    
    handler = handlers.get(command)   
    if not handler:
        print('command passed does not exist')
        status_code = "INVALID"
        msg_to_send = {"status_code": status_code, "seq":seq}
        socket.sendto((json.dumps(msg_to_send).encode('utf-8')), clientAddress)
        continue

    msg_to_send = handler(msg_json)  
    msg_to_send.update(seq=seq) #Update the msg to send with sequence number

    print(f'command handled, returning status code of {msg_to_send[status_code]}')
    print(f'sending msg: {msg_to_send} to {clientAddress}')  
    socket.sendto((json.dumps(msg_to_send)).encode('utf-8'), clientAddress)

    if len(requests_received) >= 10:  #maximum ten old msgs to be stored of duplicate check
        requests_received.popitem()
    if not status_code == "CORRUPTION": #Policy decision: don't store requests that result in CORRUPTION status-code
        requests_received.update(request=msg_to_send) #buffer to allow server to recognize duplicate request
        

