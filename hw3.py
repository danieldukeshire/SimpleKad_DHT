# hw3.py: SimpleKad DHT Inplementation
# written by: Daniel Dukeshire, Thomas Durkin, Chris Allen, Christopher Pence
# This program implements a distrbuted hash table siilar to Kademlia:
# https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf
# This implementation is done usign gRPC, and is run via:
#
# python3 hw3.py <nodeID> <port> <k>
#

#!/usr/bin/env python3
from concurrent import futures
import sys
import socket
import grpc
import time
import select
import queue as Queue
import threading
import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc

# Some global variables
N = 4                                   # The maximum number of buckets (N) is 4
buckets = [[]] * N                      # The k_buckets array

# Server-side ---------------------------------------------------------------------------

#
#
#
def serve():
    print("gRPC server starting at: {}".format(my_address+':'+my_port))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    csci4220_hw3_pb2_grpc.add_KadImplServicer_to_server(KadImpl(), server)
    server.add_insecure_port(my_address + ':' + my_port)
    server.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop()
        sys.exit()

# class KadImpl()
# Provides methods that implement functionality of KadImplServer.
# Is called by the store, bootstrap, findvalue, and findnode from
# other client/servers
class KadImpl(csci4220_hw3_pb2_grpc.KadImplServicer):
    def FindNode(self, request, context):
        print("To be implemented")

    def FindValue(self, request, context):
        print("To be implemented")

    def Store(self, request, context):
        print("To be implemented")

    def Quit(self, request, context):
        print("To be implemented")

# Client-side -----------------------------------------------------------------------------

# store()
# Takes input: the input string from the console
#
def store(input):
    print("STORE")
    print(input)

# bootStrap()
# Takes input: the input string from the console
#
def bootStrap(input):
    print("BOOSTRAP")
    print(input)

# findValue()
# Takes input: the input string from the console
#
def findValue(input):
    print("FINDVALUE")
    print(input)

# findNode()
# Takes input: the input string from the console
#
def findNode(input):
    print("FINDNODE")
    print(input)

# quit()
# Takes input: the input string from the console
#
def quit(input):
    print("QUIT")
    print(input)

# run()
# reads-in input from the command-line in the form of:
# <nodeID> <portnum> <k>
def run():
    if len(sys.argv) != 4:                              # Some error checking
        print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
        sys.exit(-1)
    global local_id
    global my_port
    global my_hostname
    global my_address

    local_id = int(sys.argv[1])
    my_port = str(int(sys.argv[2]))                     # add_insecure_port() will want a string
    k = int(sys.argv[3])

    #my_hostname = socket.gethostname()                 # Gets my host name
    my_hostname = "127.0.0.1"
    print(my_hostname)
    my_address = socket.gethostbyname(my_hostname)      # Gets my IP address from my hostname

    threading.Thread(target = serve).start()            # server thread, so we can simultaniously do ....
    client()

# client()
# Reads in from the terminal, acting as a "client" and passes the input
# to the client functions
def client():
    while(True):
        buf = input()
        print("MAIN: Received from stdin: " + buf)
        if buf == "STORE":                              # Passes to store function
            store(buf)
        elif buf == "BOOTSTRAP":                         # Passes to bootstrap function
            bootStrap(buf)
        elif buf == "FIND_VALUE":                       # Passes to find_value function
            findValue(buf)
        elif buf == "FIND_NODE":                        # Passes to find_node function
            findNode(buf)
        elif buf == "QUIT":                             # Terminates the function
            quit(buf)
            sys.exit()
        else:                                           # Otherwise... keep looping and print the following
            print("Invalid command. Try: 'STORE', 'BOOTSTRAP', 'FIND_VALUE', 'FIND NODE', 'QUIT'")

# main
# Gathers input from the command line, stores in global variables
# Threads off a server to simultaniously work alongside the client
# The client works through gatherinput
if __name__ == '__main__':
    run()
