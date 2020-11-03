# hw3.py: SimpleKad DHT Inplementation
# written by: Daniel Dukeshire, Thomas Durkin, Chris Allen, Christopher Pence
# This program implements a distrbuted hash table siilar to Kademlia:
# https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf
# This implementation is done usign gRPC, and is run via:
#
# python3 hw3.py <nodeID> <port> <k>
#

# !/usr/bin/env python3
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
N = 4  # The maximum number of buckets (N) is 4
k_buckets = [[]] * N  # The k_buckets array: N buckets of size k
_ONE_DAY_IN_SECONDS = 86400
local_id = None
my_port = None
my_hostname = None
my_address = None


# Server-side ---------------------------------------------------------------------------
# class KadImpl()
# Provides methods that implement functionality of KadImplServer.
# Is called by the store, bootstrap, findvalue, and findnode from
# other client/servers
class KadImpl(csci4220_hw3_pb2_grpc.KadImplServicer):
    def FindNode(self, request, context):
        # Getting values from the client request
        node = request.node
        id_request = node.id
        address_request = node.address
        port_request = node.port
        id_target = node.idkey

        closest_nodes = findClosestNodes(id_target)  # determines the closest nodes

        str = "Serving FindNode(" + id_target + ") request for " + id_request  # String formatting for output
        print(str)  # Prints to remote client

    def FindValue(self, request, context):
        print("To be implemented")

    def Store(self, request, context):
        node = request.node
        id_request = node.id
        address_request = node.address
        port_request = node.port
        print(
            "Recieved store request from id:{}, port:{}, address:{}".format(id_request, address_request, port_request))

        key_request = request.key
        val_request = request.value
        print("Storing key {} value {}".format(key_request, val_request))

        # implement storePair

    def Quit(self, request, context):
        print("To be implemented")


# server
#
#
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  # Creates a server with a max 10
    csci4220_hw3_pb2_grpc.add_KadImplServicer_to_server(KadImpl(), server)  # Passes the server to the grpc
    server.add_insecure_port(my_address + ':' + my_port)  # Adds the port
    server.start()

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


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
def bootstrap(input, my_hostname, my_address):
    hostname = input.split()[1]  # Gettting the hostname from input
    port = input.split()[2]  # Getting the port from input
    address = socket.gethostbyname(hostname)  # Getting the hostname

    channel = grpc.insecure_channel(address + ':' + port)  # Establishing an insecure channel
    stub = csci4220_hw3_pb2_grpc.KadImplStub(channel)  # Using the gRPC API in "stub"
    z = stub.FindNode(
        csci4220_hw3_pb2.IDKey(node=csci4220_hw3_pb2.Node(id=local_id, port=int(my_port), address=my_address),
                               idkey=local_id))


# findValue()
# Takes input: the input string from the console
#
def find_value(input):
    print("FINDVALUE")
    print(input)


# findNode()
# Takes input: the input string from the console
#
def find_node(input):
    print("FINDNODE")
    print(input)


# quit()
# Takes input: the input string from the console
#
def quit(input):
    print("QUIT")
    print(input)
    print("Shut down node {}".format(local_id))


# run()
# reads-in input from the command-line in the form of:
# <nodeID> <portnum> <k>
# and proceeds to read input from stdin
def run():
    if len(sys.argv) != 4:  # Some error checking
        print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
        sys.exit(-1)
    global local_id
    global my_port
    global my_hostname
    global my_address

    local_id = int(sys.argv[1])
    my_port = str(int(sys.argv[2]))  # add_insecure_port() will want a string
    k = int(sys.argv[3])

    # my_hostname = socket.gethostname()                 # Gets my host name
    my_hostname = "127.0.0.1"
    my_address = socket.gethostbyname(my_hostname)  # Gets my IP address from my hostname

    while True:
        buf = input()
        print("MAIN: Received from stdin: " + buf)
        if "STORE" in buf:  # Passes to store function
            store(buf)
        elif "BOOTSTRAP" in buf:  # Passes to bootstrap function
            bootstrap(buf, my_hostname, my_address)
        elif "FIND_VALUE" in buf:  # Passes to find_value function
            find_value(buf)
        elif "FIND_NODE" in buf:  # Passes to find_node function
            find_node(buf)
        elif "QUIT" in buf:  # Terminates the function
            quit(buf)
            sys.exit()
        else:  # Otherwise... keep looping and print the following
            print("Invalid command. Try: 'STORE', 'BOOTSTRAP', 'FIND_VALUE', 'FIND NODE', 'QUIT'")


# main
# Gathers input from the command line, stores in global variables
# Threads off a server to simultaniously work alongside the client
# The client works through run()
if __name__ == '__main__':
    threading.Thread(target=serve).start()  # server thread, so we can simultaniously do ....
    run()
