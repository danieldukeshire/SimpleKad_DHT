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
import threading
import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc
from LRUCache import LRUCache

# Some global variables
N = 4  # The maximum number of buckets (N) is 4
k = None
k_buckets = [[]] * N  # The k_buckets array: N buckets of size k
_ONE_DAY_IN_SECONDS = 86400
local_id = None
my_port = None
my_hostname = None
my_address = None
hash_table = None


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

        print("Serving FindNode(" + id_target + ") request for " + id_request)




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


# Store a node in the correct bucket
# Note: Head of the list is the last index
def save_node(node):
    xor = node.id ^ local_id
    base = 2
    exp = 0
    val = base**exp

    if xor != 0:
        while val < xor:
            val = base**++exp

        if val > xor:
            --exp

    node_idx = 0

    # See if node already exists
    while node_idx < len(k_buckets[exp]):
        cmp_node = k_buckets[exp][node_idx]
        if cmp_node.id == node.id and cmp_node.address == node.address and cmp_node.port == node.port:
            break
        ++node_idx

    # If node already exists or list is full, remove the correct node
    if node_idx < len(k_buckets[exp]):
        k_buckets[exp].pop(node_idx)
    else:
        if len(k_buckets[exp]) >= k:
            k_buckets[exp].pop(0)

    # Finally, append node to head of list [last index]
    k_buckets[exp].append(node)


# bootStrap()
# Takes args: the input string from the console
#
def bootstrap(args):
    hostname = args.split()[1]  # Gettting the hostname from input
    port = args.split()[2]  # Getting the port from input
    address = socket.gethostbyname(hostname)  # Getting the hostname

    channel = grpc.insecure_channel(address + ':' + port)
    kad = csci4220_hw3_pb2_grpc.KadImplStub(channel)

    # Get NodeList from remote node to update k_buckets
    response = kad.FindNode(
        csci4220_hw3_pb2.IDKey(
            node=csci4220_hw3_pb2.Node(
                id=local_id,
                port=int(my_port),
                address=my_address),
            idkey=local_id))

    for node in response.nodes:
        save_node(node)

    print("After BOOTSTRAP({}), k_buckets now look like:\n{}", response.responding_node.id, print_buckets())


def print_buckets():
    result = str()
    for i in range(len(k_buckets)):
        result += str(i) + " ["
        for j in range(len(k_buckets[i])):
            if j < len(k_buckets[i]) - 1:
                result += str(k_buckets[i][j].id) + ":" + str(k_buckets[i][j].port) + " "
            else:
                result += str(k_buckets[i][j].id) + ":" + str(k_buckets[i][j].port) + "]\n"

    return result


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


# Reads command line arguments and initializes variables
def initialize():
    if len(sys.argv) != 4:  # Some error checking
        print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
        sys.exit(-1)
    global local_id
    global my_port
    global k
    global my_hostname
    global my_address
    global hash_table

    local_id = int(sys.argv[1])
    my_port = str(int(sys.argv[2]))
    k = sys.argv[3]
    hash_table = LRUCache(k)

    my_hostname = socket.gethostname()
    my_address = socket.gethostbyname(my_hostname)


def run():
    while True:
        buf = input()
        print("MAIN: Received from stdin: " + buf)
        if "STORE" in buf:
            store(buf)
        elif "BOOTSTRAP" in buf:
            bootstrap(buf)
        elif "FIND_VALUE" in buf:
            find_value(buf)
        elif "FIND_NODE" in buf:
            find_node(buf)
        elif "QUIT" in buf:
            quit(buf)
            sys.exit()
        else:
            print("Invalid command. Try: 'STORE', 'BOOTSTRAP', 'FIND_VALUE', 'FIND NODE', 'QUIT'")


# main
# Gathers input from the command line, stores in global variables
# Threads off a server to simultaniously work alongside the client
# The client works through run()
if __name__ == '__main__':
    initialize()
    threading.Thread(target=serve).start()
    run()
