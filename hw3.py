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
k_buckets = []
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
        node = request.node
        id_key = request.idkey

        print("Serving FindNode({}) request for {}".format(id_key, node.id))

        closest_nodes = find_k_closest(id_key)
        save_node(node)

        return csci4220_hw3_pb2.NodeList(
            responding_node=csci4220_hw3_pb2.Node(
                id=local_id,
                port=int(my_port),
                address=my_address
            ),
            nodes=closest_nodes
        )

    def FindValue(self, request, context):
        print("Serving FindKey({}) request for {}".format(request.idkey, request.node.id))
        has_key = hash_table.contains_key(request.idkey)
        value = ""
        nodes = []
        if has_key:
            value = hash_table.get(request.idkey)
        else:
            nodes = find_k_closest(request.idkey)

        return csci4220_hw3_pb2.KV_Node_Wrapper(
            responding_node=csci4220_hw3_pb2.Node(
                id=local_id,
                port=int(my_port),
                address=my_address
            ),
            mode_kv=has_key,
            kv=csci4220_hw3_pb2.KeyValue(
                node=request.node,
                key=request.idkey,
                value=value
            ),
            nodes=nodes
        )

    def Store(self, request, context):
        print("Storing key {} value \"{}\"".format(request.key, request.value))
        hash_table.put(request.key, request.value)
        save_node(request.node)
        # Need to return something, but this isn't used
        return csci4220_hw3_pb2.IDKey(
            node=request.node,
            idkey=request.key
        )

    def Quit(self, request, context):
        if node_is_stored(request.node):
            bucket_num = remove_node(request.idkey)
            print("Evicting quitting node {} from bucket {}".format(request.idkey, bucket_num))
        else:
            print("No record of quitting node {} in k-buckets.".format(request.idkey))

        return request


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
def store(args):
    key = int(args.split()[1])
    value = args.split()[2]

    k_closest = find_k_closest(key)
    closest_node = None if len(k_closest) < 1 else k_closest[0]

    if closest_node is None or key ^ local_id < key ^ closest_node.id:
        print("Storing key {} value \"{}\"".format(key, value))
        hash_table.put(key, value)
    else:
        print("Storing key {} at node {}".format(key, closest_node.id))
        channel = grpc.insecure_channel("{}:{}".format(closest_node.address, closest_node.port))
        kad = csci4220_hw3_pb2_grpc.KadImplStub(channel)

        kad.Store(csci4220_hw3_pb2.KeyValue(
            node=closest_node,
            key=key,
            value=value
        ))

# Remove node of specified id
# If node does not exist, returns -1
def remove_node(node_id):
    index = -1
    position = -1
    for i in range(len(k_buckets)):
        for j in range(len(k_buckets[i])):
            if node_id == k_buckets[i][j].id:
                index = i
                position = j
                break
        if index != -1:
            break

    k_buckets[index].pop(position)
    return index

# Store a node in the correct bucket
# Note: Head of the list is the last index
def save_node(node):
    xor = node.id ^ local_id
    base = 2
    exp = 0
    val = base**exp

    if xor != 0:
        while val < xor:
            exp += 1
            val = base**exp

        if val > xor:
            exp -= 1

    exists = False
    existing_idx = 0
    # See if node already exists
    for existing_node in k_buckets[exp]:
        if existing_node.id == node.id and existing_node.address == node.address and existing_node.port == node.port:
            exists = True
            break
        existing_idx += 1
    # If node already exists or list is full, remove the correct node
    if exists:
        k_buckets[exp].pop(existing_idx)
    elif len(k_buckets[exp]) == k:
        k_buckets[exp].pop(0)

    # Finally, append node to head of list [last index]
    k_buckets[exp].append(node)


# Return the k closest nodes to this node ordered by xor distance
def find_k_closest(id_key):
    k_closest = list()
    for node_list in k_buckets:
        for node in node_list:
            k_closest.append((node, int(id_key) ^ int(node.id)))
    if len(k_closest) < 1:
        return []
    # Return first k nodes ordered by distance
    nodes = []
    for item in k_closest:
        nodes.append(item[0])
    return nodes

#
# bootStrap()
# Takes args: the input string from the console
# This command lets a node connect to another node by exchanging information so that both nodes know each
# other’s ID, address, and port. This is done by sending the remote node a FindNode RPC using the local
# node’s ID as the argument.
#
def bootstrap(args):
    hostname = args.split()[1]                                      # Gettting the hostname from input
    port = args.split()[2]                                          # Getting the port from input
    address = socket.gethostbyname(hostname)                        # Getting the hostname

    channel = grpc.insecure_channel("{}:{}".format(address, port))
    kad = csci4220_hw3_pb2_grpc.KadImplStub(channel)

    # Get NodeList from remote node to update k_buckets
    response = kad.FindNode(
        csci4220_hw3_pb2.IDKey(
            node=csci4220_hw3_pb2.Node(
                id=local_id,
                port=int(my_port),
                address=my_address),
            idkey=local_id))

    # Save nodes learned from bootstrap node
    for node in response.nodes:
        print("NODE: {}".format(node.id))
        save_node(node)

    # Save bootstrap node
    save_node(response.responding_node)

    print("After BOOTSTRAP({}), k_buckets now look like:\n{}".format(response.responding_node.id, print_buckets()))


#
# print_buckets()
# Prints the current k_buckets array in the format specified in the pdf
# Format:
# 0: 1: 9001 0:5001
# 1:
# 2:
# 3:
#
def print_buckets():
    result = str()
    for i in range(len(k_buckets)):
        result += str(i) + ": "
        for j in range(len(k_buckets[i])):
            result += "{}:{}".format(k_buckets[i][j].id, k_buckets[i][j].port)
            if j < len(k_buckets[i]) - 1:                   # String formatting
                result += " "
        if i < len(k_buckets) - 1:
            result += "\n"

    return result

#
# findValue()
# Takes input: the input string from the console in the form:
# FIND_VALUE <key>
# This behaves the same way that FIND_NODE does, but uses the key instead of a node ID to determine
# which node to query next, and the FindValue RPC instead of the FindNode RPC. If the remote node has
# not been told to store the key, it will reply with the k closest nodes to the key. If the remote node has been
# told to store the key before, then it does not return a list of nodes, and instead responds with the key and
# the associated value.
#
def find_value(args):
    print("Before FIND_VALUE command, k-buckets are:\n{}".format(print_buckets()))

    key = int(args.split()[1])                              # Getting value from the input
    if hash_table.contains_key(key):                        # the hash table is where we keep the key value pairs
        print("Found data \"{}\" for key {}".format(hash_table.get(key), key))
        return
    else:
        unvisited = find_k_closest(key)                     #  if we dont have the value in out hash ...
        visited = list()
        visited.append(csci4220_hw3_pb2.Node(               # we need to loop over the k closest nodes, similar to findnode
                id=local_id,
                port=int(my_port),
                address=my_address
            ))
        next_visit = []

        value_found = False
        value = None
        while len(unvisited) > 0 and not value_found:       # Now, we loop over all of the unvisited nodes
            for node in unvisited:
                channel = grpc.insecure_channel("{}:{}".format(node.address, node.port))
                kad = csci4220_hw3_pb2_grpc.KadImplStub(channel)
                response = kad.FindValue(                   # Create a connection to that node
                    csci4220_hw3_pb2.IDKey(
                        node=node,
                        idkey=key
                    )
                )
                save_node(node)                             # Save it, as it needs to be updated based on LRU
                visited.append(node)                        # And we add it to the visited nodes array

                if response.mode_kv:
                    value = response.kv.value
                    value_found = True
                    break

                for resp_node in response.nodes:            # If we havent found the value in our direct k_buckets....
                    if not node_is_stored(resp_node) and resp_node.id != local_id:      # We look in each node's k_buckets
                        save_node(resp_node)
                    if resp_node not in visited:
                        next_visit.append(resp_node)
            unvisited = next_visit
            next_visit = []

            if value_found:                                 # Handling the output statements for the found value, if it is found/not
                print("Found value \"{}\" for key {}".format(value, key))
            else:
                print("Could not find key ".format(key))

            print("After FIND_VALUE command, k-buckets are:\n" + print_buckets())

#
# node_is_stored()
# Takes in a node as input
# Returns True if node is stored in a k_bucket, false otherwise
#
def node_is_stored(node):
    for node_list in k_buckets:             # Loops over k_buckets
        for cmp_node in node_list:          # Loops over each node
            if node.id == cmp_node.id:      # Makes comparison
                return True
    return False                            # If no nodes are found, we return false.

#
# findNode()
# Takes input: the input string from the console in the form:
# FIND_NODE <nodeID>
# attempts to find a remote node, and has the side effect of updating the current node’s kbuckets.
# If the node’s ID is <nodeID>, then no search should be made and the node can skip directly to
# the post-search output, and should behave as though it found the node.
#
def find_node(args):
    print("Before FIND_NODE command, k-buckets are:\n{}".format(print_buckets()))

    node_id = int(args.split()[1])
    unvisited = find_k_closest(node_id)                 # Accesses the k closest nodes in the buckets
    visited = []                                        # returned in a dictionary
    next_visit = []

    node_found = False
    while len(unvisited) > 0 and not node_found:
        for node in unvisited:                          # looping over all of the unvisted nodes
            channel = grpc.insecure_channel("{}:{}".format(node.address, node.port))
            kad = csci4220_hw3_pb2_grpc.KadImplStub(channel)
                                                        # creates a connection to each node
            response = kad.FindNode(                    # attempts to find the node in search by searching this
                csci4220_hw3_pb2.IDKey(                 # node's k_buckets
                    node=node,
                    idkey=node_id))
            save_node(node)                             # As we just accessed this node, we need to update the buckets
            visited.append(node)                        # Add it to the visited node, do not want to backtrack

            if node.id == node_id:                      # If this node is the one we are searching for we can break
                node_found = True
                break

            for resp_node in response.nodes:            # Otherwise we also need to check to see the node's k_buckets
                if not node_is_stored(resp_node):
                    save_node(resp_node)                # If we come across a node not in our network.... we add it
                if resp_node not in visited:
                    next_visit.append(resp_node)        # Also add this one to the visited nodes list
                if resp_node.id == node_id:
                    node_found = True
                    break
        unvisited = next_visit
        next_visit = []

        print("After FIND_NODE command, k-buckets are:\n" + print_buckets())
        if node_found:                                  # Handling the computed node / no node found messages
            print("Found destination id {}".format(node_id))
        else:
            print("Could not find destination id ".format(node_id))

#
# execute_quit()
# Terminates the current node (the client)
# Whilst doing this, notifies each node upon termination
# in order to update the k_buckets for each client in the
# p2p connection list
#
def execute_quit():
    for node_list in k_buckets:
        for node in node_list:
            print("Letting {} know I'm quitting.".format(node.id))                      # notifies each node in the k_buckets
            channel = grpc.insecure_channel("{}:{}".format(node.address, node.port))
            kad = csci4220_hw3_pb2_grpc.KadImplStub(channel)                            # creates a connection to each node
            kad.Quit(csci4220_hw3_pb2.IDKey(                                            # and calls a quit on THIS node
                node=csci4220_hw3_pb2.Node(                                             # to other nodes, so they can update
                    id=local_id,                                                        # their k_buckets
                    port=int(my_port),
                    address=my_address),
                idkey=local_id)
            )
    print("Shut down node {}".format(local_id))                                         # Outputs to the console

#
# initialize()
# Stores the command-line input into global variables
# ./out <node id> <port number> <k value>
#
def initialize():
    if len(sys.argv) != 4:                          # Some error checking
        print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
        sys.exit(-1)
    global local_id, my_port, k, my_hostname, my_address, hash_table, k_buckets

    local_id = int(sys.argv[1])                     # Storing the local id
    my_port = str(int(sys.argv[2]))                 # Storing the portnum
    k_buckets = [[] for i in range(N)]
    k = int(sys.argv[3])
    hash_table = LRUCache(k)                        # Initializing LRU cache datastructure in LRUCache.py

    my_hostname = socket.gethostname()              # Calculating the hostname with the given parameters
    my_address = socket.gethostbyname(my_hostname)

#
# run()
# Takes input from the terminal, and handles the input in
# accordance to the Kadilema functionality. Calls helper functions
# for each different action performed in the p2p
#
def run():
    while True:
        buf = input()
        if "STORE" in buf:                  # handling store message from the client
            store(buf)
        elif "BOOTSTRAP" in buf:            # handling bootstrap message from the client
            bootstrap(buf)
        elif "FIND_VALUE" in buf:           # handling find value message from the client
            find_value(buf)
        elif "FIND_NODE" in buf:            # handling find node message from the client
            find_node(buf)
        elif "QUIT" in buf:                 # handling quit message from the client
            execute_quit()
            sys.exit()
        else:
            print("Invalid command. Try: 'STORE', 'BOOTSTRAP', 'FIND_VALUE', 'FIND NODE', 'QUIT'")

#
# main()
# Threads off a server to simultaniously work alongside the client
# The client works through run()
#
if __name__ == '__main__':
    initialize()                                # initializes the global variables
    threading.Thread(target=serve).start()      # threads off the server to work simultaniously with the client
    run()                                       # starts the client process
