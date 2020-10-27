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
import queue as queue
import threading
import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc

# gatherCommandLine()
# reads-in input from the command-line in the form of:
# <nodeID> <portnum> <k>
def gatherCommandLine():
	if len(sys.argv) != 4:
		print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
		sys.exit(-1)

	local_id = int(sys.argv[1])
	my_port = str(int(sys.argv[2]))                                                             # add_insecure_port() will want a string
	k = int(sys.argv[3])
	my_hostname = socket.gethostname()                                                          # Gets my host name
	my_address = socket.gethostbyname(my_hostname)                                              # Gets my IP address from my hostname

def initializingBuckets():


if __name__ == '__main__':
	gatherCommandLine()
    initializingBuckets()
