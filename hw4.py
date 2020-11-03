#!/usr/bin/env python3

from concurrent import futures
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()

import select
import queue as Queue
import grpc

import csci4220_hw4_pb2
import csci4220_hw4_pb2_grpc
import threading
import time

#Declaring global variables
_ONE_DAY_IN_SECONDS = 86400
MAX_BUCKETS = 4
local_id = None # Uint32 (int)
my_port = None # Uint32 (int)
my_address = None # String
k = None
idkey_idkey = None # Uint32 (int)
k_buckets = []
node_key = None
node_value = None

# class Peer:
#   def __init__(self, id, address, port):
#     self.id = id
#     self.address = address
#     self.port = port

class KadImpl(csci4220_hw4_pb2_grpc.KadImplServicer):
	# Takes an IDKey and returns k nodes with distance closest to ID requested
	def FindNode(self, request, context):
		request_id = request.node.id
		request_address = request.node.address
		request_port = request.node.port
		target_id = request.idkey

		print("Serving FindNode({}) request for {}".format(target_id,request_id))

		exclude_nodes = [csci4220_hw4_pb2.Node(id=local_id, port=int(my_port), address=my_address)]
		k_closest_nodes = getKClosestNodesToTargetNode(target_id, exclude_nodes)

		new_node = csci4220_hw4_pb2.Node(id=request_id, port=request_port, address=request_address)
		storeNodeInKBuckets(new_node)

		k_bucket_str = formatKBucketString()
		print("k_bucket_str:\n" + k_bucket_str)

		return csci4220_hw4_pb2.NodeList(
			responding_node = csci4220_hw4_pb2.Node(
				id = local_id,
				port = int(my_port),
				address = my_address),
			nodes = k_closest_nodes)

	# Takes an IDKey
	# If mode_kv is true, then read value returned by kv
	# Else, read from list of k nodes
	def FindValue(self, request, context):
		request_id = request.node.id
		request_address = request.node.address
		request_port = request.node.port
		key = request.idkey

		print("Serving FindKey({}) request for {}".format(key,request_id))

		if node_key == key:
			return csci4220_hw4_pb2.KV_Node_Wrapper(
				responding_node = csci4220_hw4_pb2.Node(
					id = local_id,
					port = int(my_port),
					address = my_address),
				mode_kv = True,
				kv = csci4220_hw4_pb2.KeyValue(
					node = csci4220_hw4_pb2.Node(
						id = local_id,
						port = int(my_port),
						address = my_address),
					key = node_key,
					value = node_value),
				nodes = [])
		else:
			exclude_nodes = [csci4220_hw4_pb2.Node(id=local_id, port=int(my_port), address=my_address)]
			print("Nodes to exclude: " + str(exclude_nodes))
			k_closest_nodes = getKClosestNodesToTargetNode(key, exclude_nodes)
			# print("k_closest_nodes: " + str(k_closest_nodes))
			return csci4220_hw4_pb2.KV_Node_Wrapper(
				responding_node = csci4220_hw4_pb2.Node(
					id = local_id,
					port = int(my_port),
					address = my_address),
				mode_kv = False,
				kv = csci4220_hw4_pb2.KeyValue(
					node = csci4220_hw4_pb2.Node(
						id = local_id,
						port = int(my_port),
						address = my_address),
					key = None,
					value = None),
				nodes = k_closest_nodes)

	# Takes a KeyValue
	# Stores the value at the given node
	# Needs to return something, but client does not use this return value
	def Store(self, request, context):
		request_id = request.node.id
		request_port = request.node.port
		request_address = request.node.address
		print("Received store request from id:{}, port:{}, address:{}".format(request_id, request_port, request_address))
		print("Storing key {} value {}".format(request.key, request.value))
		storeKeyValuePair(request.key, request.value)

		return csci4220_hw4_pb2.IDKey(
			node = csci4220_hw4_pb2.Node(
				id = local_id,
				port = int(my_port),
				address = my_address),
			idkey = idkey_idkey)

	# Takes an IDKey
	# Notifies remote node that the node with the ID in IDKey is quitting the network
	# Remove from the remote node's k_buckets
	# Needs to return something, but client does not use this return value
	def Quit(self, request, context):
		request_id = request.node.id
		request_port = request.node.port
		request_address = request.node.address

		# Try to find quitting node in k_bucket
		found = False
		bucketIndex = 0
		nodeIndex = 0
		for i,bucket in enumerate(k_buckets):
			for j,node in enumerate(bucket):
				if node.id == request_id and node.port == request_port and node.address == request_address:
					found = True
					bucketIndex = i
					nodeIndex = j
					break

		# Evict qutting node from k_bucket should it be in there
		if found == True:
			print("Evicting quitting node {} from bucket {}".format(request_id, bucketIndex))
			k_buckets[bucketIndex].pop(nodeIndex)
		else:
			print("No record of quitting node {} in k-buckets.".format(request_id))

		return csci4220_hw4_pb2.IDKey(
			node = csci4220_hw4_pb2.Node(
				id = local_id,
				port = int(my_port),
				address = my_address),
			idkey = idkey_idkey)

#Finds the k closest nodes to the target_node_id that are not in the list of nodes
#to exclude
def getKClosestNodesToTargetNode(target_node_id, exclude_nodes):
	k_closest_nodes = []
	k_closest_distances = [] #stores the distance for each of the k closest nodes to the target node
	k_closest_nodes_len = 0 #length of the k_closest_nodes array
	for bucket in k_buckets:
		for node in bucket:
			if not node in exclude_nodes:
				if k_closest_nodes_len < k:
					k_closest_nodes.append(node)
					k_closest_distances.append(k_closest_nodes[k_closest_nodes_len].id ^ target_node_id)
					k_closest_nodes_len += 1
				else:
					current_node_distance = node.id ^ target_node_id
					#find the greatest distance of the k_closest_nodes
					largest_distance = max(k_closest_distances)
					largest_distance_index = k_closest_distances.index(largest_distance)
					#swap current node with largest node if current node distance < largest node distance
					if(current_node_distance < largest_distance):
						k_closest_nodes[largest_distance_index] = node
						k_closest_distances[largest_distance_index] = current_node_distance

	return k_closest_nodes

def findClosestNodeForStore(key):
	closest_distance = sys.maxsize
	closest_node = None
	for bucket in k_buckets:
		for node in bucket:
			# print("I'm on this node: " + str(node.id))
			distance = node.id ^ key
			# print("This is the node distance: " + str(distance))
			if(distance < closest_distance):
				closest_distance = distance
				closest_node = node

	current_node_distance = local_id ^ key
	# print("Closest distance: {}. current_node_distance: {}".format(closest_distance,current_node_distance))
	if closest_node is None or current_node_distance < closest_distance:
		return None
	else:
		# print("Found other closest_node: {} with distance: {}".format(closest_node.id, closest_distance))
		return closest_node

#Add the request node to the k_buckets
#Might have to handle a case of fullness (kick something out)
def storeNodeInKBuckets(Node):
	# find the bucket to place the node
	start = 2
	somePower = 0
	result = start**somePower

	xor = local_id ^ Node.id

	# Check if local_id == Node.id
	# Else, perform algorithm
	if xor != 0:
		while result < xor:
			somePower+=1
			result = start**somePower

		if result > xor:
			somePower-=1

	# If node is already in list, make sure it is the most recent
	# else, just add it
	found = False
	foundIndex = 0
	for bucket_node in k_buckets[somePower]:
		if bucket_node.id == Node.id and bucket_node.port == Node.port and bucket_node.address == Node.address:
			found = True
			break

	# Check if the node was found in the list
	if found == True:
		k_buckets[somePower].pop(foundIndex) # pop the node
		k_buckets[somePower].append(Node) # re-append the node
	else:
		if(len(k_buckets[somePower]) < k):
			k_buckets[somePower].append(Node) # add node to the end
		else:
			k_buckets[somePower].pop(0) # Pop least recent node
			k_buckets[somePower].append(Node) # add node to the end


def storeKeyValuePair(key,value):
	global node_key, node_value
	node_key = key
	node_value = value

def formatKBucketString():
	k_bucket_str = ""
	for i, bucket in enumerate(k_buckets):
		k_bucket_str += str(i) + ": "
		for j, node in enumerate(bucket):
			k_bucket_str += str(bucket[j].id) + ':' + str(bucket[j].port)
			if(j != len(bucket) - 1):
				k_bucket_str += ' '
		if(i != len(k_buckets) - 1):
			k_bucket_str += '\n'
	return k_bucket_str

def isInkBuckets(node_id):
	for bucket in k_buckets:
		for node in bucket:
			if node.id == node_id:
				return True
	return False

def handleBootstrapMSG(buffer):
	# print("Received BOOTSTRAP command")
	remote_hostname = buffer.split()[1]
	remote_port = buffer.split()[2]
	#send remote node a find_node RPC (defined in the proto file)
	remote_addr = socket.gethostbyname(remote_hostname)
	print("remote_hostname: {}, remote_addr: {}, remote_port: {}".format(remote_hostname, remote_addr, remote_port))
	#need to establish an insecure channel
	channel = grpc.insecure_channel(remote_addr + ':' + remote_port)
	stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)

	response = stub.FindNode(csci4220_hw4_pb2.IDKey(node=csci4220_hw4_pb2.Node(id=local_id,port=int(my_port),address=my_address)
		, idkey = local_id))

	print("Just received: id:{} address:{} port:{} nodes:{}".format(
		str(response.responding_node.id), response.responding_node.address, str(response.responding_node.port),
		response.nodes))
	#store remote and each of the k closest nodes in k_buckets
	storeNodeInKBuckets(response.responding_node)
	for node in response.nodes:
		storeNodeInKBuckets(node)
	k_bucket_str = formatKBucketString()
	print("After BOOTSTRAP({}) k_buckets now look like:\n{}".format(response.responding_node.id, k_bucket_str))

def handleFindNodeMsg(buffer):
	# print("Received FIND_NODE command")
	print("Before FIND_NODE command, k-buckets are:\n" + formatKBucketString())
	node_id = int(buffer.split()[1])
	print("node_id " + str(node_id))
	if(local_id == node_id):
		print("Found destination id: " + str(node_id))
		kb_str = formatKBucketString()
		print("After FIND_NODE command, k-buckets are:\n{}".format(kb_str))
	else:
		print("Did not find node. searching...")
		#get the k closest nodes to nodeid
		visited_nodes = []
		k_closest_nodes = getKClosestNodesToTargetNode(node_id, [])
		node_was_found = False
		while len(k_closest_nodes) > 0:
			for node in k_closest_nodes:
				visited_nodes.append(node)
				#Might need to check if this is the node we're looking for
				if node.id == node_id:
					node_was_found = True
					break
				#Send a find_node rpc to this node
				#Ask about what node to send
				channel = grpc.insecure_channel(node.address + ':' + str(node.port))
				stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
				response = stub.FindNode(csci4220_hw4_pb2.IDKey(
					node=csci4220_hw4_pb2.Node(id=local_id,port=int(my_port),address=my_address),
					 idkey = node_id))
				R = response.nodes
				storeNodeInKBuckets(node) #mark node as most recent
				#Update k_buckets with all nodes in R
				for response_node in R:
					if response_node.id == node_id:
						node_was_found = True
					if not isInkBuckets(response_node.id):
						storeNodeInKBuckets(response_node)
				if node_was_found:
					break
				else:
					#reevaluate the k closest nodes
					k_closest_nodes = getKClosestNodesToTargetNode(node_id, visited_nodes)
			if node_was_found:
				break

		print("After FIND_NODE command, k-buckets are:\n" + formatKBucketString())
		if node_was_found:
			print("Found destination id " + node_id)
		else:
			print("Could not find destination id " + node_id)

def handleFindValueMsg(buffer):
	# print("Received FIND_VALUE command")
	print("Before FIND_VALUE command, k-buckets are:\n" + formatKBucketString())
	key = int(buffer.split()[1])
	print("key " + str(key))
	if node_key == key:
		print("Found data \"{}\" for key {}".format(node_value,node_key))
	else:
		print("Did not find node. searching...")
		#get the k closest nodes to nodeid
		k_closest_nodes = getKClosestNodesToTargetNode(key, [])
		print("k_closest_nodes outside loop:" + str(k_closest_nodes))
		visited_nodes = []
		node_was_found = False
		value = None
		EXIT_EVERYTHING = False
		while len(k_closest_nodes) > 0:
			print("Entering loop")
			for node in k_closest_nodes:
				print("Inside loop")
				visited_nodes.append(node)
				#Send a find_node rpc to this node
				#Ask about what node to send
				channel = grpc.insecure_channel(node.address + ':' + str(node.port))
				stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
				response = stub.FindValue(csci4220_hw4_pb2.IDKey(
					node=csci4220_hw4_pb2.Node(id=local_id,port=int(my_port),address=my_address),
					 idkey = key))
				R = response.nodes
				storeNodeInKBuckets(node) #mark node as most recent

				#Found the value
				if response.mode_kv:
					print("I'm breaking in response.mode.kv")
					value = response.kv.value
					node_was_found = True
					break

				#Remove itself from the list of k closest nodes
				remove_index = None
				for i, node in enumerate(R):
					if node.id == local_id:
						remove_index = i
						break
				if not remove_index == None:
					R.pop(remove_index)
				print("Response nodes: " + str(R))

				#Update k_buckets with all nodes in R
				for response_node in R:
					print("Response node: " + str(response_node.id))
					if response_node.id == key:
						node_was_found = True
					if not isInkBuckets(response_node.id):
						storeNodeInKBuckets(response_node)

				if node_was_found:
					break
				else:
					#reevaluate the k closest nodes
					k_closest_nodes = getKClosestNodesToTargetNode(key, visited_nodes)
			if node_was_found:
				break

		print("After FIND_VALUE command, k-buckets are:\n" + formatKBucketString())
		if node_was_found:
			print("Found value \"{}\" for key {}".format(value,key))
		else:
			print("Could not find key " + str(key))

def handleStoreMsg(buffer):
	# print("Received STORE command")
	key = int(buffer.split()[1])
	value = buffer.split()[2]
	# print("key: {}, value: {}".format(key,value))
	closest_node = findClosestNodeForStore(key)
	#Already at the closest node
	if closest_node is None:
		print("Storing key {} value {}".format(key,value))
		storeKeyValuePair(key,value)
	else:
		print("Storing key {} at node {}".format(key,closest_node.id))
		channel = grpc.insecure_channel(closest_node.address + ':' + str(closest_node.port))
		stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
		response = stub.Store(csci4220_hw4_pb2.KeyValue(
			node=csci4220_hw4_pb2.Node(
				id=local_id,
				port=int(my_port),
				address=my_address
			),
			key=key,
			value=value
			))
		#Might need to add node to most recent
		print("Just received: id:{} address:{} port:{} key:{}".format(
			str(response.node.id), response.node.address, str(response.node.port),
			str(response.idkey)))

def handleQuitMsg():
	# print("Received QUIT command")
	for bucket in k_buckets:
		for node in bucket:
			remote_id = node.id
			remote_port = node.port
			remote_addr = node.address

			print("Letting {} know I'm quitting.".format(remote_id))

			channel = grpc.insecure_channel(str(remote_addr) + ':' + str(remote_port))
			stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
			response = stub.Quit(csci4220_hw4_pb2.IDKey(
				node = csci4220_hw4_pb2.Node(
					id = local_id,
					port = int(my_port),
					address = my_address),
				idkey = local_id))

	print("Shut down node {}".format(local_id))

def setCommandLineArgs():
	if len(sys.argv) != 4:
		print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
		sys.exit(-1)

	global local_id, my_port, my_address, k

	#Read command line arguments
	local_id = int(sys.argv[1])
	my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
	k = int(sys.argv[3])
	#change back to my_hostname = socket.gethostname() # Gets my host name
	my_hostname = "127.0.0.1"
	my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname

def initializeKBuckets():
	global k_buckets
	k_buckets = [[] for i in range(4)]

def listenForConnections():
	print("gRPC server starting at: {}".format(my_address+':'+my_port))
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))


	csci4220_hw4_pb2_grpc.add_KadImplServicer_to_server(KadImpl(), server)

	server.add_insecure_port(my_address + ':' + my_port)

	server.start()

	try:
	    while True:
	        time.sleep(_ONE_DAY_IN_SECONDS)
	except KeyboardInterrupt:
	    server.stop(0)

def blockOnStdin():
	while(True):
		buffer = input()
		print("MAIN Received from stdin: {}".format(buffer))

		if "BOOTSTRAP" in buffer:
			handleBootstrapMSG(buffer)
		elif "FIND_NODE" in buffer:
			handleFindNodeMsg(buffer)
		elif "FIND_VALUE" in buffer:
			handleFindValueMsg(buffer)
		elif "STORE" in buffer:
			handleStoreMsg(buffer)
		elif "QUIT" in buffer:
			handleQuitMsg()
			sys.exit()
		else:
			print("Invalid command! Please try again!")

if __name__ == '__main__':
	bufferSize = 1024
	setCommandLineArgs()
	initializeKBuckets()
	threading.Thread(target=listenForConnections).start()
	blockOnStdin()
