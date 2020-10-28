# DHT
Kad DHT Implementation

to create pb2.py file and pb2_grpc.py file...

Quickstart gRPC: https://grpc.io/docs/languages/python/quickstart/

To install necessary packages: 
'''
pip install -r requirements.txt
'''

run (in bash):
'''
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./csci4220_hw4.proto
'''
