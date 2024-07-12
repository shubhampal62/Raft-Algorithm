# # THIS IS THE PROTO FILE :
# # syntax = "proto3";

# # message LogEntry {
# #     int32 term = 1;
# #     string operation = 2;
# #     int32 index = 3;
# # }

# # message RequestVoteArgs {
# #     int32 term = 1;
# #     int32 candidate_id = 2;
# #     int32 last_log_index = 3;
# #     int32 last_log_term = 4;
# # }

# # message RequestVoteReply {
# #     int32 term = 1;
# #     bool vote_granted = 2;
# # }

# # message AppendEntriesArgs {
# #     int32 term = 1;
# #     int32 leader_id = 2;
# #     int32 prev_log_index = 3;
# #     int32 prev_log_term = 4;
# #     repeated LogEntry entries = 5;
# #     int32 leader_commit = 6;
# # }

# # message AppendEntriesReply {
# #     int32 term = 1;
# #     bool success = 2;
# # }

# # message ServeClientArgs {
# #   string Request = 1;
# # }

# # message ServeClientReply {
# #   string Data = 1;
# #   string LeaderID = 2;
# #   bool Success = 3;
# # }

# # service Raft {
# #     rpc RequestVote(RequestVoteArgs) returns (RequestVoteReply) {}
# #     rpc AppendEntries(AppendEntriesArgs) returns (AppendEntriesReply) {}
# #     rpc ServeClient(ServeClientArgs) returns (ServeClientReply) {}
# # }

# # Storage and Database Operations
# # As mentioned in the introduction, we are building a database that stores key-value pairs mapping string (key) to string (value). The Raft Nodes serve the client for storing and retrieving these key-value pairs and replicating this data among other nodes in a fault-tolerant manner.


# # The data (logs) will only store all the WRITE OPERATIONS and NO-OP operations as it is, along with the term number mentioned in the Logs section. An example log.txt file:

# # NO-OP 0
# # SET name1 Jaggu 0 [SET {key} {value} {term}]
# # SET name2 Raju 0
# # SET name3 Bheem 1

# # The operations supported for the client on this database are as follows:

# # SET K V: Maps the key K to value V; for example, {SET x hello} will map the key “x” to value “hello.” (WRITE OPERATION)
# # GET K: Returns the latest committed value of key K. If K doesn’t exist in the database, an empty string will be returned as value by default. (READ OPERATION)

# # Apart from the abovementioned operations, a node can also initiate an empty instruction known as the NO-OP operation. This operation has been elaborated on in the Election Functionalities section.


# # 3. Client Interaction
# # Followers, candidates, and a leader form a Raft cluster serving Raft clients. A Raft client implements the following functionality:

# # Leader Information:
# # The client stores the IP addresses and ports of all the nodes.
# # The client stores the current leader ID, although this information might get outdated.
# # Request Server:
# # It sends a GET/SET request to the leader node. (Refer to the Storage and Database Operations section)
# # In case of a failure, it updates its leader ID and resends the request to the updated leader.
# # The node returns what it thinks is the current leader and a failure message
# # If there is no leader in the system, then the node will return NULL for the current leader and a failure message
# # The client continues sending the request until it receives a SUCCESS reply from any node.


# import grpc
# import raft_pb2
# import raft_pb2_grpc
# import time
# import sys
# import random
# import threading


# no_of_nodes = 5
# ip_port = ["127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003","127.0.0.1:8004"]


# class Client:
#     def __init__(self, ip, port, nodes, name):
#         self.ip = ip
#         self.port = port
#         self.nodes = nodes
#         self.name = name
#         self.leader = None
#         self.client_id = str(random.randint(1, 1000))
#         self.channels = [grpc.insecure_channel(node) for node in ip_port]
#         self.start_client()

#     def start_client(self):
#         while True:
#             print("Enter the operation (GET/SET) and the key value pair (key value) separated by space")

#             # FORMAT:-
#             # SET K V: Maps the key K to value V; for example, {SET x hello} will map the key “x” to value “hello.” (WRITE OPERATION)
#             # GET K: Returns the latest committed value of key K. If K doesn’t exist in the database, an empty string will be returned as value by default. (READ OPERATION)
#             input_string = input()
#             operation = input_string.split(" ")[0]
#             key = input_string.split(" ")[1]
#             if operation == "GET":
#                 response = self.Get(key)
#                 print(response.Data)
#             elif operation == "SET":
#                 value = input_string.split(" ")[2]
#                 response = self.Set(key, value)
#                 print(response.Data)


#     def Get(self, key):
#         while True:

#             if self.leader is None:
#                 for channel in self.channels:
#                     response = raft_pb2_grpc.RaftStub(channel).ServeClient(raft_pb2.ServeClientArgs(Request="GET " + key))
#                     if response.Success:
#                         self.leader = response.LeaderID
#                         return response
#                     elif response.LeaderID is not None:
#                         self.leader = response.LeaderID
#             else:
#                 response = raft_pb2_grpc.RaftStub(self.channels[int(self.leader)]).ServeClient(raft_pb2.ServeClientArgs(Request="GET " + key))
#                 if response.Success:
#                     return response
#                 else:
#                     self.leader = response.LeaderID

#     def Set(self, key, value):
#         while True:
#             if self.leader is None:
#                 for channel in self.channels:
#                     response = raft_pb2_grpc.RaftStub(channel).ServeClient(raft_pb2.ServeClientArgs(Request="SET " + key + " " + value))
#                     if response.Success:
#                         self.leader = response.LeaderID
#                         return response
#                     elif response.LeaderID is not None:
#                         self.leader = response.LeaderID
#             else:
#                 response = raft_pb2_grpc.RaftStub(self.channels[int(self.leader)]).ServeClient(raft_pb2.ServeClientArgs(Request="SET " + key + " " + value))
#                 if response.Success:
#                     return response
#                 else:
#                     self.leader = response.LeaderID


# def main():
#     client = Client(ip = "127.0.0.1:5000", port = 5000, nodes = no_of_nodes, name = "client")
#     client.start_client()

# if __name__ == "__main__":
#     main()


import grpc
import raft_pb2
import raft_pb2_grpc
import random
import threading

no_of_nodes = 5
ip_port = ["127.0.0.1:8000", "127.0.0.1:8001",
           "127.0.0.1:8002", "127.0.0.1:8003", "127.0.0.1:8004"]


class Client:
    def __init__(self, ip, port, nodes, name):
        self.ip = ip
        self.port = port
        self.nodes = nodes
        self.name = name
        self.leader = None
        self.client_id = str(random.randint(1, 1000))
        self.channels = [grpc.insecure_channel(node) for node in ip_port]
        self.stubs = [raft_pb2_grpc.RaftStub(
            channel) for channel in self.channels]
        self.start_client()

    def start_client(self):
        while True:
            print(
                "Enter the operation (GET/SET) and the key-value pair (key value) separated by space")
            input_string = input().split()
            operation = input_string[0]
            key = input_string[1]

            if operation == "GET":
                response = self.Get(key)
                print(response.Data)
            elif operation == "SET" and len(input_string) == 3:
                value = input_string[2]
                response = self.Set(key, value)
                print(response.Data)
            else:
                print("Invalid input format.")

    def Get(self, key):
        return self.__send_request(f"GET {key}")

    def Set(self, key, value):
        return self.__send_request(f"SET {key} {value}")

    def __send_request(self, request):
        while True:
            if self.leader is None or self.leader == "None":
                for i, stub in enumerate(self.stubs):
                    try:
                        response = stub.ServeClient(
                            raft_pb2.ServeClientArgs(Request=request))
                        print("I am here")
                        if response.Success:
                            self.leader = str(response.LeaderID)
                            print("Leader is: ", self.leader)
                            print("Response is: ", response.Data)
                            return response
                        elif response.LeaderID:
                            print("New leader found: ",
                                  response.LeaderID)
                            self.leader = response.LeaderID
                            break
                    except grpc.RpcError as e:
                        # print("ERROR IS: ", e)
                        print("unable to connect to the node: ",
                              i, " it maybe down")
                        print("trying to connect to the next node...")

            else:
                try:
                    response = self.stubs[int(self.leader)].ServeClient(
                        raft_pb2.ServeClientArgs(Request=request))
                    if response.Success:
                        print("Leader is: ", self.leader)
                        print("Response is: ", response.Data)
                        return response
                    else:
                        self.leader = None if not response.LeaderID else response.LeaderID
                except grpc.RpcError as e:
                    self.leader = None


def main():
    client = Client(ip="127.0.0.1:5000", port=5000,
                    nodes=no_of_nodes, name="client")
    client.start_client()


if __name__ == "__main__":
    main()
