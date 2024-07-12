# PROBABLY SOLVED :
# -----------------------------
# KABHI KABHI LEADER ELECTION MEI VOTE DE RAHE HAI LEKIN LEADER NHI BAN PAA RAHA ( VOTE RECEIVE COUNT KI VALUE 1 HI REHTI HAI) (shyd race condition issue)
# NO-OP HANDLE KARNA HAI
# KABHI KABHI 2 BAAR APPEND HO JAATA HAI EK HI COMMAND (shyd threading issue)    -> REPLACE KAR RAHA HU PUURI FILE KO WITH THE LATEST SELF.LOGS KYUKI WO SAHI AA RAHE H HAR BAAR
# -----------------------------


# REMAINING ISSUES:
# LOG FILE MEI BAAR BAAR APPEND HO RAHA HAI DOWN HONE KE BAAD PHIRSE ON HONE PAR (mtlb wo saari logs phirse append ho rahi hai jabki sirf latest wali honi chaiye)
# CLIENT SHOULD GET AN ERROR WHEN THE MAJORITY OF NODES DO NOT REPLICATE THE LOG I.E. THE LEADER DOES NOT COMMIT THE LOG
# agar leader lease khatam nhi hui tho client ka write nhi karna

import grpc
import time
import random
import os
import threading
from concurrent import futures
import raft_pb2
import raft_pb2_grpc
leader_lease_time = 10
no_of_nodes = 5
ip_port = ["127.0.0.1:8000", "127.0.0.1:8001",
           "127.0.0.1:8002", "127.0.0.1:8003", "127.0.0.1:8004"]

# PROTO FILE
"""s
syntax = "proto3";

message LogEntry {
    int32 term = 1;
    string operation = 2;
    int32 index = 3;
}

message RequestVoteArgs {
    int32 term = 1;
    int32 candidate_id = 2;
    int32 last_log_index = 3;
    int32 last_log_term = 4;
}

message RequestVoteReply {
    int32 term = 1;
    bool vote_granted = 2;
    int32 remaining_lease = 3;
}

message AppendEntriesArgs {
    int32 term = 1;
    int32 leader_id = 2;
    int32 prev_log_index = 3;
    int32 prev_log_term = 4;
    repeated LogEntry entries = 5;
    int32 leader_commit = 6;
    int32 remaining_lease = 7;
}

message AppendEntriesReply {
    int32 term = 1;
    bool success = 2;
}

message ServeClientArgs {
  string Request = 1;
}

message ServeClientReply {
  string Data = 1;
  string LeaderID = 2;
  bool Success = 3;
}

service Raft {
    rpc RequestVote(RequestVoteArgs) returns (RequestVoteReply) {}
    rpc AppendEntries(AppendEntriesArgs) returns (AppendEntriesReply) {}
    rpc ServeClient (ServeClientArgs) returns (ServeClientReply) {}
}
"""


class Raft(raft_pb2_grpc.RaftServicer):
    def __init__(self, id, ip_port, nodes):
        self.id = id  # Node ID
        self.ip_port = ip_port  # Node IP and port
        self.nodes = nodes  # List of all node addresses
        self.currentTerm = 0  # Current term from this node's perspective
        self.votedFor = None  # Node voted for in current term
        self.logs = []  # Log entries
        self.commitLength = 0  # Index of highest log entry known to be committed
        self.leaderID = None  # Leader ID
        # self.electionTimeout = random.randint(5, 10)  # Election timeout in seconds
        self.electionTimeout = 10
        self.heartbeatTimeout = 1  # Heartbeat timeout in seconds
        self.currentRole = "Follower"  # Current role
        self.voteCount = 0  # Vote count in current election
        self.votesReceived = {}  # Votes received in current election
        self.sentLength = {}  # For each server, index of the next log entry to send
        self.ackedLength = {}  # For each server, highest log entry known to be replicated
        self.ackCount = set()  # Ack count for how many servers have replicated the log
        self.log_file = f"./logs_node_{self.id}/logs.txt"
        self.vote_lock = threading.Lock()
        self.log_file_lock = threading.Lock()             # YAHA
        self.leader_lease = 0

        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        for node in self.nodes:
            self.sentLength[node] = len(self.logs)
            self.ackedLength[node] = len(self.logs)

    def run_election_timer(self):
        while True:
            if self.currentRole == "Follower" or self.currentRole == "Candidate":
                time.sleep(1)
                self.electionTimeout -= 1
                if self.electionTimeout == 0:
                    self.start_election()

    def run_leader_lease_timer(self):
        while True:
            time.sleep(1)
            self.leader_lease -= 1
            print("HERE 1: Leader lease ", self.leader_lease)
            if (self.leader_lease <= 0):
                self.leader_lease = 0
            if self.leader_lease <= 0 and self.currentRole == "Leader":
                self.become_follower()

    def start_election(self):
        self.currentTerm += 1
        self.currentRole = "Candidate"
        self.votedFor = self.id
        self.votesReceived = {self.id}
        self.voteCount = 1
        lastTerm = 0
        if len(self.logs) > 0:
            lastTerm = self.logs[-1].term

        msg = (self.id, self.currentTerm, len(self.logs), lastTerm)
        for node in self.nodes:
            if node != self.ip_port:
                threading.Thread(target=self.request_vote,
                                 args=(node, msg)).start()

        self.reset_election_timeout()

    def request_vote(self, node, msg):
        try:
            channel = grpc.insecure_channel(node)
            stub = raft_pb2_grpc.RaftStub(channel)
            response = stub.RequestVote(raft_pb2.RequestVoteArgs(
                term=msg[1],
                candidate_id=msg[0],
                last_log_index=msg[2],
                last_log_term=msg[3]
            ))
            self.handle_vote_response(response)
        except Exception as e:
            print(f"No response from node: {node}. It is detected to be down.")

    def RequestVote(self, request, context):
        print(
            f"RequestVote received from {request.candidate_id} for term {request.term}")

        if request.term > self.currentTerm:
            self.become_follower(request.term)
        elif request.term < self.currentTerm:
            return raft_pb2.RequestVoteReply(term=self.currentTerm, vote_granted=False, remaining_lease=self.leader_lease)

        lastTerm = 0
        if len(self.logs) > 0:
            lastTerm = self.logs[-1].term
        logOk = (request.last_log_term > lastTerm) or (
            request.last_log_term == lastTerm and request.last_log_index >= len(self.logs))

        if request.term == self.currentTerm and logOk and (self.votedFor is None or self.votedFor == request.candidate_id):
            self.votedFor = request.candidate_id
            print("HERE 5: Voted ", self.votedFor)
            print("HERE 6: leader lease ", self.leader_lease)
            self.reset_election_timeout()
            return raft_pb2.RequestVoteReply(term=self.currentTerm, vote_granted=True, remaining_lease=self.leader_lease)
        return raft_pb2.RequestVoteReply(term=self.currentTerm, vote_granted=False, remaining_lease=self.leader_lease)

    def handle_vote_response(self, response):
        print("HANDLE VOTE RESPONSE")
        print(f"Vote response received: {response.vote_granted}")

        if response.term > self.currentTerm:
            print("BECOME FOLLOWER")
            self.become_follower(response.term)
        elif self.currentRole == "Candidate" and response.term == self.currentTerm and response.vote_granted:
            with self.vote_lock:
                self.votesReceived.add(response.term)
                self.voteCount += 1
                self.leader_lease = max(
                    self.leader_lease, response.remaining_lease)
                print("VOTE COUNT: ", self.voteCount)
                while (True):
                    if self.voteCount >= (len(self.nodes) + 1) // 2 and self.leader_lease <= 0:
                        print("Election won")
                        self.become_leader()
                        break
                    elif self.voteCount >= (len(self.nodes) + 1) // 2 and self.leader_lease > 0:
                        print("Election won but leader lease is not over")
                        time.sleep(1)

    def become_follower(self, term=None):
        self.currentRole = "Follower"
        self.currentTerm = term if term is not None else self.currentTerm
        self.votedFor = None
        self.reset_election_timeout()

    def become_leader(self):
        print(f"Leader elected: {self.id}")
        self.leader_lease = leader_lease_time
        self.currentRole = "Leader"
        self.leaderID = self.id
        self.cancel_election_timer()
        self.ackCount.add(self.id)

        if len(self.logs) == 0 or self.logs[-1].term != self.currentTerm:
            # self.logs.append(raft_pb2.LogEntry(term=self.currentTerm, operation="NO-OP", index=len(self.logs) + 1))
            self.broadcast_message("NO-OP")       # YAHA

        for follower in self.nodes:
            if follower != self.ip_port:
                self.sentLength[follower] = len(self.logs)
                self.ackedLength[follower] = 0
                self.ReplicateLog(follower)

    def reset_election_timeout(self):
        self.electionTimeout = random.randint(5, 10)

    def cancel_election_timer(self):
        self.electionTimeout = float('inf')

    def ServeClient(self, request, context):
        client_ip = "127.0.0.1:5000"
        print("ServeClient received")

        if self.currentRole == "Leader":
            request_formatted = request.Request.split()[0]

            if request_formatted == "SET":
                stringleader = str(self.leaderID)
                msg = f"SET {request.Request.split()[1]} {request.Request.split()[2]}"
                self.broadcast_message(msg)
                return raft_pb2.ServeClientReply(Data="SET request processed", LeaderID=stringleader, Success=True)

            elif request_formatted == "GET":
                # Implement logic to retrieve value from logs
                key = request.Request.split()[1]
                print(f"GET request received for key: {key}")
                stringleader = str(self.leaderID)
                value = None
                for log_entry in reversed(self.logs):
                    operation = log_entry.operation.split()
                    if operation[0] == "SET" and operation[1] == key:
                        value = operation[2]
                        break

                if value is None:
                    print(f"Key {key} not found in logs.")
                    return raft_pb2.ServeClientReply(Data="Key not found", LeaderID=stringleader, Success=False)
                else:
                    print(
                        f"GET request processed for key: {key}, value: {value}")
                    return raft_pb2.ServeClientReply(Data=value, LeaderID=stringleader, Success=True)

        elif self.currentRole == "Follower":
            stringleader = str(self.leaderID)
            if self.leaderID is not None:
                return raft_pb2.ServeClientReply(Data="Forwarding request to leader", LeaderID=stringleader, Success=False)
            else:
                return raft_pb2.ServeClientReply(Data="Leader not known", LeaderID=stringleader, Success=False)

    def broadcast_message(self, msg):
        if self.currentRole == "Leader":
            log_entry = raft_pb2.LogEntry(
                term=self.currentTerm, operation=msg, index=len(self.logs) + 1)
            self.logs.append(log_entry)
            for follower in self.nodes:
                if follower != self.ip_port:
                    self.ackedLength[follower] = len(self.logs)
                    threading.Thread(target=self.ReplicateLog,
                                     args=(follower,)).start()

        else:
            print("Node is not the leader, forwarding request to the leader.")
            # Implement logic to forward the request to the leader
            stringleader = str(self.leaderID)
            if self.leaderID is not None:
                channel = grpc.insecure_channel(self.leaderID)
                stub = raft_pb2_grpc.RaftStub(channel)
                try:
                    response = stub.ServeClient(
                        raft_pb2.ServeClientArgs(Request=msg))
                    return raft_pb2.ServeClientReply(Data=response.Data, LeaderID=stringleader, Success=response.Success)
                except Exception as e:
                    print(f"Error forwarding request to leader: {e}")
                    return raft_pb2.ServeClientReply(Data="Error forwarding request to leader", LeaderID=stringleader, Success=False)
            else:
                return raft_pb2.ServeClientReply(Data="Leader not known", LeaderID=stringleader, Success=False)

    def ReplicateLog(self, follower):
        prefixLen = self.sentLength[follower]
        suffix = self.logs[prefixLen:]
        prefixTerm = 0
        if prefixLen > 0:
            prefixTerm = self.logs[prefixLen - 1].term

        channel = grpc.insecure_channel(follower)
        stub = raft_pb2_grpc.RaftStub(channel)
        try:
            response = stub.AppendEntries(raft_pb2.AppendEntriesArgs(
                term=self.currentTerm,
                leader_id=self.id,
                prev_log_index=prefixLen - 1,
                prev_log_term=prefixTerm,
                entries=suffix,
                leader_commit=self.commitLength,
                remaining_lease=self.leader_lease
            ))
            self.handle_append_entries_response(response, follower)
        except Exception as e:
            print(
                f"Unable to connect to node: {follower}. It is detected to be down.")

    def AppendEntries(self, request, context):
        print("AppendEntries received")

        if request.term > self.currentTerm:
            self.become_follower(request.term)
            self.leaderID = request.leader_id
        elif request.term < self.currentTerm:
            return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False)

        self.reset_election_timeout()
        self.leaderID = request.leader_id
        prefixLen = request.prev_log_index + 1

        logOk = (len(self.logs) >= prefixLen) and (
            prefixLen == 0 or self.logs[prefixLen - 1].term == request.prev_log_term)
        self.leader_lease = request.remaining_lease
        if request.term == self.currentTerm and logOk:
            self.AppendEntries_helper(
                prefixLen, request.leader_commit, request.entries)
            ack = prefixLen + len(request.entries)
            return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=True)
        else:
            return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False)

    def AppendEntries_helper(self, prefixLen, leaderCommit, suffix):
        if len(suffix) > 0 and len(self.logs) > prefixLen:
            index = min(len(self.logs), prefixLen + len(suffix)) - 1
            if self.logs[index].term != suffix[index - prefixLen].term:
                self.logs = self.logs[:prefixLen]

        if prefixLen + len(suffix) > len(self.logs):
            for i in range(len(self.logs) - prefixLen, len(suffix)):
                self.logs.append(suffix[i])

        if leaderCommit > self.commitLength:
            for i in range(self.commitLength, leaderCommit):
                threading.Thread(target=self.deliver_log_entry, args=(
                    self.logs[i],)).start()
                # self.deliver_log_entry(self.logs[i])
            self.commitLength = leaderCommit

    def deliver_log_entry(self, log_entry):
        with self.log_file_lock:
            print(f"Delivering log entry: {log_entry.operation}")
            # Implement logic to deliver the log entry to the application
            # with open(self.log_file, "a") as f:
            #     f.write(f"{log_entry.operation} {log_entry.term}\n")

            # DOUBT MEI HAI YE CHEEZ ABHI
            # if (self.currentRole == "Leader"):
            with open(self.log_file, "w") as f:          # YAHA
                for log in self.logs:
                    f.write(f"{log.operation} {log.term}\n")

    def handle_append_entries_response(self, response, follower):
        if response.term > self.currentTerm:
            self.become_follower(response.term)
        elif self.currentRole == "Leader":
            if response.success and response.term == self.currentTerm:
                self.ackCount.add(follower)
                if len(self.ackCount) >= (len(self.nodes) + 1) // 2:
                    print("Majority acks received")
                    self.ackedLength[follower] = max(
                        self.ackedLength[follower], len(self.logs))
                    self.leader_lease = leader_lease_time
                    self.CommitLogEntries()
                    self.ackCount.clear()
                    self.ackCount.add(self.leaderID)
                # else:
                #     print("Not enough acks received")

            else:
                if self.sentLength[follower] > 0:
                    self.sentLength[follower] -= 1
                self.ReplicateLog(follower)

    def CommitLogEntries(self):
        minAcks = (len(self.nodes) + 1) // 2
        acks = {node: length for node, length in self.ackedLength.items()
                if length >= len(self.logs)}
        if (len(acks) >= minAcks) and (max(acks.values()) > self.commitLength) and (self.logs[max(acks.values()) - 1].term == self.currentTerm):
            for i in range(self.commitLength, max(acks.values())):
                # make threads for delivering log entries
                threading.Thread(target=self.deliver_log_entry, args=(
                    self.logs[i],)).start()

                # self.deliver_log_entry(self.logs[i])
            self.commitLength = max(acks.values())

    def periodic_tasks(self):
        while True:
            time.sleep(self.heartbeatTimeout)
            if self.currentRole == "Leader":
                for follower in self.nodes:
                    if follower != self.ip_port:
                        threading.Thread(target=self.ReplicateLog, args=(
                            follower,)).start()


def serve():
    print("Server started")
    id = int(input("Enter node id: "))
    print(ip_port[id])

    raft_node = Raft(id, ip_port[id], ip_port)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    raft_pb2_grpc.add_RaftServicer_to_server(raft_node, server)
    print(f'ip_port[id]: {ip_port[id]}')
    server.add_insecure_port(ip_port[id])
    server.start()

    try:
        with open(raft_node.log_file, "r") as f:                # YAHA
            for line in f:
                line = line.strip().split()
                term = int(line[-1])
                operation = " ".join(line[:-1])
                raft_node.logs.append(raft_pb2.LogEntry(
                    term=term, operation=operation, index=len(raft_node.logs) + 1))
    except FileNotFoundError:
        print("No previous logs found.")

    # Deleting previous log file after writing logs to logs list
    try:
        os.remove(raft_node.log_file)
    except FileNotFoundError:
        pass

    print("Server started at " + ip_port[id])
    threading.Thread(target=raft_node.run_election_timer).start()
    threading.Thread(target=raft_node.periodic_tasks).start()
    threading.Thread(target=raft_node.run_leader_lease_timer).start()

    while True:
        time.sleep(86400)


print("start")
serve()

# client ko infinitely taal doon
# log ek kam karke bhej de
# log_file_lock
# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
