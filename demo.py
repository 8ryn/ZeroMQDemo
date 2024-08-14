import zmq
import sys
import threading
import time
import random


def tprint(msg):
    """like print, but won't get newlines confused with multiple threads"""
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


class CompClientTask(threading.Thread):
    """ClientTask

    These are the Followers
    """

    def __init__(self, id):
        self.id = id
        threading.Thread.__init__(self)
        self.workersDict = {}
        for i in range(4):
            worker = demoLocalWorker(
                self.id * 100 + i, self.id * 3 + i
            )  # Period set based on id for demo
            self.workersDict[worker.id] = worker

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        identity = "follower-%d" % self.id
        socket.identity = identity.encode("ascii")
        socket.connect("tcp://localhost:5570")
        print("Client %s started" % (identity))
        poll = zmq.Poller()
        poll.register(socket, zmq.POLLIN)
        # Code making the initial registration with the server:
        print("Client %s sending registration request" % (identity))
        reg_msg = [b"register"]
        for worker in self.workersDict.values():
            reg_msg.append(str(worker.id).encode())
            reg_msg.append(str(worker.period).encode())

        socket.send_multipart(reg_msg)

        while True:
            sockets = dict(poll.poll(1000))
            if socket in sockets:
                msg = socket.recv_multipart()
                tprint("Client %s received: %s" % (identity, msg))
                if msg[0] == b"call":
                    # TODO: Various error checking should probably be here really
                    id = int(msg[1].decode())
                    self.workersDict[id].call()
                elif msg[0] == b"heartbeat_ping":
                    socket.send_string("heartbeat_pong")

        socket.close()
        context.term()


class ServerAllInOne:
    """ServerAllInOne"""

    def __init__(self):
        self.workersDict = {}
        self.followerAlive: dict[bytes, bool] = {}
        self.registration_thread = threading.Thread(
            target=self.registration_loop, daemon=True
        )
        self.registration_thread.start()
        self.timing_thread = threading.Thread(target=self.timing_loop)
        self.timing_thread.start()

    def registration_loop(self):
        context = zmq.Context()
        self.server = context.socket(zmq.ROUTER)
        self.server.bind("tcp://*:5570")
        tprint("Server started")

        while True:
            msg = self.server.recv_multipart()
            ident = msg[0]
            tprint("Server received %s from %s" % (msg, ident))
            if msg[1] == b"register":
                tprint("Registering %s" % (ident))
                if msg[2:]:
                    self.followerAlive[ident] = True
                    for b_id, b_period in zip(msg[2::2], msg[3::2]):
                        id = int(
                            b_id.decode()
                        )  # Converting byte to str to in to int seems non-ideal
                        period = int(b_period.decode())
                        worker = demoRemoteWorker(id, self.server, ident, period)
                        # TODO: Probably need a lock here (plan for actual implementation is to have a separate dictionary of workers to be added which are added at end of timing loop to avoid constant locking)
                        self.workersDict[id] = worker
                        print("Registered worker %s with period %s" % (id, period))
                    reply = b"registered"
                    self.server.send_multipart([ident, reply])
            elif (
                msg[1] == b"deregister"
            ):  # Currently you could deregister every worker from a follower but would not be removed from followersAlive dict
                tprint("Deregistering %s" % (ident))
                for b_id in msg[2:]:
                    id = int(b_id.decode())
                    self.workersDict.pop(id)
                    print("Deregistered worker %s" % (id))
                reply = b"deregistered"
                self.server.send_multipart([ident, reply])
            elif msg[1] == b"heartbeat_pong":
                self.followerAlive[ident] = True
            else:
                tprint("Invalid message")
        context.term()

    def timing_loop(self):
        """Timing loop

        Equivalent of the main timing loop.
        Since we have no specific periods to call workers in this demo just call them at random
        """
        while True:
            if self.workersDict:
                remote_worker = random.choice(list(self.workersDict.values()))
                print("Randomly selected worker %s" % (remote_worker.id))
                remote_worker.call()

            for ident in self.followerAlive:
                if not self.followerAlive[ident]:
                    print("Follower %s not responding" % (ident))
                    # The heartbeat doesn't currently achieve anything other than alerting that there is an issue but I think that is valuable
                else:
                    self.followerAlive[ident] = False
                self.server.send_multipart([ident, b"heartbeat_ping"])
            time.sleep(5)


class demoLocalWorker:

    def __init__(self, id: int, period: int = 60) -> None:
        self.id = id
        self.period = period

    def call(self):
        print("Local worker called. ID = %s Period = %s" % (self.id, self.period))


class demoRemoteWorker:

    def __init__(
        self, id: int, socket: zmq.SyncSocket, ident: bytes, period: int = 60
    ) -> None:
        self.id = id
        self.socket = socket
        self.ident = ident
        self.period = period

    def call(self):
        print("Remote worker called. ID = %s Period = %s" % (self.id, self.period))
        msg = [self.ident, b"call", str(self.id).encode()]
        self.socket.send_multipart(msg)


def main():
    """main function"""
    server = ServerAllInOne()
    for i in range(3):
        client = CompClientTask(i)
        client.start()

    server.registration_thread.join()


if __name__ == "__main__":
    main()
