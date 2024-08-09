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
            worker = demoLocalWorker(self.id * 100 + i)
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
        reqs = 0
        # Code making the initial registration with the server:
        print("Client %s sending registration request" % (identity))
        reg_msg = "register"
        for worker in self.workersDict.values():
            reg_msg += " " + str(worker.id)

        socket.send_string(reg_msg)

        while True:
            sockets = dict(poll.poll(1000))
            if socket in sockets:
                msg = socket.recv()
                tprint("Client %s received: %s" % (identity, msg))
                split_msg = msg.split(b" ")
                if split_msg[0] == b"call":
                    # TODO: Various error checking should probably be here really
                    id = int(split_msg[1].decode())
                    self.workersDict[id].call()

        socket.close()
        context.term()


class ServerAllInOne:
    """ServerAllInOne"""

    def __init__(self):
        self.workersDict = {}
        self.registration_thread = threading.Thread(
            target=self.registration_loop, daemon=True
        )
        self.registration_thread.start()
        self.timing_thread = threading.Thread(target=self.timing_loop)
        self.timing_thread.start()

    def registration_loop(self):
        context = zmq.Context()
        server = context.socket(zmq.ROUTER)
        server.bind("tcp://*:5570")
        tprint("Server started")

        while True:
            ident, msg = server.recv_multipart()
            tprint("Server received %s from %s" % (msg, ident))
            split_msg = msg.split(b" ")
            if split_msg[0] == b"register":
                tprint("Registering %s" % (ident))
                for b_id in split_msg[1:]:
                    id = int(
                        b_id.decode()
                    )  # Converting byte to str to in to int seems non-ideal
                    worker = demoRemoteWorker(id, server, ident)
                    # TODO: Probably need a lock here (plan for actual implementation is to have a separate dictionary of workers to be added which are added at end of timing loop to avoid constant locking)
                    self.workersDict[id] = worker
                    print("Registered worker %s" % (id))
                reply = b"registered"
                server.send_multipart([ident, reply])
            elif split_msg[0] == b"deregister":
                tprint("Deregistering %s" % (ident))
                for b_id in split_msg[1:]:
                    id = int(b_id.decode())
                    self.workersDict.pop(id)
                    print("Deregistered worker %s" % (id))
                reply = b"deregistered"
                server.send_multipart([ident, reply])
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
            time.sleep(5)


class demoLocalWorker:

    def __init__(self, id: int) -> None:
        self.id = id

    def call(self):
        print("Local worker called. ID = %s" % (self.id))


class demoRemoteWorker:

    def __init__(self, id: int, socket: zmq.SyncSocket, ident: bytes) -> None:
        self.id = id
        self.socket = socket
        self.ident = ident

    def call(self):
        print("Remote worker called. ID = %s" % (self.id))
        msg = "call " + str(self.id)
        msg_bytes = msg.encode()
        self.socket.send_multipart([self.ident, msg_bytes])


def main():
    """main function"""
    server = ServerAllInOne()
    for i in range(3):
        client = CompClientTask(i)
        client.start()

    server.registration_thread.join()


if __name__ == "__main__":
    main()
