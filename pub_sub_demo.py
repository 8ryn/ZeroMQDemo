import zmq
import sys
import threading
import time
import random
from datetime import datetime, timezone


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
        self.timing_channel_workers = {}
        self.timing_channel_workers["0"] = []
        self.timing_channel_workers["1"] = []
        for i in range(4):
            channel = str(i % 2)
            worker = demoLocalWorker(
                self.id * 100 + i, channel
            )  # Channel set based on id for demo
            self.timing_channel_workers[channel].append(worker)

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        identity = "follower-%d" % self.id
        socket.connect("tcp://localhost:5570")
        print("Client %s started" % (identity))

        for channel in self.timing_channel_workers.keys():
            socket.setsockopt_string(zmq.SUBSCRIBE, channel)

        while True:
            msg = socket.recv_string()
            channel, time_str = msg.split()

            tprint("Client %s received: %s" % (identity, msg))
            if channel in self.timing_channel_workers.keys():
                timestamp = datetime.fromisoformat(time_str)
                for worker in self.timing_channel_workers[channel]:
                    worker.call(timestamp)

            # Using recv_multipart
            # msg = socket.recv_multipart()
            # tprint("Client %s received: %s" % (identity, msg))
            # channel = msg[0].decode()
            # if channel in self.timing_channel_workers.keys():
            #    timestamp = datetime.fromisoformat(msg[1].decode())
            #    for worker in self.timing_channel_workers[channel]:
            #        worker.call(timestamp)

        socket.close()
        context.term()


class ServerAllInOne:
    """ServerAllInOne"""

    def __init__(self):
        self.timingChannels = ["0", "1"]
        context = zmq.Context()
        self.socket = context.socket(zmq.PUB)
        self.socket.bind("tcp://*:5570")
        time.sleep(1)
        self.timing_thread = threading.Thread(target=self.timing_loop)
        self.timing_thread.start()

    def timing_loop(self):
        """Timing loop

        Equivalent of the main timing loop.
        Since we have no specific periods to call workers in this demo just call them at random
        """
        while True:

            timing_channel = random.choice(self.timingChannels)
            print("Randomly selected channel %s" % (timing_channel))
            calltime = datetime.now(timezone.utc)

            # Using send_string
            self.socket.send_string(f"{timing_channel} {calltime.isoformat()}")

            # Using send_multipart
            # msg = [timing_channel.encode(), calltime.isoformat().encode()]
            # self.socket.send_multipart(msg)

            time.sleep(5)


class demoLocalWorker:

    def __init__(self, id: int, channel: str = "default") -> None:
        self.id = id
        self.channel = channel

    def call(self, timestamp: datetime):
        print(
            "Local worker called. Timestamp = %s ID = %s Channel = %s"
            % (timestamp, self.id, self.channel)
        )


def main():
    """main function"""
    for i in range(3):
        client = CompClientTask(i)
        client.start()

    server = ServerAllInOne()
    server.timing_thread.join()


if __name__ == "__main__":
    main()
