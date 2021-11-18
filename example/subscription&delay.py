import sys
import os
import datetime
sys.path.append(os.path.abspath('..'))
from active_objects import ActiveObjectsController, ActiveObject, simple_loop, emulate_asap, SignalPub, SignalSub

next_print: datetime.datetime = None # global lock to delay printing

class PrintAO(ActiveObject):

    def __init__(self, controller, id, pub:SignalPub):
        super().__init__(controller)
        self.sub = SignalSub(self, edge=True, pub=pub)
        self.id = id
        self.signal() # auto start

    def process(self):
        global next_print
        if self.sub.is_set: # is print requested
            if self.reached(next_print): # is print allowed
                print(self.now(), self.id)
                self.sub.reset() # clear print request
                # delay any next print for a second
                next_print = self.now() + datetime.timedelta(seconds=1)

class PublisherAO(ActiveObject):

    def __init__(self, controller):
        super().__init__(controller)
        self.stop_time = None
        self.next1 = None
        self.pub = SignalPub()
        self.signal() # auto start

    def process(self):

        # stop in 60 seconds
        if self.stop_time is None:
            self.stop_time = self.schedule_seconds(60)
        elif self.reached(self.stop_time):
            self.controller.terminate()
            print(self.now(), 'stop')
            return

        if self.reached(self.next1):
            self.pub.signal() # Notify all subscribers by setting theirs sub signals
            print(self.now(), 'signal')
            self.next1 = self.schedule_seconds(10)

controller = ActiveObjectsController()
publisher_ao = PublisherAO(controller)
print_ao1 = PrintAO(controller, 1, publisher_ao.pub)
print_ao2 = PrintAO(controller, 2, publisher_ao.pub)
print_ao2 = PrintAO(controller, 3, publisher_ao.pub)

#simple_loop(controller)
emulate_asap(controller, datetime.datetime(year=2000, month=1, day=1))
