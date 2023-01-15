import sys
import os
import datetime
sys.path.append(os.path.abspath('..'))
from active_objects import ActiveObjectsController, ActiveObject, simple_loop, emulate_asap, Signaler, Listener

next_print: datetime.datetime = None # global lock to delay printing

class PublisherAO(ActiveObject):

    def __init__(self, controller):
        super().__init__(controller)
        self.stop_time = None
        self.next1 = None
        self.event = Signaler()
        self.signal() # auto start

    def process(self):

        # stop in 60 seconds
        if self.stop_time is None:
            self.stop_time = self.schedule_seconds(30)
        elif self.reached(self.stop_time):
            self.controller.terminate()
            print(self.now(), 'stop')
            return

        if self.reached(self.next1):
            self.event.signalAll()
            print(self.now(), 'signal')
            self.next1 = self.schedule_seconds(10)

    def close(self):
        self.event.close()
        super().close()

class PrintAO(ActiveObject):

    def __init__(self, controller, id, pub:PublisherAO):
        super().__init__(controller)
        self.pub = pub
        self.listen = Listener(self)
        self.listen.wait(self.pub.event)
        self.id = id
        self.signal() # auto start

    def process(self):
        global next_print
        if self.listen.is_signaled(): # is print requested
            if self.reached(next_print): # is print allowed
                print(self.now(), self.id)
                self.listen.wait(self.pub.event)
                # delay any next print for a second
                next_print = self.now() + datetime.timedelta(seconds=1)

    def close(self):
        self.listen.close()
        super().close()


controller = ActiveObjectsController()
publisher_ao = PublisherAO(controller)
print_ao1 = PrintAO(controller, 1, publisher_ao)
print_ao2 = PrintAO(controller, 2, publisher_ao)
print_ao2 = PrintAO(controller, 3, publisher_ao)

simple_loop(controller)
#emulate_asap(controller, datetime.datetime(year=2000, month=1, day=1))
