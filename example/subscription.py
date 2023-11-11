import sys
import os
import datetime
sys.path.append(os.path.abspath('..'))
from active_objects import ActiveObjectsController, ActiveObject, simple_loop, emulate_asap, Signaler, AOListener

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
            self.stop_time = self.schedule_seconds(60)
        elif self.reached(self.stop_time):
            self.controller.terminate()
            print(self.now(), 'stop')
            return

        if self.reached(self.next1):
            self.event.signalAll() # Notify all subscribers by setting theirs sub signals
            print(self.now(), 'signal')
            self.next1 = self.schedule_seconds(5)

    def close(self):
        self.event.close()
        super().close()

class PrintAO(ActiveObject):

    def __init__(self, controller, id, event: Signaler):
        super().__init__(controller)
        self.event = event
        self.listen = AOListener(self)
        self.listen.wait(self.event)
        self.id = id
        self.signal() # auto start

    def process(self):
        if self.listen.check(self.event):
            # do something if sub signal was set
            print(self.now(), self.id)

    def close(self):
        self.listen.close()
        super().close()

controller = ActiveObjectsController()
publisher_ao = PublisherAO(controller)
print_ao1 = PrintAO(controller, 1, publisher_ao.event)
print_ao2 = PrintAO(controller, 2, publisher_ao.event)

simple_loop(controller)
#emulate_asap(controller, datetime.datetime(year=2000, month=1, day=1))
