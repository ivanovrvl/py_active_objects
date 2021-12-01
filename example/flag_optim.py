import sys
import os
import datetime
sys.path.append(os.path.abspath('..'))
from active_objects import ActiveObjectsController, ActiveObject, simple_loop, emulate_asap, SignalPub, SignalSub, Flag, FlagListener

can_print = Flag()

class PrintAO(ActiveObject):

    def __init__(self, controller, id, pub:SignalPub):
        super().__init__(controller)
        self.next_print = None
        self.flag = FlagListener(self)
        self.id = id
        self.signal() # auto start

    def process(self):
        if self.reached(self.next_print):
            if self.flag.is_up(can_print):
                print(self.now(), self.id)
                can_print.down()
                self.next_print = self.schedule_seconds(5)

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
            can_print.up(notify_all=False)
            print(self.now(), 'signal')
            self.next1 = self.schedule_seconds(10)

        if can_print.notify():
            self.signal()

controller = ActiveObjectsController()
publisher_ao = PublisherAO(controller)
print_ao1 = PrintAO(controller, 1, publisher_ao.pub)
print_ao2 = PrintAO(controller, 2, publisher_ao.pub)
print_ao2 = PrintAO(controller, 3, publisher_ao.pub)

#simple_loop(controller)
emulate_asap(controller, datetime.datetime(year=2000, month=1, day=1))
