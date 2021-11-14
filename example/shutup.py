import sys
import os
import datetime
sys.path.append(os.path.abspath('..'))
from active_objects import ActiveObjectsController, ActiveObject, simple_loop, emulate_asap

print_ao = None
shutup_ao = None

class PrintAO(ActiveObject):

    def __init__(self, controller):
        super().__init__(controller)
        self.next1 = None
        self.n = 0
        self.signal() # auto start
        self.is_shutup = False

    def process(self):
        if self.is_shutup != shutup_ao.shutup:
            print(self.now(), 'Shutup', shutup_ao.shutup)
            self.is_shutup = shutup_ao.shutup
        if not shutup_ao.shutup:
            if self.reached(self.next1):
                self.n += 1
                print(self.now(), self.n)
                self.next1 = self.schedule_seconds(1)

class ShutupAO(ActiveObject):

    def __init__(self, controller):
        super().__init__(controller)
        self.stop_time = None
        self.next1 = None
        self.shutup = True
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
            self.shutup = not self.shutup
            print_ao.signal() # Notify change
            self.next1 = self.schedule_seconds(9.5)


controller = ActiveObjectsController()
shutup_ao = ShutupAO(controller)
print_ao = PrintAO(controller)

#simple_loop(controller)
emulate_asap(controller, datetime.datetime(year=2000, month=1, day=1))
