import sys
import os
import datetime
sys.path.append(os.path.abspath('..'))
from active_objects import ActiveObjectsController, ActiveObjectWithRetries, simple_loop, emulate_asap

class TestAO(ActiveObjectWithRetries):

    def __init__(self, controller):
        super().__init__(controller)
        self.max_retry_interval = 10
        self.stop_time = None
        self.next1 = None
        self.next2 = None
        self.signal() # auto start

    def _process(self):

        # emulate error to check WithRetries
        # raise Exception("error")

        # stop in 60 seconds
        if self.stop_time is None:
            self.stop_time = self.schedule_seconds(60)
        elif self.reached(self.stop_time):
            self.controller.terminate()
            print(self.now(), 'stop')
            return

        # each 3 seconds
        if self.reached(self.next1):
            print(self.now(), "3")
            self.next1 = self.schedule_seconds(3)

        # each 4 seconds
        if self.reached(self.next2):
            print(self.now(), "4")
            self.next2 = self.schedule_seconds(4)

    def process_internal(self):
        try:
            # continue on error (with retries)
            super().process_internal()
        except Exception as e:
            print(self.now(), str(e))

controller = ActiveObjectsController()
ao = TestAO(controller)

simple_loop(controller)
#emulate_asap(controller, datetime.datetime(year=2000, month=1, day=1))

