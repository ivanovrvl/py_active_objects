# This code is under MIT licence, you can find the complete file here: https://github.com/ivanovrvl/py_active_objects/blob/main/LICENSE

import avl_tree
import linked_list
from datetime import datetime, timedelta

class ActiveObject:

    type_id=None
    priority:int=0

    def __init__(self, controller, id=None):
        self.t:datetime = None
        self.id = id
        self.controller = controller
        self._tree_by_t = avl_tree.TreeNode(self)
        self._tree_by_id = avl_tree.TreeNode(self)
        self._signaled = linked_list.DualLinkedListItem(self)
        if id is not None and self.type_id is not None:
            controller._tree_by_id.add(self._tree_by_id)
        self.signal()

    def _process(self):
        pass

    def _process_internal(self):
        self._process()

    def is_signaled(self) -> bool:
        return self._signaled.in_list()

    def is_scheduled(self) -> bool:
        return self._tree_by_t.in_tree()

    def schedule(self, t:datetime):
        if t is not None:
            if not self._tree_by_t.in_tree() or t < self.t:
                self.controller._tree_by_t.remove(self._tree_by_t)
                self.t = t
                self.controller._tree_by_t.add(self._tree_by_t)

    def schedule_delay(self, delay:timedelta)->datetime:
        t = self.controller.now() + delay
        self.schedule(t)
        return t

    def schedule_milliseconds(self, delay)->datetime:
        return self.schedule_delay(timedelta(milliseconds=delay))

    def schedule_seconds(self, delay)->datetime:
        return self.schedule_delay(timedelta(seconds=delay))

    def schedule_minutes(self, delay)->datetime:
        return self.schedule_delay(timedelta(minutes=delay))

    def unschedule(self):
        self.controller._tree_by_t.remove(self._tree_by_t)
        self.t = None

    def deactivate(self):
        self.controller._tree_by_t.remove(self._tree_by_t)
        self.t = None
        self._signaled.remove()

    def signal(self):
        if not self._signaled.in_list():
            self.controller._signaled[self.priority].add(self._signaled)

    def reached(self, t:datetime) -> bool:
        if t is None:
            return True
        else:
            if t <= self.controller.now():
                return True
            else:
                self.schedule(t)
                return False

    def get_t(self) -> datetime:
        return self.t

    def now(self):
        return self.controller.now()

    def close(self):
        self.controller._tree_by_t.remove(self._tree_by_t)
        self.controller._tree_by_id.remove(self._tree_by_id)
        self._signaled.remove()

class ActiveObjectWithRetries(ActiveObject):

    def __init__(self, controller, id=None):
        super().__init__(controller, id)
        self.__next_retry = None
        self.__next_retry_interval = None
        self.min_retry_interval = 1
        self.max_retry_interval = 60

    def was_error(self):
        return self.__next_retry is not None

    def _process_internal(self):
        try:
            if self.__next_retry is None \
            or self.reached(self.__next_retry):
                super()._process_internal()
                self.__next_retry = None
        except:
            if self.__next_retry is None:
                self.__next_retry_interval = self.min_retry_interval
            else:
                self.__next_retry_interval = self.__next_retry_interval + self.__next_retry_interval
                if self.__next_retry_interval > self.max_retry_interval:
                    self.__next_retry_interval = self.max_retry_interval
            self.__next_retry = self.schedule_delay(timedelta(seconds=self.__next_retry_interval))
            raise

class Signaler:
    pass

class Listener:
    pass

class Signaler:

    def __init__(self):
        self.queue = linked_list.DualLinkedList()

    def signalNext(self)->bool:
        item = self.queue.remove_first()
        if item is None: return False
        item.owner.signal()
        return self.queue.first is not None

    def signalAll(self):
        item = self.queue.remove_first()
        while item is not None:
            item.owner.signal()
            item = self.queue.remove_first()

    def close(self):
        self.signalAll()

    def copyFrom(self, signaler:Signaler):
        item = signaler.queue.remove_first()
        while item is not None:
            self.queue.add(item)
            item = signaler.queue.remove_first()

    def check(self, listener:Listener)->bool:
        if listener is None: return False
        if listener.queue.in_list(self.queue): return False
        self.queue.add(listener.queue)
        return True

    def wait(self, listener:Listener):
        self.check(listener)

    def isQueued(self, listener:Listener)->bool:
        return listener.queue.in_list(self.queue)

    def hasListeners(self)->bool:
        return self.queue.first is not None

class Listener:

    def __init__(self):
        self.queue = linked_list.DualLinkedListItem(self)

    def wait(self, signaler:Signaler):
        signaler.check(self)

    def signal(self):
        self.queue.remove()

    def is_signaled(self)->bool:
        return self.queue.list is None

    def remove(self):
        self.queue.remove()

    def close(self):
        self.queue.remove()

    def check(self, signaler:Signaler)->bool:
        return signaler.check(self)

class AOListener(Listener):

    def __init__(self, owner:ActiveObject):
        super().__init__()
        self.owner = owner

    def signal(self):
        super().signal()
        self.owner.signal()

@DeprecationWarning
class SignalPub:

    def __init__(self, owner=None):
        self.subscribers = linked_list.DualLinkedList()
        self.owner = owner

    def signal(self):
        item = self.subscribers.first
        while item is not None:
            sub = item.owner
            if not sub.edge or not sub.is_set:
                sub.is_set = True
                sub.owner.signal()
            item = item.next

    def close(self):
        item = self.subscribers.remove_first()
        while item is not None:
            sub = item.owner
            if not sub.edge or not sub.is_set:
                sub.is_set = True
                sub.owner.signal()
            item = self.subscribers.remove_first()


@DeprecationWarning
class SignalSub:

    def __init__(self, owner:ActiveObject, edge:bool=False, is_set=False, pub:SignalPub=None):
        self.owner = owner
        self.pub_link = linked_list.DualLinkedListItem(self)
        self.is_set = is_set
        self.edge = edge
        if pub is not None: self.subscribe(pub)

    def subscribe(self, pub:SignalPub):
        pub.subscribers.add(self.pub_link)

    def unsubscribe(self):
        self.pub_link.remove()

    def is_subscribed(self):
        return self.pub_link.in_list()

    def is_active(self):
        if self.is_set: return True
        if not self.pub_link.in_list(): return True
        return False

    def reset(self):
        res = self.is_active()
        self.is_set = False
        return res

    def close(self):
        self.unsubscribe()

class Flag:

    def __init__(self):
        self._wait_up_queue = linked_list.DualLinkedList()
        self._wait_down_queue = linked_list.DualLinkedList()
        self.__is_up = False

    def notify_all(self):
        if self.__is_up:
            item = self._wait_up_queue.remove_first()
            while item is not None:
                item.owner.owner.signal()
                item = self._wait_up_queue.remove_first()
        else:
            item = self._wait_down_queue.remove_first()
            while item is not None:
                item.owner.owner.signal()
                item = self._wait_down_queue.remove_first()

    def notify(self) -> bool:
        if self.__is_up:
            item = self._wait_up_queue.remove_first()
            if item is None: return False
            item.owner.owner.signal()
            return self._wait_up_queue.first is not None
        else:
            item = self._wait_down_queue.remove_first()
            if item is None: return False
            item.owner.owner.signal()
            return self._wait_down_queue.first is not None

    def up(self, notify_all:bool=True):
        if self.__is_up: return False
        self.__is_up = True
        if notify_all:
            self.notify_all()

    def down(self, notify_all:bool=True):
        if not self.__is_up: return False
        self.__is_up = False
        if notify_all:
            self.notify_all()

class FlagListener:

    def __init__(self, owner:ActiveObject):
        self._wait_queue = linked_list.DualLinkedListItem(self)
        self.owner:ActiveObject = owner

    def close(self):
        self._wait_queue.remove()
        self.owner:ActiveObject = None
        self.flag:Flag = None

    def is_up(self, flag: Flag) -> bool:
        if flag.__is_up:
            if self._wait_queue.list is flag._wait_up_queue:
                flag._wait_up_queue.remove(self._wait_queue)
            return True
        else:
            if self._wait_queue.list is None \
            or self._wait_queue.list is not flag._wait_up_queue:
                flag._wait_up_queue.add(self._wait_queue)
            return False

    def is_down(self, flag: Flag) -> bool:
        if not flag.__is_up:
            if self._wait_queue.list is flag._wait_down_queue:
                flag._wait_down_queue.remove(self._wait_queue)
            return True
        else:
            if self._wait_queue.list is None \
            or self._wait_queue.list is not flag._wait_down_queue:
                flag._wait_down_queue.add(self._wait_queue)
            return False

def _compkey_id(k, n):
    if k[0] > n.owner.type_id:
        return 1
    elif k[0] < n.owner.type_id:
        return -1
    elif k[1] > n.owner.id:
        return 1
    elif k[1] == n.owner.id:
        return 0
    else:
        return -1

def _compkey_type(k, n):
    if k > n.owner.type_id:
        return 1
    elif k < n.owner.type_id:
        return -1
    else:
        return 0

def _comp_id(n1, n2):
    return _compkey_id((n1.owner.type_id, n1.owner.id), n2)

def _comp_t(n1, n2):
    if n1.owner.t > n2.owner.t:
        return 1
    elif n1.owner.t == n2.owner.t:
        return 0
    else:
        return -1

class ActiveObjectsController():

    def __init__(self, priority_count:int=1):
        self._tree_by_t = avl_tree.Tree(_comp_t)
        self._tree_by_id = avl_tree.Tree(_comp_id)
        self._signaled = [linked_list.DualLinkedList() for i in range(0, priority_count)]
        self.terminated: bool = False
        self.emulated_time = None

    def find(self, type_id, id) -> ActiveObject:
        node = self._tree_by_id.find((type_id,id), _compkey_id)
        if node is not None:
            return node.owner

    def now(self) -> datetime:
        if self.emulated_time is None:
            return datetime.now()
        else:
            return self.emulated_time

    def get_nearest(self) -> ActiveObject:
        node = self._tree_by_t.get_leftmost()
        if node is not None:
            return node.owner

    def wakeup(self):
        raise Exception("Not supported")

    def process(self, max_count: int=None, on_before=None, on_success=None, on_error=None) -> datetime:

        def do(obj:ActiveObject):
            obj.unschedule()
            if on_before is not None:
                if on_before(obj):
                    return
            if on_error is None:
                obj._process_internal()
                if on_success is not None:
                    on_success(obj)
            else:
                try:
                    obj._process_internal()
                    if on_success is not None:
                        on_success(obj)
                except Exception as e:
                    on_error(obj, e)

        def remove_next_signaled() -> ActiveObject:
            for queue in self._signaled:
                item = queue.remove_first()
                if item is not None:
                    return item

        while not self.terminated:
            obj = self.get_nearest()
            next_time = None
            while obj is not None:
                if obj.get_t() > self.now():
                    next_time = obj.get_t()
                    break                
                t = obj._tree_by_t.get_successor()
                next_task = t.owner if t is not None else None
                obj.unschedule()
                obj.signal()
                obj = next_task

            item = remove_next_signaled()
            if item is None:
                return next_time
            n = 10
            while item is not None:
                do(item.owner)
                n -= 1
                if n < 0: break
                if max_count is not None:
                    max_count -= 1
                    if max_count <= 0:
                        return self.now()
                if self.terminated: break
                item = remove_next_signaled()

    def for_each_object(self, type_id, func):
        if type_id is None:
            n = self._tree_by_id.get_leftmost()
            while n is not None:
                func(n.owner)
                n = n.get_successor()
        else:
            n = self._tree_by_id.find_leftmost_eq(type_id, _compkey_type)
            while n is not None and n.owner.type_id == type_id:
                n2 = n.get_successor()
                func(n.owner)
                n = n2

    def for_each_object_with_break(self, type_id, func):
        if type_id is None:
            n = self._tree_by_id.get_leftmost()
            while n is not None:
                n2 = n.get_successor()
                v = func(n.owner)
                if v:
                    return v
                n = n2
        else:
            n = self._tree_by_id.find_leftmost_eq(type_id, _compkey_type)
            while n is not None and n.owner.type_id == type_id:
                n2 = n.get_successor()
                v = func(n.owner)
                if v:
                    return v
                n = n2
        return None

    def get_ids(self, type_id) -> list:
        res = list()
        self.for_each_object(type_id, lambda o: res.append(o.id))
        return res

    def signal(self, type_id=None):
        self.for_each_object(type_id, lambda o: o.signal())

    def terminate(self):
        self.terminated = True

class AbstractTask(ActiveObject):

    def __init__(self, controller, id=None):
        super().__init__(controller, id)
        self.exit_code = None
        self._cancel_requested = False
        self._kill_requested = False
        self.error = None
        self.completed_signal = Signaler()
        self.signal()

    def is_completed(self, listener:Listener=None)->bool:
        if self.exit_code is not None:
            return True
        if listener is not None:
            listener.wait(self.completed_signal)
        return False

    def is_cancelled(self)->bool:
        return self._cancel_requested

    def get_exit_code(self):
        return self.exit_code

    def _process(self):
        if self.is_completed():
            self.completed_signal.signalAll()
            self.close()

    def set_exit_code(self, exit_code:int):
        if self.exit_code is None:
            self.signal()
            self.exit_code = exit_code

    def cancel(self, kill:bool=False):
        if not self._cancel_requested:
            self._cancel_requested = True
            self.signal()
        if kill and not self._kill_requested:
            self._kill_requested = True
            self.signal()

    def close(self):
        self.completed_signal.close()
        super().close()

async def async_loop(controller:ActiveObjectsController):
    import asyncio
    controller.terminated = False
    controller.emulated_time = None
    controller._wakeup_event = asyncio.Event()
    controller.wakeup = lambda: controller._wakeup_event.set()

    while not controller.terminated:
        next_time = controller.process()
        if controller.terminated: return
        if not controller._wakeup_event.is_set():
            if next_time is not None:
                delta = (next_time - controller.now()).total_seconds()
                if delta > 0:
                    try:
                        await asyncio.wait_for(controller._wakeup_event.wait(), timeout=delta)
                    except TimeoutError:
                        pass
            else:
                await controller._wakeup_event.wait()
        controller._wakeup_event.clear()


def simple_loop(controller:ActiveObjectsController):
    import time
    controller.terminated = False
    controller.emulated_time = None
    while not controller.terminated:
        next_time = controller.process()
        if controller.terminated: return
        if next_time is not None:
            delta = (next_time - controller.now()).total_seconds()
            if delta > 0:
                time.sleep(delta)

def emulate_asap(controller:ActiveObjectsController, start_time:datetime):
    controller.emulated_time = start_time
    while not controller.terminated:
        controller.emulated_time = controller.process()
        if controller.terminated: return
        if controller.emulated_time is None:
            raise Exception('controller.emulated_time is None!')


