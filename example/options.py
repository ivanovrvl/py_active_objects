import sys
import os
import datetime
sys.path.append(os.path.abspath('..'))
from active_objects import avl_tree, linked_list, ActiveObjectsController, ActiveObjectWithRetries, simple_loop, emulate_asap, Signaler, Listener

sub_queue = linked_list.DualLinkedList() # очередь на подписку
sub_queue_add_event = Signaler() # сигнал добавления в очередь

nosub_queue = linked_list.DualLinkedList() # очередь отписку
nosub_queue_add_event = Signaler() # сигнал добавления в очередь

# правило сравнения страйков для бинарного дерева
def compare_strikes(s1, s2):
    if s1.__price__ > s2.__price__:
        return 1
    elif s1.__price__ < s2.__price__:
        return -1
    return 0

strikes_by_price = avl_tree.Tree(compare_strikes) # упорядоченная таблица страйков

class Strike(ActiveObjectWithRetries):

    def __init__(self, controller):
        super().__init__(controller)
        self.live_ref = 0 # счетчик удержания
        self.sub_ref = 0 # счетчик требований подписки
        self.__is_sub__ = False # фактическое состояние подписки
        self.sub_link = linked_list.DualLinkedListItem(self) # чтобы торчать в очередях на подписку/отписку
        self.__price__ = None
        self.__tree_by_price__ = avl_tree.TreeNode(self)
        self.signal()

    def add_live_ref(self):
        self.live_ref += 1
        return self.live_ref

    def release_live_ref(self):
        self.live_ref -= 1
        if self.sub_ref == 0 and self.live_ref == 0:
            self.close()
        return self.live_ref

    def set_is_sub(self, val:bool):
        if self.__is_sub__ == val:
            return
        # в течение фактической подписки удерживаем страйк от удаления
        if val:
            self.add_live_ref()
        else:
            self.release_live_ref()
        self.__is_sub__ = val

    def add_sub_ref(self):
        if self.sub_ref == 0:
            self.add_live_ref() # удерживаем страйк от удаления, раз есть подписка
        self.sub_ref += 1
        if not self.__is_sub__ and self.sub_link.list != sub_queue:
            sub_queue.add(self.sub_link) # если фактически не подписан и не в очереди на пописку, то ставим в нее
            sub_queue_add_event.signalAll()
        return self.sub_ref

    def release_sub_ref(self):
        self.sub_ref -= 1
        if self.sub_ref == 0:
            self.release_live_ref()
            if self.__is_sub__ and self.sub_link.list != nosub_queue:
                nosub_queue.add(self.sub_link) # если фактически  подписан и не в очереди на отписку, то ставим в нее
                nosub_queue_add_event.signalAll()
        return self.sub_ref

    def process(self):
        pass

    def get_price(self):
        return self.__price__

    def set_price(self, price:float):
        if price == self.__price__:
            return
        self.__price__ = price
        strikes_by_price.remove(self.__tree_by_price__) # переиндексируем
        if self.__price__ is not None:
            strikes_by_price.add(self.__tree_by_price__)

    def close(self):
        self.sub_link.remove()
        strikes_by_price.remove(self.__tree_by_price__)
        print(f"Strike {self.get_price()} is deleted")
        super().close()


class StrikeSubManager(ActiveObjectWithRetries):

    def __init__(self, controller):
        super().__init__(controller)
        self.sub_listener = Listener(self)
        self.nosub_listener = Listener(self)
        self.signal()

    def process(self):
        if self.sub_listener.check(sub_queue_add_event):
            while True:
                item = sub_queue.remove_first()
                if item is None: break
                print(f"Strike {item.owner.get_price()} is subscribed")
                item.owner.set_is_sub(True) # подписываем

        if self.nosub_listener.check(nosub_queue_add_event):
            while True:
                item = nosub_queue.remove_first()
                if item is None: break
                print(f"Strike {item.owner.get_price()} is unsubscribed")
                item.owner.set_is_sub(False) # отписываем

    def close(self):
        self.sub_listener.close()
        self.nosub_listener.close()
        super().close()

class SomeStrikeSubscriber(ActiveObjectWithRetries):

    def __init__(self, controller):
        super().__init__(controller)
        self.when_unsubscribe = None
        self.strike = None
        self.signal()

    def process(self):
        if self.when_unsubscribe is None:
            # начало
            self.strike = Strike(self.controller)
            self.strike.set_price(123)
            self.strike.add_sub_ref()
            self.when_unsubscribe = self.schedule_seconds(3)
        else:
            if self.reached(self.when_unsubscribe) and self.strike is not None:
                # завершение
                self.strike.release_sub_ref()
                self.strike = None

controller = ActiveObjectsController()
manager = StrikeSubManager(controller)

s1 = SomeStrikeSubscriber(controller)

simple_loop(controller)


