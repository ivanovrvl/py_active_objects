import copy
from active_objects import ActiveObjectWithRetries, ActiveObjectsController

class DbObject:
    pass

class DbObject(ActiveObjectWithRetries):
    """
    A task object corresponding to the record in table_name table
    """
    #table_name
    #table_key_fields = ['id']
    #table_fields = ['active', 'locked_until', 'stop']
    #notify_key = '!' + table_name
    #_changed = set()
    #_deleted = set()
    
    table_name = None    
    table_key_fields = ['id']
    table_fields = None
    version_field_name = None

    def __init__(self, controller:ActiveObjectsController, id):
        super().__init__(controller, id)
        self.__old_db_state__ = None # last known DB state
        self.db_state = None # current state
        self.changed_fields = set()
        self.is_deleted = True

    @classmethod
    def cast_id(cls, id):
        return id
    
    @classmethod
    def parse_id(cls, s:str):
        return int(s)           
        
    def set_field(self, name:str, value, set_changed:bool=True):
        if self.db_state is None:
            raise Exception("Can`t set field: self.db_state is None")
        try:
            cur = self.db_state[name]
            if cur is value or cur == value:
                return False
        except KeyError:
            pass
        self.db_state[name] = value
        if set_changed:
            self.changed_fields.add(name)
        return True
    
    @classmethod
    def invalidate_all(cls, controller:ActiveObjectsController):
        controller.for_each_object(cls.type_id, lambda o: o.invalidate())
    
    def invalidate(self):
        pass

    def set_deleted(self):
        if not self.is_deleted:            
            self.info("DELETED")
            self.is_deleted = True
            self.__old_db_state__ = None
            self.db_state = None
            self.changed_fields.clear()
            self.invalidate()
            self.signal()

    def set_db_state(self, db_state):
        if self.is_deleted:
            self.invalidate()
            self.is_deleted = False        
        if self.__old_db_state__ is None \
        or (self.__class__.version_field_name is not None \
        and self.__old_db_state__[self.__class__.version_field_name] != db_state[self.__class__.version_field_name]):
            self.__old_db_state__ = db_state
            self.db_state = copy.copy(db_state)
            self.changed_fields.clear()
        else:
            self.__old_db_state__ = db_state
            old = self.db_state
            self.db_state = copy.copy(db_state)
            for n in self.changed_fields:
                self.db_state[n] = old[n]        
        self.signal()
        
    @classmethod
    def refresh_db_states(cls, controller:ActiveObjectsController, cur, expected_ids:set=None) -> bool:
        found_ids = set()
        for row in cur.fetchall():
            db_state = get_db_state(cur, row)
            if len(cls.table_key_fields)==1:
                id = db_state[cls.table_key_fields[0]]
            elif len(cls.table_key_fields)==2:
                id = (db_state[cls.table_key_fields[0]], db_state[cls.table_key_fields[1]])
            elif len(cls.table_key_fields)==3:
                id = (db_state[cls.table_key_fields[0]], db_state[cls.table_key_fields[1]], db_state[cls.table_key_fields[2]])
            else:
                raise Exception('Not supported') 
            id = cls.cast_id(id)
            found_ids.add(id)
            obj = controller.find(cls.type_id, id)
            if obj is None and cls.must_be_loaded(db_state):
                obj = cls(controller, id)
            if obj is not None:
                obj.set_db_state(db_state)
        if expected_ids is not None:
            for id in expected_ids.difference(found_ids):
                obj = controller.find(cls.type_id, id)
                if obj is not None:
                    obj.set_deleted()
        return len(found_ids)        

    def refresh_db_state(self, conn):
        self.__old_db_state__ = None
        with conn.cursor() as cur:
            sql = f"""
                SELECT {self.__class__.cls.get_select_fields_sql()}
                FROM {self.__class__.table_name}
                WHERE {' and '.join([f + '=%s' for f in self.__class__.table_key_fields])}
            """
            cur.execute(sql, self.id)
            self.refresh_db_states(self.controller, cur, set([self.id]))

    def save_db_state(self, conn):
        if len(self.changed_fields) > 0:
            sql = f"""
                UPDATE {self.__class__.table_name}
                SET {','.join([n + '=%s' for n in self.changed_fields])}
                WHERE {' and '.join([f + '=%s' for f in self.__class__.table_key_fields])}
            """
            values = [self.db_state[n] for n in self.changed_fields] + [self.db_state[n] for n in self.__class__.table_key_fields]
            if self.__class__.version_field_name is not None:
                values.append(self.db_state[self.__class__.version_field_name])
                sql = sql + ' and ' + self.__class__.version_field_name + '=%s'
            with conn.cursor() as cur:
                cur.execute(sql, values)
                self.changed_fields.clear()
                if cur.rowcount == 0:
                    self.refresh_db_state()

    def info(self, msg:str):
        if msg is not None:
            print(self.type_id + repr(self.id) + ': ' + msg)

    def error(self, msg:str):
        if msg is not None:
            print(self.type_id + repr(self.id) + '! ' + msg)
            
    def must_be_loaded(db_state:map) -> bool:
        return True            

    @classmethod
    def _clear_changes(cls):
        cls._changed.clear()
        cls._deleted.clear()

    @classmethod
    def _add_change(cls, msg:str):
        if len(msg) > 2 and msg[1] == ' ' and msg[0] in ('I','U','D'):            
            id = cls.parse_id(msg[2:])
            if msg[0] == 'D':
                cls._deleted.add(id)
            else:
                cls._changed.add(id)
                
    @classmethod
    def get_select_fields_sql(cls):
        return ','.join(cls.table_key_fields + cls.table_fields)        

    @classmethod
    def _apply_changes(cls, controller:ActiveObjectsController, conn):
        changed = cls._changed.difference(cls._deleted)
        if len(changed)>0: 
            with conn.cursor() as cur:
                if len(cls.table_key_fields) == 1:
                    sql = f"""
                        SELECT {cls.get_select_fields_sql()}
                        FROM {cls.table_name}
                        WHERE id = any(%s)
                        """
                    values = (list(changed),)
                else:
                    vs = '(' + ','.join(['%s' for f in cls.table_key_fields]) + ')'
                    sql = f"""
                        SELECT {cls.get_select_fields_sql()}
                        FROM {cls.table_name}
                        WHERE ({','.join(cls.table_key_fields)}) in (
                        {','.join([vs for d in changed])}
                        )
                        """
                    values = [d[i] for d in changed for i in range(0,len(cls.table_key_fields))]
                cur.execute(sql, values)
                cls.refresh_db_states(controller, cur)
        cls._changed.clear()
        for id in cls._deleted:
            task = controller.find(cls.type_id, id)
            if task is not None:
                task.set_deleted()
        cls._deleted.clear()

    @classmethod
    def find_or_new(cls, controller:ActiveObjectsController, id) -> DbObject:
        obj = controller.find(cls.type_id, id)
        if obj is None:
            obj = cls(controller, id)
        return obj

def get_db_state(cur, row) -> map:
    return {d.name: row[i] for i, d in enumerate(cur.description)}

def poll_db_changes(controller:ActiveObjectsController, conn, db_object_types)->bool:
    res = False
    notify_keys = {t.notify_key:t for t in db_object_types}    
    for c in db_object_types:
        c._clear_changes()
    conn.poll()
    while conn.notifies:
        n = conn.notifies.pop()
        c = notify_keys.get(n.channel)
        if c is not None:
            res = True     
            c._add_change(n.payload)
    for c in db_object_types:
        c._apply_changes(controller, conn)
    return res

