# py_active_objects
Python library implementing long-live objects acting asyncroniously based on event queues

See [pg_tasks](https://github.com/ivanovrvl/pg_tasks) as an example

# Введение
* Все работает асинхронно в один поток. Основная логика реализуется в методе AO.process(), который должен как можно скорее изучить текущую обстановку, выполнить действия и завершиться, синхронное ожидание в нем заморозит обработку остального.
* Активный объект (AO) может находиться в состоянии signaled, что означает, что должен "как можно скорее" (ASAP) быть вызван его метод AO.process(). Когда AO становится signaled (при вызове AO.signaled()), он встает в ASAP очередь на вызов AO.process(). Библиотека обеспечивает вызов AO.process() ASAP в порядке очереди. Поддерживаются несколько очередей для разных приоритетов процессов.
* Вызов AO.process() может быть запланирован на время вызовом AO.shedule(<время>). По достижении времени, AO становится signaled. На самом деле, AO может быть запланирован только на 1 момент времени. AO.schedule(<время>) перепланирует AO, только если запрошено более ближнее время, чем на которое AO запланирован сейчас. Этого достаточно, поскольку для AO интересно только ближайшее время запуска, после очередного запуска, он будет запланирован на следующее ближайшее время.
* Метод AO.reached(<время>) возвращает True, если текущее время достигло заданного, либо планирует AO на указанное время.

# Принципы написания логики процесса в AO
* AO.process() может быть вызван в любое время. Конкретная причина почему он вызван не известна и в общем случае она может быть не одна. AO.process() должен всегда проверять все возможные причины (возможна частичная обработка с обязательным вызовом self.signaled() для последующей полной обработки)
* если изменилось что-то важное для экземпляра AO, то внешний код либо другой экземпляр AO должен вызвать AO.signaled() для интересующегося процесса. Таким образом, за каждым значимым изменением будет следовать ASAP вызов AO.signeled(). Для удобства, внешний код в этом случае может вызывать какие-либо другие методы AO, которые должны содержать вызов self.signaled().
* Для реализации сложной логики внутри AO следует использовать конечный автомат. Если меняется состояние (статус) конечного автомата, то, возможно, потребуется вызов self.signaled(), чтобы AO.process() усвоил изменение.
* Ожидание момента времени X реализуется вызовом self.reached(X), который вернет True, если момент достигнут. Если момент не достигнут, то self.reached(X) обеспечит планирование запуска AO.process() на момент X.
* Для получения текущего времени предпочтительно использовать AO.now(). Это потенциально позволит отлаживать процессы в режиме эмулированного времени.
