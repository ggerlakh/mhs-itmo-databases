## task_1


## task_2
Перед запуском утилиты необходимо:
1. Установить все зависимости утилиты
   ```bash
   python3 -m pip install -r requirements.txt
   ```
2. Запустить `redis`, `kafka`, `mongo`
   Для запуска kafka и mongo нужно запусть docker-compose описанный в файле `docker-compose.yml`
   ```bash
   docker-compose up -d
   ```
   Подключится к запусщенном инстансу mongo можно через `mongosh`
   ```bash
   mongosh mongodb://user:pass@127.0.0.1:27017/
   ```

Также можно собрать отдельный docker образ с данной утилитой, используя `Dockerfile`:
```bash
docker build . -t hw4
```

Консольная утилита для второго задания

```bash
% python3 hw4_cli.py task-2 --help
Usage: hw4_cli.py task-2 [OPTIONS] MONGODB_CONNECTION_STRING

Options:
  --db-name TEXT    name of database in mongodb (default 'hw4')
  --col-name TEXT   name of collection in --db-name in mongodb (default
                    'data')
  --input_csv TEXT  path to input dataset in csv format
  --help            Show this message and exit.
```

Запустить утилиту можно следующим образом:
```bash
python3 hw4_cli.py task-2 --db-name hw4 --col-name taxi --input-csv taxi_ds.csv mongodb://user:pass@127.0.0.1:27017/ task_2
```

После запуска будут созданы нужные коллекции требуемые в задании №2.

В файле `queries_mongo.help` также есть вспомогательные запросы для mongosh которые были использованы для выполнения данного задания.

Пример хранимых событий в mongo
```json
[
  {
    _id: ObjectId('67896e820d23c35c16518acf'),
    PULocationID: '229',
    VendorID: '2',
    timestamp: ISODate('2018-07-19T04:37:15.000Z'),
    tpep_pickup_datetime: ISODate('2018-07-19T04:37:15.000Z'),
    trip_id: '0afd4392-69c3-488c-a8a5-151f5b2837d6'
  },
  {
    _id: ObjectId('67896eb376dc8fee899ae802'),
    DOLocationID: '7',
    RatecodeID: '1',
    extra: '0.5',
    fare_amount: '14',
    improvement_surcharge: '0.3',
    mta_tax: '0.5',
    passenger_count: '1',
    payment_type: '1',
    store_and_fwd_flag: 'N',
    timestamp: ISODate('2018-07-19T04:51:59.000Z'),
    tip_amount: '3.06',
    tolls_amount: '0',
    total_amount: '18.36',
    tpep_dropoff_datetime: ISODate('2018-07-19T04:51:59.000Z'),
    trip_distance: '3.42',
    trip_id: '0afd4392-69c3-488c-a8a5-151f5b2837d6'
  }
]
```

## task_3
Перед запуском утилиты необходимо:
1. Установить все зависимости утилиты
   ```bash
   python3 -m pip install -r requirements.txt
   ```
2. Запустить `redis`, `kafka`, `mongo`
   Для запуска kafka и mongo нужно запусть docker-compose описанный в файле `docker-compose.yml`
   ```bash
   docker-compose up -d
   ```
   Подключится к запусщенном инстансу mongo можно через `mongosh`
   ```bash
   mongosh mongodb://user:pass@127.0.0.1:27017/
   ```

Также можно собрать отдельный docker образ с данной утилитой, используя `Dockerfile`:
```bash
docker build . -t hw4
```

Консольная утилита для третьего задания

```bash
% python3 hw4_cli.py task-3 --help
Usage: hw4_cli.py task-3 [OPTIONS] MONGODB_CONNECTION_STRING KAFKA_BOOTSTRAP_SERVER

Options:
  --db-name TEXT      name of database in mongodb (default 'hw4')
  --col-name TEXT     name of collection in --db-name in mongodb (default
                      'taxi')
  --kafka-topic TEXT  kafka topic from cli reads data
  --help              Show this message and exit.
```

Запустить утилиту можно следующим образом:
```bash
python3 hw4_cli.py task-3 --db-name hw4 --col-name task_2 --kafka-topic hw4_taxi mongodb://user:pass@127.0.0.1:27017/ localhost:29092
```

Для выполнения task-3 была написаниа консольная утилита которая читает данные из соответствующей db в mongodb и звписывает данные в указанный один топик в kafka, для объединения всех событий связанных с taxi. Также в конфигурации producer при отправке в качестве key был выбран trip_id, чтобы при отправке событий в kafka, события pickup и dropoff для одной и той же поездки попали в одну и ту же partition.

## task_4_5
Перед запуском утилиты необходимо:
1. Установить все зависимости утилиты
   ```bash
   python3 -m pip install -r requirements.txt
   ```
2. Запустить `redis`, `kafka`, `mongo`
   Для запуска kafka и mongo нужно запусть docker-compose описанный в файле `docker-compose.yml`
   ```bash
   docker-compose up -d
   ```
   Подключится к запущенному инстансу redis можно через `redis-cli`
   ```bash
   redis-cli -h localhost -p 6379 -n 0
   ```

Также можно собрать отдельный docker образ с данной утилитой, используя `Dockerfile`:
```bash
docker build . -t hw4
```

Консольная утилита для четверого/пятого задания

```bash
% python3 hw4_cli.py task-4-5 --help
Usage: hw4_cli.py task-4-5 [OPTIONS] KAFKA_BOOTSTRAP_SERVER

Options:
  --redis-host TEXT     redis host (default 'localhost')
  --redis-port INTEGER  redis port (default 6379)
  --redis-db INTEGER    redis db number (default 0)
  --kafka-topic TEXT    kafka topic from cli reads data
  --help                Show this message and exit.
```
Запустить утилиту можно следующим образом:
```bash
python3 hw4_cli.py task-4-5 --redis-host localhost --redis-port 6379 --redis-db 0 --kafka-topic hw4_taxi localhost:29092
```

Kafka consumer:
- Для обеспечения надежности и возможности перечитки в случае перезапуска consumer в его конфигурации используется механизм consumer group (через задание `group_id`), а также параметры `auto_offset_reset='earliest'` и `enable_auto_commit=False` для коммита последнего обработанного offset инстансом вручную после обработки прочитанного события.


Redis:
- Общее количество активных поездок в redis доступно по ключу `total_active_trips_count`.  
- Количество активных поездок по району для каждого района доступно по ключам такого типа `PULocationID:{PULocationID}:active_count`, пример `PULocationID:232:active_count`.  
  Если читается события с началом поездки, то выполняется incr по соответствующему ключу `PULocationID:{PULocationID}:active_count`, а также выполняется incr для общего числа поездок `total_active_trips_count`.  
  Также заводится hash с соответствием `trip_id - PULocationId`, который доступен по ключу `trip_id_pulocation_map`, для того чтобы при обработке события конца поездки получать информацию о том в каком районе началась поездка.  
  При обработке события конца поездки количество активных поездок уменьшается при помощи команды decr по соответствующему ключу `PULocationID:{PULocationID}:active_count`, а также выполняется decr для общего числа поездок `total_active_trips_count`. Затем, после обработки поездки соответствующий ключ удаляется из hash `trip_id_pulocation_map`.
- Для корректной работы redis через несколько инстансов приложения, все команды выполняются в транзакциях через `MULTI-EXEC`, f также через использование команды `WATCH`, которая защищает от race condition и позволяет работать с соответствующим ключом только одному инстансу.
- Также для выполнения 5 задания были заведены ключи следующего формата `is_processed:pu:{trip_id}` или `is_processed:do:{trip_id}` c TTL = 30 минут для проверки того было ли соответствующее событие (pu, {trip_id}) или (do, {trip_id}) уже обработанно ранее. Если соответствующий ключ есть, значит событие было обработано, иначе нет. Перед каждой обработкой события в начале транзакции будет устанавливаться соответствующй ключ.
- Для решения 6 задания была использована следующая логика:
  - В каждый момент времени для каждого `PUlocationId` поддерживается множество с приоритетами (ZSET) по ключу `PULocationID:{PULocationID}:destinations`, элементами которого являются следующие пары (`DOLocationID:{DOLocationID}` [key], `{count}` [score]).  
    При получении события о конце поездки, в zset под названием `PULocationID:{PULocationID}:destinations` score элемента `DOLocationID:{DOLocationID}` будет увеличен на 1.
  - Для запроса пользователя будет возвращатся результат запроса `ZREVRANGE PULocationID:{PULocationID}:destinations 0 0 WITHSCORES`


## task_6

Перед запуском утилиты необходимо:
1. Установить все зависимости утилиты
   ```bash
   python3 -m pip install -r requirements.txt
   ```
2. Запустить `redis`, `kafka`, `mongo`
   Для запуска kafka и mongo нужно запусть docker-compose описанный в файле `docker-compose.yml`
   ```bash
   docker-compose up -d
   ```
   Подключится к запущенному инстансу redis можно через `redis-cli`
   ```bash
   redis-cli -h localhost -p 6379 -n 0
   ```

Также можно собрать отдельный docker образ с данной утилитой, используя `Dockerfile`:
```bash
docker build . -t hw4
```

Консольная утилита для шестого задания

```bash
% python3 hw4_cli.py task-6 --help                                                                                        
Usage: hw4_cli.py task-6 [OPTIONS] INPUT_PULOCATION_ID

Options:
  --redis-host TEXT     redis host (default 'localhost')
  --redis-port INTEGER  redis port (default 6379)
  --redis-db INTEGER    redis db number (default 0)
  --help                Show this message and exit.
```
Запустить утилиту можно следующим образом:
```bash
python3 hw4_cli.py task-6 --redis-host localhost --redis-port 6379 --redis-db 0 79 
```

Для решения задачи была использована следующая логика:
- В каждый момент времени для каждого `PUlocationId` поддерживается множество с приоритетами (ZSET) по ключу `PULocationID:{PULocationID}:destinations`, элементами которого являются следующие пары (`DOLocationID:{DOLocationID}` [key], `{count}` [score]).  
  При получении события о конце поездки, в zset под названием `PULocationID:{PULocationID}:destinations` score элемента `DOLocationID:{DOLocationID}` будет увеличен на 1.
- Для запроса пользователя будет возвращатся результат запроса `ZREVRANGE PULocationID:{PULocationID}:destinations 0 0 WITHSCORES`