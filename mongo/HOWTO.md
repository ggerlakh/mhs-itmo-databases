# hw3_mongo_cli

Перед запуском утилиты необходимо:
1. Установить все зависимости утилиты
   ```bash
   python3 -m pip install -r requirements.txt
   ```
2. Запустить саму `mongodb`  
   Можно апустить docker контейнер с `mongodb` можно через 
   ```bash 
   bash start_mongo.sh
   ```
   Подключится к запусщенном инстансу mongo можно через `mongosh`
   ```bash
   mongosh mongodb://user:pass@127.0.0.1:27017/
   ```

Также можно собрать отдельный docker образ с данной утилитой, используя `Dockerfile`:
```bash
docker build . -t hw3_mongo_cli
```

Утилита имеет соответствующие подкоманды выполняющие соответствующее задание:
```bash
% python3 hw3_mongo_cli.py --help
Usage: hw3_mongo_cli.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  task-1
  task-2
  task-3
  task-3-dupl
  task-4
  task-5
  task-6
  task-7
  task-8
```

## task_1
Консольная утилита для первого задания
```bash
% python3 hw3_mongo_cli.py task-1 --help
Usage: hw3_mongo_cli.py task-1 [OPTIONS] MONGODB_CONNECTION_STRING DATE_RANGE

Options:
  --db-name TEXT        name of database in mongodb (default 'hw3')
  --col-name TEXT       name of collection in --db-name in mongodb (default
                        'gh_data')
  --artifacts-dir TEXT
  --help                Show this message and exit.
```

Запустить утилиту можно следующим образом:
```bash
python3 hw3_mongo_cli.py task-1 --db-name hw3 --col-name gh_data mongodb://user:pass@127.0.0.1:27017/ 2015-11-05:2015-11-08
```

В этом случае все скачанные файлы будут схоранены в текущей директории, но если требуется их сохранить в отдельной директорий, тогда можно использовать опцию `--artifacts-dir`:
```bash
python3 hw3_mongo_cli.py task-1 --db-name hw3 --col-name gh_data --artifacts-dir download_dataset mongodb://user:pass@127.0.0.1:27017/ 2015-11-05:2015-11-08
```

## task_2
Консольная утилита для второго задания

```bash
% python3 hw3_mongo_cli.py task-2 --help
Usage: hw3_mongo_cli.py task-2 [OPTIONS] MONGODB_CONNECTION_STRING DATE_RANGE

Options:
  --db-name TEXT        name of database in mongodb (default 'hw3')
  --col-name TEXT       name of collection in --db-name in mongodb (default
                        'gh_data')
  --artifacts-dir TEXT
  --help                Show this message and exit.
```

Запустить утилиту можно следующим образом:
```bash
python3 hw3_mongo_cli.py task-2 --db-name hw3 --col-name gh_data mongodb://user:pass@127.0.0.1:27017/ 2015-11-06:2015-11-09
```

В этом случае все скачанные файлы будут схоранены в текущей директории, но если требуется их сохранить в отдельной директорий, тогда можно использовать опцию `--artifacts-dir`:
```bash
python3 hw3_mongo_cli.py task-2 --db-name hw3 --col-name gh_data --artifacts-dir download_dataset mongodb://user:pass@127.0.0.1:27017/ 2015-11-06:2015-11-09
```

## task_3
Консольная утилита для третьего задания

```bash
% python3 hw3_mongo_cli.py task-3 --help
Usage: hw3_mongo_cli.py task-3 [OPTIONS] MONGODB_CONNECTION_STRING

Options:
  --db-name TEXT   name of database in mongodb (default 'hw3')
  --col-name TEXT  name of collection in --db-name in mongodb (default
                   'gh_data')
  --help           Show this message and exit.
```

Запустить утилиту можно следующим образом (предварительно загрузив данные в указанную коллекцию, если коллекция не будет найдена, утилита выведет соответствующее сообщение об ошибке):
```bash
python3 hw3_mongo_cli.py task-3 --db-name hw3 --col-name gh_data mongodb://user:pass@127.0.0.1:27017/
```

## task_3_dupl
Консольная утилита для третьего задания (dupl)
```bash
% python3 hw3_mongo_cli.py task-3-dupl --help
Usage: hw3_mongo_cli.py task-3-dupl [OPTIONS] MONGODB_CONNECTION_STRING

Options:
  --db-name TEXT       name of database in mongodb (default 'hw3')
  --in-col-name TEXT   name of collection with input data in --db-name in
                       mongodb (default 'gh_data')
  --out-col-name TEXT  name of collection with output data in --db-name in
                       mongodb (default 'out_gh_data')
  --help               Show this message and exit.
```

Запустить утилиту можно следующим образом (предварительно загрузив данные в указанную коллекцию, если входная коллекция не будет найдена, утилита выведет соответствующее сообщение об ошибке):
```bash
python3 hw3_mongo_cli.py task-3-dupl --db-name hw3 --in-col-name gh_data --out-col-name gh_data_out  mongodb://user:pass@127.0.0.1:27017/
```

## task_4
Консольная утилита для четвертого задания

## task_5
Консольная утилита для пятого задания
```bash
% python3 hw3_mongo_cli.py task-5 --help
Usage: hw3_mongo_cli.py task-5 [OPTIONS] MONGODB_CONNECTION_STRING

Options:
  --db-name TEXT       name of database in mongodb (default 'hw3')
  --in-col-name TEXT   name of collection with input data in --db-name in
                       mongodb (default 'gh_data')
  --out-col-name TEXT  name of collection with output data in --db-name in
                       mongodb (default 'out_gh_data')
  --help               Show this message and exit.
```

Запустить утилиту можно следующим образом (предварительно загрузив данные в указанную коллекцию, если входная коллекция не будет найдена, утилита выведет соответствующее сообщение об ошибке):
```bash
python3 hw3_mongo_cli.py task-5 --db-name hw3 --in-col-name gh_data --out-col-name gh_data_out  mongodb://user:pass@127.0.0.1:27017/
```
## task_6
Консольная утилита для шестого задания
## task_7
Консольная утилита для седьмого задания
## task_8
Консольная утилита для восьмого задания
Консольная утилита для третьего задания (dupl)
```bash
% python3 hw3_mongo_cli.py task-8 --help
Usage: hw3_mongo_cli.py task-8 [OPTIONS] MONGODB_CONNECTION_STRING

Options:
  --db-name TEXT       name of database in mongodb (default 'hw3')
  --in-col-name TEXT   name of collection with input data in --db-name in
                       mongodb (default 'gh_data')
  --out-col-name TEXT  name of collection with output data in --db-name in
                       mongodb (default 'out_gh_data')
  --help               Show this message and exit.
```

Запустить утилиту можно следующим образом (предварительно загрузив данные в указанную коллекцию, если входная коллекция не будет найдена, утилита выведет соответствующее сообщение об ошибке):
```bash
python3 hw3_mongo_cli.py task-8 --db-name hw3 --in-col-name gh_data --out-col-name gh_data_t8_out  mongodb://user:pass@127.0.0.1:27017/
```