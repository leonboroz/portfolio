# Описание проекта

## API_date_load

Этот скрипт предназначен для загрузки данных из API, их очистки и обработки, а также для загрузки данных в базу данных PostgreSQL.

## Описание

Скрипт подключается к API, загружает данные в формате JSON, очищает их, преобразует и затем загружает в базу данных PostgreSQL. Для этого создаются необходимые классы для подключения к API, очистки данных и взаимодействия с базой данных.

## Установка

Для того чтобы запустить скрипт, необходимо выполнить несколько шагов:

### 1. Установите необходимые библиотеки:

Создайте виртуальное окружение (рекомендуется) и активируйте его:
Затем установите все зависимости:

pip install -r requirements.txt


### 2. Создать файл конфигурации для API `config_API.json`:
    ```json
    {
        "url": "https://api.example.com/data",
        "client": "your_client_id",
        "client_key": "your_client_key",
        "interval": 7
    }
    ```

### 3. Создать файл конфигурации для базы данных `config_DB.json`:
    ```json
    {
        "dbname_start": "postgres",
        "user": "your_db_user",
        "password": "your_db_password",
        "host": "localhost",
        "port": 5432,
        "new_base": "new_database",
        "new_schema": "public",
        "new_table": "data_table",
        "stroc": ["user_id INT", "oauth_consumer_key VARCHAR(255)", "lis_result_sourcedid VARCHAR(255)", "lis_outcome_service_url VARCHAR(255)", "is_correct BOOLEAN", "attempt_type VARCHAR(255)", "created_at TIMESTAMP"]
    }
    ```

## Запуск

Для запуска скрипта просто выполните его:


python ETL  from API to Database.py

## Описание классов и методов

## APIClient
### Методы:
get_instance(url=None, param=None) — возвращает единственный экземпляр класса (Singleton), подключается к API.
close_connect() — закрывает подключение к API.

## Dataload
### Методы:
load() — загружает данные с API.

## Transform
### Методы:
transf() — очищает и преобразует данные, подготавливая их к загрузке в базу данных.

## Database
### Методы:
connect_and_create_base() — подключается к базе данных, создаёт базу данных и таблицу.
loading_to_base() — загружает обработанные данные в таблицу базы данных.

## LogManager
###  Методы:
cleanup_old_logs() — удаляет старые лог-файлы.
Логирует все важные события, такие как ошибки или успешные операции.

### Логирование
Все логи хранятся в директории logs. Старые логи автоматически удаляются через заданное количество дней. Логи включают информацию об ошибках подключения, загрузке данных и успешных операциях.

### Дополнительная информация
Этот скрипт легко адаптируется под различные API и базы данных. Можно модифицировать методы обработки данных и логику работы с БД в зависимости от ваших потребностей.
