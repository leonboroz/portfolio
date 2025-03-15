# Импортируем модули
import datetime
from datetime import datetime,timedelta,date
import json
import psycopg2
import pytz
import requests
import ast
import uuid
import os
import glob
import logging

# Подготавливем переменные к API
with open('config_API.json') as config_file:
    config=json.load(config_file)
URL=config['url']
client=config['client']
client_key=config['client_key']
interval=config['interval']
zero_time=datetime.now(pytz.utc).replace(hour=0,minute=0,second=0,microsecond=0)
start=(zero_time-timedelta(days=interval)).strftime("%Y-%m-%d %H:%M:%S.%f")
end=zero_time.strftime("%Y-%m-%d %H:%M:%S.%f")
param={'client':client,'client_key':client_key,'start':start,'end':end}

#Создадим класс подключения и отключения к API и обработки ошибок
class APIClient:
    __instance = None  
#Метод для получения единственного экземпляра
    @staticmethod
    def get_instance(url=None, param=None):
        if not APIClient.__instance:
            APIClient.__instance = APIClient(URL, param)
        return APIClient.__instance

    def __init__(self, url, param):
        """Инициализация синглтона с параметрами API"""
        if APIClient.__instance:
            raise Exception("Этот класс является Singleton! Воспользуйся методом get_instance")  
        self.url = url
        self.params = param

        try:
            self.session = requests.Session()
            # Тестируем подключение через head
            response = self.session.head(self.url, params=self.params, timeout=10)
            response.raise_for_status()

            logging.info("Успешное подключение к API")

            APIClient.__instance = self  # Сохраняем экземпляр

        except requests.exceptions.Timeout:
            logging.warning("Ошибка: Время ожидания ответа от API истекло")
            return None
        except requests.exceptions.ConnectionError:
            logging.warningt(" Ошибка: Не удалось подключиться к API (проблемы с сетью)")
            return None
        except requests.exceptions.HTTPError as err:
            logging.error(f" Ошибка HTTP: {err}")
            return None
        except Exception as e:
            logging.error(f" Ошибка: {e}")
            return None
    def close_connect(self):
        APIClient.__instance.session.close()  # Закрываем сессию
        logging.info("Соединение закрыто")
        APIClient.__instance= None  # Сбрасываем Singleton       

#Создаем класс загрузки сырых данных
class Dataload:
    def __init__(self,api_client):        
        self.api_client=api_client
        
    def load(self):
        
        try:
            res=self.api_client.session.get(self.api_client.url,params=self.api_client.params)
            res.raise_for_status()        
        except requests.exceptions.HTTPError as err:
            logging.error(f" Ошибка HTTP: {err}")
            return None
        except Exception as e:
            logging.error(f" Ошибка: {e}")
            return None
        
        try:
            data=res.json()
            logging.info ("Данные корректные и являются JSON")
            logging.info ("Данные загружены")
            return data
        except ValueError:
            logging.warning ('полученные данные не JSON')
            return None
      
      # Создаем класс очистки данных и подготовки к загрузке в БД
class Transform:
    def __init__(self,data):
        self.data=data 
    def transf(self):
        if len([i for i in self.data if  not i['lti_user_id'] or not i['passback_params']]):
            logging.warning ("Внимание! В исходных данных есть пустые значения lti_user_id и passback_params ")
        data_to=[i for i in self.data if i['lti_user_id'] and i['passback_params']]
        for i in data_to:
            if not isinstance(i['passback_params'],dict):
                    i['passback_params']=ast.literal_eval(i['passback_params']) 
        for i in data_to:
             i['passback_params']['oauth_consumer_key']=str(uuid.uuid5(uuid.NAMESPACE_DNS, str(i['lti_user_id'])))
        
        data_to_load = [
             (
                i['lti_user_id'],
                i['passback_params'].get('oauth_consumer_key', None),
                i['passback_params'].get('lis_result_sourcedid', None),
                i['passback_params'].get('lis_outcome_service_url', None),  
                i.get('is_correct', None),
                i.get('attempt_type', None),
                i.get('created_at', None)
            ) for i in data_to
        ]
        
        return data_to_load
    
    # Подготовим пересенные к SQL подключению
with open ('config_DB.json') as fill:
    data1=json.load(fill)
DBNAME=data1['dbname_start']
USER=data1['user']
PASSWORD=data1['password']
HOST=data1['host']
PORT=data1['port']
base=data1['new_base']
schema=data1['new_schema']
table=data1['new_table']
parametrs=', '.join(data1['stroc'])

class Database:
    def __init__(self, dbname, user, password, host, port, base, schema, table, parameters, data):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.base = base
        self.schema = schema
        self.table = table
        self.parameters = parameters
        self.data = data

    def connect_and_create_base(self):
        # Подключаемся к серверу, используя существующую базу данных 
        conn = psycopg2.connect(
            dbname=self.dbname,  
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

        logging.info("Успешное подключение к серверу!")

        # Создаем курсор
        cur = conn.cursor()

        # Закрываем текущую транзакцию, чтобы создать базу данных вне транзакции
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        # Создаем новую базу данных
        cur.execute(f"CREATE DATABASE  {self.base}")
        logging.info(f"Новая база данных '{self.base}' создана")

        # Закрываем соединение
        conn.close()

        # Подключаемся к новой базе данных
        conn = psycopg2.connect(
            dbname=self.base,  # Теперь подключаемся к созданной базе
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

        logging.info(f"Успешно подключились к новой базе данных '{self.base}'!")

        cur=conn.cursor()

        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
        logging.info(f"Схема {self.schema} создана!")
        conn.commit()

        cur.execute(f"CREATE TABLE {self.schema}.{self.table} ({parametrs})")
        logging.info (f"Таблица {self.table} создана!")
        conn.commit()
        
        cur.close()
        conn.close()# Закрываем соединение
                
    def loading_to_base(self):
        conn = psycopg2.connect(
            dbname=self.base,  # Подключаемся к созданной базе
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        logging.info(f"Успешно подключились к базе данных {self.base} для загрузки данных!")

        cur=conn.cursor()
        cur.executemany(f"""INSERT INTO {self.schema}.{self.table} 
                       (user_id, oauth_consumer_key, lis_result_sourcedid, 
                       lis_outcome_service_url, is_correct, attempt_type, created_at) 
                       VALUES (%s, %s, %s, %s,%s,%s, %s)
                       ON CONFLICT (user_id,created_at) 
                       DO NOTHING""", self.data)
        logging.info ("Данные загружены в таблицу!")
        conn.commit()
        logging.info ("Данные зафиксированы")
        cur.close()
        conn.close()
        logging.info ("Курсор и соединение закрыто")

        # Создадим класс логгинга
class LogManager:
    def __init__(self, log_dir="logs", log_file="app.log", days_to_keep=3):
        """Инициализация логирования с кодировкой UTF-8"""
        self.log_dir = log_dir
        self.log_file = log_file
        self.days_to_keep = days_to_keep

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        self.cleanup_old_logs()  # Удаляем старые логи

        log_path = os.path.join(log_dir, log_file)

        # Создаём обработчик файла с кодировкой UTF-8
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", 
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)

        # Настроим логгер без консольного вывода
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        # Убираем все обработчики, если они есть (например, консольный)
        if self.logger.handlers:
            self.logger.handlers.clear()

        # Добавляем только file_handler
        self.logger.addHandler(file_handler)

    def cleanup_old_logs(self):
        """Удаление старых логов"""
        now = datetime.now()
        log_files = glob.glob(os.path.join(self.log_dir, "*.log"))

        for log_file in log_files:
            file_mtime = os.path.getmtime(log_file)
            file_date = datetime.fromtimestamp(file_mtime)

            if file_date < now - timedelta(days=self.days_to_keep):
                os.remove(log_file)
                print(f"Удален старый лог: {log_file}")



a=LogManager(log_file=f"{datetime.now().strftime('%Y-%m-%d')}.log")
s1=APIClient.get_instance(URL,param)
d=Dataload(s1)
a=d.load()
s1.close_connect()
d=Transform(a)
data=d.transf()
x=Database(DBNAME,USER,PASSWORD,HOST,PORT,base,schema,table,parametrs,data)
#x.connect_and_create_base()
x.loading_to_base()
