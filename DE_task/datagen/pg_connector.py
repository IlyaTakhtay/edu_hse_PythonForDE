import psycopg2
from psycopg2 import pool
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Обработчик для вывода в консоль
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

class PgConnector:
    def __init__(self, db_config, minconn=1, maxconn=5):
        self.db_config = db_config
        self.minconn = minconn
        self.maxconn = maxconn
        self.connection_pool = self._create_connection_pool()

    def _create_connection_pool(self):
        try:
            logger.info(f"Создание пула соединений к {self.db_config['host']}...")
            conn_pool = psycopg2.pool.SimpleConnectionPool(
                self.minconn,
                self.maxconn,
                **self.db_config
            )
            logger.info("Пул соединений успешно создан!")
            return conn_pool
        except Exception as e:
            logger.error(f"Ошибка при создании пула соединений: {e}")
            raise

    def get_connection(self):
        try:
            logger.info("Получение соединения из пула...")
            connection = self.connection_pool.getconn()
            logger.info("Соединение успешно получено из пула.")
            return connection
        except Exception as e:
            logger.error(f"Ошибка при получении соединения из пула: {e}")
            raise

    def put_connection(self, connection):
        try:
            logger.info("Возвращение соединения в пул...")
            self.connection_pool.putconn(connection)
            logger.info("Соединение возвращено в пул.")
        except Exception as e:
            logger.error(f"Ошибка при возвращении соединения в пул: {e}")
            raise

    def close_all_connections(self):
        try:
            logger.info("Закрытие всех соединений...")
            self.connection_pool.closeall()
            logger.info("Все соединения закрыты.")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединений: {e}")
            raise

    def execute(self, query, params=None, fetchone=False, fetchall=False):
        connection = None  # Инициализируем переменную
        try:
            connection = self.get_connection()
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                connection.commit()

                if fetchone:
                    result = cursor.fetchone()
                    return result
                elif fetchall:
                    result = cursor.fetchall()
                    return result

        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            raise
        finally:
            if connection:
                self.put_connection(connection)