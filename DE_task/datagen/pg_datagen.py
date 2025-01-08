import os
import logging
import random
import requests
import json
import datetime
from faker import Faker
from pg_connector import PgConnector

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

postgres_host = os.environ.get("POSTGRESQL_APP_HOST")
postgres_db = os.environ.get("POSTGRESQL_APP_DB")
postgres_schema = os.environ.get("POSTGRESQL_APP_SCHEMA")
postgres_user = os.environ.get("POSTGRESQL_APP_USER")
postgres_password = os.environ.get("POSTGRESQL_APP_PASSWORD")
db_config = {
    "user": postgres_user,
    "password": postgres_password,
    "host": postgres_host,
    "port": 5432,
    "database": postgres_db,
    "options": f"-c search_path={postgres_schema}"
}

class DataGenerator:
    def __init__(self, connector, num_users=100, num_categories=20, num_products=40, num_orders=50):
        self.connector = connector
        self.num_users = num_users
        self.num_categories = num_categories
        self.num_products = num_products
        self.num_orders = num_orders
        self.fake = Faker('ru_RU')
        self.fake.add_provider(CommerceProvider)
        self.category_ids = []
        self.token_expire = 0
        self.__access_token = None

    def table_is_empty(self, table_name):
        """Проверяет, пуста ли таблица."""
        query = f"SELECT COUNT(*) FROM {table_name};"
        count = self.connector.execute(query, fetchone=True)[0]
        return count == 0

    def create_users(self):
        if self.table_is_empty("users"):
            logger.info(f"Генерация {self.num_users} пользователей...")
            for _ in range(self.num_users):
                gender = random.choice(['male', 'female'])
                if gender == 'male':
                    first_name = self.fake.first_name_male()
                    last_name = self.fake.last_name_male()
                else:
                    first_name = self.fake.first_name_female()
                    last_name = self.fake.last_name_female()
                email = self.fake.email()
                phone = self.fake.numerify(text='8-###-###-####')
                registration_date = self.fake.date_time_this_decade()
                loyalty_status = random.choice(["Gold", "Silver", "Bronze"])

                query = """
                INSERT INTO users (first_name, last_name, email, phone, registration_date, loyalty_status)
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                self.connector.execute(query, (first_name, last_name, email, phone, registration_date, loyalty_status))
        else:
            logger.info("Таблица users не пуста, пропускаем генерацию пользователей.")

    def generate_categories(self, num_root_categories=20, max_subcategories=5, max_depth=2):
        root_categories = [
            "Электроника", "Бытовая техника", "Одежда", "Обувь", "Спорт и отдых",
            "Товары для дома", "Книги",
            "Автотовары", "Инструменты",
            "Часы",
        ]

        subcategories = {
            "Электроника": ["Смартфоны", "Ноутбуки", "Планшеты", "Телевизоры", "Фотоаппараты", "Наушники", "Компьютеры", "Комплектующие", "Игровые приставки", "Умные часы", "Фитнес-браслеты", "Внешние аккумуляторы"],
            "Бытовая техника": ["Холодильники", "Стиральные машины", "Посудомоечные машины", "Пылесосы", "Микроволновые печи", "Духовые шкафы", "Варочные панели", "Вытяжки", "Кофемашины", "Чайники", "Утюги", "Мультиварки"],
            "Одежда": ["Женская одежда", "Мужская одежда", "Детская одежда", "Верхняя одежда", "Нижнее белье", "Спортивная одежда"],
            "Обувь": ["Женская обувь", "Мужская обувь", "Детская обувь", "Спортивная обувь"],
            "Спорт и отдых": ["Тренажеры", "Велосипеды", "Туризм", "Рыбалка", "Спортивное питание"],
            "Товары для дома": ["Посуда", "Текстиль", "Освещение", "Декор", "Бытовая химия", "Хранение вещей"],
            "Книги": ["Художественная литература", "Учебная литература", "Научно-популярная литература", "Детские книги", "Бизнес-литература"],
            "Автотовары": ["Автоэлектроника", "Автохимия", "Шины", "Диски", "Инструменты", "Аксессуары"],
            "Инструменты": ["Ручной инструмент", "Электроинструмент", "Измерительный инструмент", "Расходные материалы"],
            "Часы": ["Наручные часы", "Настенные часы", "Напольные часы", "Будильники"],
        }

        categories_dict = {}
        selected_root_categories = random.sample(root_categories, num_root_categories)

        def add_subcategories(parent_category, category_dict, depth=0):
            if depth >= max_depth:
                return

            if parent_category in subcategories:
                num_subcats = random.randint(1, max_subcategories)
                selected_subcategories = random.sample(subcategories[parent_category], num_subcats)
                category_dict[parent_category] = {}
                for subcategory in selected_subcategories:
                    add_subcategories(subcategory, category_dict[parent_category], depth + 1)
            else:
                category_dict[parent_category] = {}

        for root_category in selected_root_categories:
            add_subcategories(root_category, categories_dict)

        return categories_dict

    def create_product_categories(self):
        if self.table_is_empty("productcategories"):
            logger.info(f"Генерация {self.num_categories} категорий товаров...")
            categories_data = self.generate_categories(num_root_categories=self.num_categories // 3, max_subcategories=3, max_depth=2)

            def insert_categories(categories, parent_id=None):
                for category_name, subcategories in categories.items():
                    query = """
                    INSERT INTO productcategories (name, parent_category_id)
                    VALUES (%s, %s)
                    RETURNING category_id;
                    """
                    category_id = self.connector.execute(query, (category_name, parent_id), fetchone=True)[0]
                    self.category_ids.append(category_id)
                    insert_categories(subcategories, category_id)

            insert_categories(categories_data)
            logger.info(f"Категории успешно добавлены в базу данных. Всего категорий: {len(self.category_ids)}")
        else:
            logger.info("Таблица productcategories не пуста, пропускаем генерацию категорий.")

    def update_access_token(self, user_id, user_api_key):
        url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"

        payload = {
            'scope': 'GIGACHAT_API_PERS'
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'RqUID': f'{user_id}',
            'Authorization': f'Basic {user_api_key}'
        }

        response = requests.request("POST", url, headers=headers, data=payload, verify=False)
        self.__access_token = response.json()['access_token']
        self.token_expire = response.json()['expires_at']

    def generate_product(self, category):
        desc_length = 8
        user_id = os.environ.get("AI_USER_ID")
        user_api_key = os.environ.get("AI_API_KEY")
        if self.token_expire < datetime.datetime.now().timestamp():
            logging.debug(datetime.datetime.now().timestamp())
            logging.debug("токен истек")
            self.update_access_token(user_id, user_api_key)

        api_url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json"
        }

        payload = json.dumps({
            "model": "GigaChat",
            "messages": [
                {
                    "role": "system",
                    "content":
                    """Придумай название и описание товара. Должно включать известный бренд.
                    Без кавычек и лишних знаков препинания.
                    Примеры:
                    Adidas Palermo S0245 | Кеды для ходьбы.
                    Samsung Galaxy S23 SM-S9**B | Смартфон с 6.1\" AMOLED экраном.
                    Bosch Serie 6  BGS412234A | Тихий и мощный пылесос.
                    Canon EOS 250D Kit 18-55 IS STM | Зеркальный фотоаппарат для начинающих.
                    Строгое соответствие формату: {название}|{описание}. Наличие | обязательно. Если не можешь придумать описание, оставь поле пустым после |
                    """
                },
                {
                    "role": "user",
                    "content": f"Категория: {category}. Имя и краткое описание товара (максимальная длина {desc_length} слов)"
                }
            ],
            "max_tokens": 30
        })
        response = requests.request("POST", api_url, headers=headers, data=payload, verify=False)
        logging.debug(response.json())
        generated_text = response.json()['choices'][0]['message']['content']
        # generated_text = response.json()
        logging.debug(generated_text)
        gen = generated_text.split('|')
        if len(gen) >= 2:
            name = gen[0]
            description = gen[1]
        else:
            name=gen[0]
            description = "empty"

        return name, description

    def create_products(self, new_generation: bool = False):
        if self.table_is_empty("products"):
            logger.info(f"Генерация {self.num_products} товаров...")
            category_data = self.connector.execute("SELECT category_id, name FROM productcategories;", fetchall=True)
            category_dict = {row[0]: row[1] for row in category_data}
            category_id = random.choice(self.category_ids)
            generated_names = []
            for _ in range(self.num_products):
                category_id = random.choice(self.category_ids)
                if new_generation:
                    name, description = self.generate_product(category_dict[category_id])
                    while name in generated_names:
                        name, description = self.generate_product(category_dict[category_id])
                    generated_names.append(name)
                else:
                    name = "empty"
                    description = "empty"
                price = round(random.uniform(10.0, 400.0), 2)
                stock_quantity = random.randint(1, 150)
                creation_date = self.fake.date_time_this_year() - datetime.timedelta(days=random.randint(1, 365))

                query = """
                INSERT INTO products (name, description, category_id, price, stock_quantity, creation_date)
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                self.connector.execute(query, (name, description, category_id, price, stock_quantity, creation_date))
        else:
            logging.info("Таблица productcategories не пуста, пропускаем генерацию продуктов.")

    def create_orders(self):
        if self.table_is_empty("orders"):
            logger.info(f"Генерация {self.num_orders} заказов...")
            user_ids = self.connector.execute("SELECT user_id FROM Users;", fetchall=True)
            user_ids = [row[0] for row in user_ids]

            for _ in range(self.num_orders):
                user_id = random.choice(user_ids)
                order_date = self.fake.date_time_this_year()
                total_amount = 0
                status = random.choice(["Pending", "Completed", "Cancelled"])
                delivery_date = self.fake.date_between_dates(date_start=order_date, date_end="+14d")

                query = """
                INSERT INTO Orders (user_id, order_date, total_amount, status, delivery_date)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING order_id;
                """
                order_id = self.connector.execute(query, (user_id, order_date, total_amount, status, delivery_date), fetchone=True)[0]
                self.create_order_details(order_id)
        else:
            logging.debug("Таблица orders не пуста, пропускаем генерацию продуктов.")

    def create_order_details(self, order_id):
        product_ids_prices = self.connector.execute("SELECT product_id, price FROM products;", fetchall=True)
        num_items = random.randint(1, 5)
        order_total = 0

        for _ in range(num_items):
            product_id, price_per_unit = random.choice(product_ids_prices)
            quantity = random.randint(1, 10)
            total_price = round(price_per_unit * quantity, 2)
            order_total += total_price

            query = """
            INSERT INTO OrderDetails (order_id, product_id, quantity, price_per_unit, total_price)
            VALUES (%s, %s, %s, %s, %s);
            """
            self.connector.execute(query, (order_id, product_id, quantity, price_per_unit, total_price))

        # Обновление общей суммы заказа
        update_query = "UPDATE orders SET total_amount = %s WHERE order_id = %s;"
        self.connector.execute(update_query, (order_total, order_id))

    def generate_all(self):
        logger.info("Начало генерации данных...")
        self.create_users()
        self.create_product_categories()
        self.create_products(new_generation=True)
        self.create_orders()
        logger.info("Генерация данных завершена.")


# Выполнение генерации данных
if __name__ == "__main__":
    connector = None
    try:
        connector = PgConnector(db_config=db_config)
        generator = DataGenerator(connector, num_users=10, num_categories=20, num_products=35, num_orders=5)
        generator.generate_all()
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
    finally:
        if connector:
            connector.close_all_connections()