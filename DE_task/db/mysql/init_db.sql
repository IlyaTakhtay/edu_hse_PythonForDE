-- БД, юзер и гранты
CREATE DATABASE IF NOT EXISTS ${MYSQL_APP_DB};
CREATE USER IF NOT EXISTS '${MYSQL_APP_USER}'@'%' IDENTIFIED BY '${MYSQL_APP_PASSWORD}';
GRANT ALL PRIVILEGES ON ${MYSQL_APP_DB}.* TO '${MYSQL_APP_USER}'@'%';

FLUSH PRIVILEGES;

USE ${MYSQL_APP_DB};

-- Таблица Users
CREATE TABLE IF NOT EXISTS Users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loyalty_status ENUM('Gold', 'Silver', 'Bronze', 'None') DEFAULT 'None'
);

-- Таблица ProductCategories
CREATE TABLE IF NOT EXISTS ProductCategories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    parent_category_id INT,
    FOREIGN KEY (parent_category_id) REFERENCES ProductCategories(category_id)
);

-- Таблица Products
CREATE TABLE IF NOT EXISTS Products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category_id INT,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT NOT NULL,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES ProductCategories(category_id)
);

-- Таблица Orders
CREATE TABLE IF NOT EXISTS Orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status ENUM('Pending', 'Completed', 'Cancelled', 'Shipped', 'Processing') DEFAULT 'Pending',
    delivery_date TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);

-- Таблица OrderDetails
CREATE TABLE IF NOT EXISTS OrderDetails (
    order_detail_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT NOT NULL,
    price_per_unit DECIMAL(10, 2) NOT NULL,
    total_price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);