import pymysql

# Create mysql connection
mysql_connection = pymysql.connect(
    host="192.168.1.121",
    user="tiberius",
    password="q123",
    db="cryptohamster",
    port=3306,
    charset="utf8",
)

# Get Cursor
cursor = mysql_connection.cursor()

cursor.execute(
    """CREATE TABLE IF NOT EXISTS raw_hamsterwheel(
    id INT NOT NULL AUTO_INCREMENT,
    hash VARCHAR(255) NOT NULL,
    time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    magnet TINYINT(1) NOT NULL,
    PRIMARY KEY ( id )
    );
"""
)

mysql_connection.commit()

cursor.execute(
    """CREATE TABLE IF NOT EXISTS closed_hamsterwheel(
    id INT NOT NULL AUTO_INCREMENT,
    hash VARCHAR(255) NOT NULL,
    time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    magnet TINYINT(1) NOT NULL,
    PRIMARY KEY ( id )
    );
"""
)

cursor.execute(
    """CREATE TABLE IF NOT EXISTS log(
    id INT NOT NULL AUTO_INCREMENT,
    time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    event VARCHAR(100) NOT NULL,
    PRIMARY KEY ( id )
    );
    """
)

mysql_connection.commit()

cursor.execute(
    """CREATE TABLE IF NOT EXISTS wallet(
            id INT NOT NULL AUTO_INCREMENT,
    currency_symbol VARCHAR(50) NOT NULL,
    amount FLOAT NOT NULL,
    PRIMARY KEY ( id )
    );
    """
)

mysql_connection.commit()

cursor.execute(
    """CREATE TABLE IF NOT EXISTS price(
    id INT NOT NULL AUTO_INCREMENT,
    currency_symbol VARCHAR(50) NOT NULL,
    price FLOAT NOT NULL,
    PRIMARY KEY ( id )
    );
    """
)

mysql_connection.commit()

cursor.execute(
    """CREATE TABLE IF NOT EXISTS orderbook(
    id INT NOT NULL AUTO_INCREMENT,
    time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    wallet_id INT NOT NULL,
    price FLOAT NOT NULL,
    amount FLOAT NOT NULL,
    currency VARCHAR(50) NOT NULL,
    type VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    PRIMARY KEY ( id ),
    FOREIGN KEY ( wallet_id ) REFERENCES wallet( id )
    );
    """
)

mysql_connection.commit()

cursor.execute(
    """CREATE TABLE IF NOT EXISTS decision(
    id INT NOT NULL AUTO_INCREMENT,
    start_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    end_time TIMESTAMP(6),
    number_hamsterwheels INT,
    type VARCHAR(50) NOT NULL,
    status VARCHAR(100),
    result VARCHAR(50),
    PRIMARY KEY ( id )
    );
"""
)

mysql_connection.close()
