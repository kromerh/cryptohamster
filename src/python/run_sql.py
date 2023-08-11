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
    """CREATE TABLE raw_hamsterwheel(
    id INT NOT NULL AUTO_INCREMENT,
    time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    magnet TINYINT(1) NOT NULL,
    PRIMARY KEY ( id )
    );
"""
)

mysql_connection.commit()
mysql_connection.close()
