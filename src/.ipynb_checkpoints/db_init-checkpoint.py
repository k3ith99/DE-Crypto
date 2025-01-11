import psycopg2


# connection establishment
def main():
    conn= psycopg2.connect(
        #database="crypto",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432",
    )
    conn.autocommit = True

    # Creating a cursor object
    cursor = conn.cursor()

    # query to create a database
    sql_db = """ CREATE DATABASE crypto """

    # executing above query
    cursor.execute(sql_db)
    print("Database has been created successfully !!")
    conn = psycopg2.connect(
        database="crypto",  # Switch to the 'crypto' database
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432",
    )
    conn.autocommit = True
    cursor = conn.cursor()
    sql_tables= """
    CREATE TABLE Crypto (
        id INT NOT NULL PRIMARY KEY,
        name VARCHAR(20) NOT NULL,
        ticker VARCHAR(5) NOT NULL
    ) ;
    CREATE TABLE Time (
        id INT NOT NULL PRIMARY KEY,
        crypto_id INT NOT NULL references Crypto(id),
        timestamp TIMESTAMP,
        year INT,
        month INT,
        day INT,
        hour INT,
        minute INT,
        second INT
    );
    CREATE TABLE Price (
        id INT NOT NULL PRIMARY KEY,
        crypto_id INT NOT NULL references Crypto(id),
        time_id INT NOT NULL references Time(id),
        currency VARCHAR(20),
        open DECIMAL,
        close DECIMAL,
        volume DECIMAL
    );
    
    """

    # executing above query
    cursor.execute(sql_tables)
    print("Tables Crypto, Time and Price have been created successfully !!")

    # Closing the connection
    conn.close()


if __name__ == "__main__":
    main()
