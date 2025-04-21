DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'crypto') THEN 
        CREATE DATABASE crypto;
    END IF;
END $$;

\c crypto;

CREATE TABLE IF NOT EXISTS Crypto (
    id BIGINT PRIMARY KEY,
    name VARCHAR(20) NOT NULL,
    ticker VARCHAR(5) NOT NULL
);

CREATE TABLE IF NOT EXISTS Time (
    id BIGINT PRIMARY KEY,
    crypto_id INT NOT NULL REFERENCES Crypto(id),
    timestamp TIMESTAMP,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    second INT
);

CREATE TABLE IF NOT EXISTS Price (
    id BIGINT PRIMARY KEY,
    crypto_id INT NOT NULL REFERENCES Crypto(id),
    time_id BIGINT NOT NULL REFERENCES Time(id),
    currency VARCHAR(20),
    open DECIMAL,
    close DECIMAL,
    volume DECIMAL
);

    