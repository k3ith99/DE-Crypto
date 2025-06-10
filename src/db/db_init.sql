DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'crypto') THEN 
        CREATE DATABASE crypto;
    END IF;
END $$;
\c crypto;


CREATE TABLE IF NOT EXISTS crypto (
  id SERIAL PRIMARY KEY,
  name VARCHAR(20),
  ticker VARCHAR(5) NOT NULL UNIQUE,
  year_founded INT
);

CREATE TABLE IF NOT EXISTS time (
  id INT PRIMARY KEY,
  datetime TIMESTAMP NOT NULL,
  year INT,
  month INT,
  day INT,
  hour INT,
  minute INT
);

CREATE TABLE IF NOT EXISTS price (
  id SERIAL PRIMARY KEY,
  crypto_id INT NOT NULL,
  time_id INT NOT NULL,
  currency VARCHAR,
  open DECIMAL,
  close DECIMAL,
  volume DECIMAL,
  FOREIGN KEY (crypto_id) REFERENCES crypto (id),
  FOREIGN KEY (time_id) REFERENCES time (id),
  UNIQUE (crypto_id, time_id)  -- Optional: prevents duplicate entries for same crypto and time
);
CREATE TABLE IF NOT EXISTS calculations (
  id SERIAL PRIMARY KEY,
  crypto_id INT NOT NULL,
  time_id INT NOT NULL,
  pct_change DECIMAL,
  std DECIMAL,
  sma DECIMAL,
  ema DECIMAL,
  vroc DECIMAL,
  FOREIGN KEY (time_id) REFERENCES time (id),
  FOREIGN KEY (crypto_id) REFERENCES crypto (id),
  UNIQUE (crypto_id, time_id)
);


INSERT INTO crypto (name, ticker)
VALUES 
  ('Bitcoin', 'BTC'),
  ('Ethereum', 'ETH'),
  ('Litecoin', 'LTC'),
  ('XRP Ledger', 'XRP'),
  ('BNB', 'BNB')
  ON CONFLICT (ticker) DO NOTHING;