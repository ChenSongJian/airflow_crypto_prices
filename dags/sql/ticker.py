create_ticker_table_sql = """
CREATE TABLE tickers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    base_coin VARCHAR(20) NOT NULL,
    quote_coin VARCHAR(20) NOT NULL,
    price DECIMAL(36, 16) NOT NULL,
    collect_datetime DATETIME NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    UNIQUE KEY idx_unique_ticker (base_coin, quote_coin, exchange),
    INDEX idx_base_coin_ticker (base_coin),
    INDEX idx_quote_coin_ticker (quote_coin),
    INDEX idx_exchange_ticker (exchange),
    INDEX idx_collect_datetime (collect_datetime)
);
"""

upsert_tickers_sql = """
INSERT INTO tickers (base_coin, quote_coin, price, exchange, collect_datetime)
VALUES {new_rows}
ON DUPLICATE KEY UPDATE price = VALUES(price), collect_datetime = VALUES(collect_datetime);
"""