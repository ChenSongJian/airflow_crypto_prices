create_snapshot_table_sql = """
CREATE TABLE snapshot_{year_month} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    base_coin VARCHAR(20) NOT NULL,
    quote_coin VARCHAR(20) NOT NULL,
    price DECIMAL(36, 16) NOT NULL,
    snapshot_datetime DATETIME NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    is_kline BOOLEAN NOT NULL DEFAULT false,
    UNIQUE KEY idx_unique_snapshot (base_coin, quote_coin, exchange, snapshot_datetime),
    INDEX idx_base_coin_snapshot (base_coin),
    INDEX idx_quote_coin_snapshot (quote_coin),
    INDEX idx_exchange_snapshot (exchange),
    INDEX idx_snapshot_datetime (snapshot_datetime)
);
"""

snapshot_price_sql = """
INSERT INTO snapshot_{year_month} (base_coin, quote_coin, price, snapshot_datetime, exchange)
SELECT base_coin, quote_coin, price, "{snapshot_datetime}", exchange
FROM tickers
"""
