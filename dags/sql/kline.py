get_kline_datetime_sql = """
SELECT DATE_FORMAT(MIN(snapshot_datetime), "%Y-%m-%d %H:%i:%S")
FROM snapshot_{year_month}
WHERE is_kline = false AND exchange = "{exchange}";
"""

upsert_with_kline_sql = """
INSERT INTO snapshot_{year_month} (base_coin, quote_coin, price, snapshot_datetime, exchange, is_kline)
VALUES {new_rows}
ON DUPLICATE KEY UPDATE price = VALUES(price), is_kline = VALUES(is_kline);
"""