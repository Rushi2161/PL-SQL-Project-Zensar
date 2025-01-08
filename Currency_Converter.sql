-- Table to store exchange rates
CREATE TABLE exchange_rates (
    rate_id INT PRIMARY KEY,
    currency_code VARCHAR2(3) NOT NULL,
    rate_to_usd DECIMAL(10, 4) NOT NULL,
    rate_date DATE NOT NULL
);

-- Table to store user transactions
CREATE TABLE user_transactions (
    transaction_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    from_currency VARCHAR2(3) NOT NULL,
    to_currency VARCHAR2(3) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    conversion_rate DECIMAL(10, 4) NOT NULL,
    converted_amount DECIMAL(12, 2) NOT NULL,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to log conversion history
CREATE TABLE conversion_history (
    history_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    currency_code VARCHAR2(3) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    conversion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fetch exchange rates for EUR
SELECT * 
FROM exchange_rates 
WHERE currency_code = 'EUR';

-- Fetch exchange rates for a given date range
SELECT * 
FROM exchange_rates 
WHERE rate_date BETWEEN TO_DATE('2024-01-01', 'YYYY-MM-DD') 
                    AND TO_DATE('2024-12-31', 'YYYY-MM-DD');

-- Get the trend of exchange rates
SELECT currency_code, 
       rate_date, 
       rate_to_usd,
       LAG(rate_to_usd) OVER (PARTITION BY currency_code ORDER BY rate_date) AS previous_rate,
       CASE 
           WHEN rate_to_usd > LAG(rate_to_usd) OVER (PARTITION BY currency_code ORDER BY rate_date) THEN 'Increasing'
           WHEN rate_to_usd < LAG(rate_to_usd) OVER (PARTITION BY currency_code ORDER BY rate_date) THEN 'Decreasing'
           ELSE 'Stable'
       END AS trend
FROM exchange_rates;

-- Trigger to log rate updates and notify users if rate change exceeds 5%
CREATE OR REPLACE TRIGGER notify_rate_update
AFTER UPDATE ON exchange_rates
FOR EACH ROW
BEGIN
    IF ABS(:NEW.rate_to_usd - :OLD.rate_to_usd) / :OLD.rate_to_usd >= 0.05 THEN
        INSERT INTO conversion_history (user_id, currency_code, amount, conversion_date)
        VALUES (1, :NEW.currency_code, NULL, CURRENT_TIMESTAMP);
        -- You can integrate the actual notification mechanism here.
    END IF;
END;
/

-- Procedure to calculate and store currency conversions
CREATE OR REPLACE PROCEDURE convert_currency(
    p_user_id IN INT,
    p_from_currency IN VARCHAR2,
    p_to_currency IN VARCHAR2,
    p_amount IN DECIMAL
)
IS
    conversion_rate DECIMAL(10, 4);
    converted_amount DECIMAL(12, 2);
BEGIN
    -- Fetch the conversion rate for the specified currencies
    SELECT e1.rate_to_usd / e2.rate_to_usd INTO conversion_rate
    FROM exchange_rates e1
    JOIN exchange_rates e2
    ON e1.currency_code = p_from_currency 
    AND e2.currency_code = p_to_currency
    WHERE e1.rate_date = (SELECT MAX(rate_date) FROM exchange_rates WHERE currency_code = p_from_currency)
      AND e2.rate_date = (SELECT MAX(rate_date) FROM exchange_rates WHERE currency_code = p_to_currency);

    -- Calculate the converted amount
    converted_amount := p_amount * conversion_rate;

    -- Record the transaction
    INSERT INTO user_transactions (transaction_id, user_id, from_currency, to_currency, amount, conversion_rate, converted_amount)
    VALUES (user_transactions_SEQ.NEXTVAL, p_user_id, p_from_currency, p_to_currency, p_amount, conversion_rate, converted_amount);

    -- Log the conversion in the history table
    INSERT INTO conversion_history (history_id, user_id, currency_code, amount, conversion_date)
    VALUES (conversion_history_SEQ.NEXTVAL, p_user_id, p_to_currency, converted_amount, CURRENT_TIMESTAMP);
    
END;
/
