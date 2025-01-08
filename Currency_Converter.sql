-- Table to store exchange rates
CREATE TABLE exchange_rates (
    rate_id NUMBER PRIMARY KEY,
    currency_code VARCHAR2(3) NOT NULL,
    rate_to_usd NUMBER(10, 4) NOT NULL,
    rate_date DATE NOT NULL
);

-- Table to store user transactions
CREATE TABLE user_transactions (
    transaction_id NUMBER PRIMARY KEY,
    user_id NUMBER NOT NULL,
    from_currency VARCHAR2(3) NOT NULL,
    to_currency VARCHAR2(3) NOT NULL,
    amount NUMBER(12, 2) NOT NULL,
    conversion_rate NUMBER(10, 4) NOT NULL,
    converted_amount NUMBER(12, 2) NOT NULL,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to log conversion history
CREATE TABLE conversion_history (
    history_id NUMBER PRIMARY KEY,
    user_id NUMBER NOT NULL,
    currency_code VARCHAR2(3) NOT NULL,
    amount NUMBER(12, 2),
    conversion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create sequence for user_transactions table
CREATE SEQUENCE user_transactions_SEQ START WITH 1 INCREMENT BY 1;

-- Create sequence for conversion_history table
CREATE SEQUENCE conversion_history_SEQ START WITH 1 INCREMENT BY 1;

-- Trigger to log rate updates and notify users if rate change exceeds 5%
CREATE OR REPLACE TRIGGER notify_rate_update
AFTER UPDATE ON exchange_rates
FOR EACH ROW
BEGIN
    IF :OLD.rate_to_usd IS NOT NULL AND :NEW.rate_to_usd IS NOT NULL THEN
        IF ABS(:NEW.rate_to_usd - :OLD.rate_to_usd) / :OLD.rate_to_usd >= 0.05 THEN
            INSERT INTO conversion_history (history_id, user_id, currency_code, amount, conversion_date)
            VALUES (conversion_history_SEQ.NEXTVAL, 1, :NEW.currency_code, NULL, CURRENT_TIMESTAMP);
            -- Notification logic can be added here
        END IF;
    END IF;
END;
/

-- Procedure to calculate and store currency conversions
CREATE OR REPLACE PROCEDURE convert_currency(
    p_user_id IN NUMBER,
    p_from_currency IN VARCHAR2,
    p_to_currency IN VARCHAR2,
    p_amount IN NUMBER
)
IS
    conversion_rate NUMBER(10, 4);
    converted_amount NUMBER(12, 2);
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
    INSERT INTO user_transactions (transaction_id, user_id, from_currency, to_currency, amount, conversion_rate, converted_amount, transaction_date)
    VALUES (user_transactions_SEQ.NEXTVAL, p_user_id, p_from_currency, p_to_currency, p_amount, conversion_rate, converted_amount, CURRENT_TIMESTAMP);

    -- Log the conversion in the history table
    INSERT INTO conversion_history (history_id, user_id, currency_code, amount, conversion_date)
    VALUES (conversion_history_SEQ.NEXTVAL, p_user_id, p_to_currency, converted_amount, CURRENT_TIMESTAMP);

END;
/

-- Test the procedure
BEGIN
    convert_currency(
        p_user_id => 1,
        p_from_currency => 'EUR',
        p_to_currency => 'USD',
        p_amount => 100.00
    );
END;
/

-- Fetch data from user_transactions table
SELECT * FROM user_transactions;

-- Fetch data from conversion_history table
SELECT * FROM conversion_history;
