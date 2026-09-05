CREATE DATABASE banking_db;
USE banking_db;
SELECT DATABASE();
CREATE TABLE branches (
    branch_id INT PRIMARY KEY AUTO_INCREMENT,
    branch_name VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SHOW TABLES;
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    date_of_birth DATE,
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE accounts (
    account_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    branch_id INT NOT NULL,
    account_number VARCHAR(30) UNIQUE NOT NULL,
    account_type VARCHAR(30) NOT NULL,
    balance DECIMAL(15,2) DEFAULT 0,
    account_status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
);
CREATE TABLE transactions (
    transaction_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    account_id INT NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);
CREATE TABLE loans (
    loan_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    branch_id INT NOT NULL,
    loan_type VARCHAR(50) NOT NULL,
    loan_amount DECIMAL(15,2) NOT NULL,
    interest_rate DECIMAL(5,2),
    loan_status VARCHAR(20) DEFAULT 'ACTIVE',
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
);
CREATE TABLE loan_payments (
    payment_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    loan_id INT NOT NULL,
    payment_date DATE NOT NULL,
    payment_amount DECIMAL(15,2) NOT NULL,
    payment_status VARCHAR(20) DEFAULT 'SUCCESS',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (loan_id) REFERENCES loans(loan_id)
);
CREATE TABLE cards (
    card_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    account_id INT NOT NULL,
    card_number VARCHAR(30) UNIQUE NOT NULL,
    card_type VARCHAR(20) NOT NULL,
    card_status VARCHAR(20) DEFAULT 'ACTIVE',
    issue_date DATE,
    expiry_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);
CREATE TABLE card_transactions (
    card_transaction_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    card_id INT NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    transaction_type VARCHAR(30),
    amount DECIMAL(15,2) NOT NULL,
    merchant_name VARCHAR(150),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (card_id) REFERENCES cards(card_id)
);
CREATE TABLE beneficiaries (
    beneficiary_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    beneficiary_name VARCHAR(100) NOT NULL,
    account_number VARCHAR(30),
    bank_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
CREATE TABLE account_payments (
    payment_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    account_id INT NOT NULL,
    beneficiary_id INT,
    payment_date TIMESTAMP NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    payment_status VARCHAR(20) DEFAULT 'SUCCESS',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (beneficiary_id) REFERENCES beneficiaries(beneficiary_id)
);
USE banking_db;

-- 1. BRANCHES
INSERT INTO branches (branch_name, city, state)
VALUES
('Main Branch', 'Chennai', 'Tamil Nadu'),
('Central Branch', 'Madurai', 'Tamil Nadu'),
('North Branch', 'Coimbatore', 'Tamil Nadu'),
('East Branch', 'Trichy', 'Tamil Nadu'),
('South Branch', 'Tirunelveli', 'Tamil Nadu');


-- 2. CUSTOMERS
INSERT INTO customers
(first_name, last_name, email, phone, date_of_birth, address, city, state)
VALUES
('Arun', 'Kumar', 'arun@gmail.com', '9876543210', '1998-05-12', 'Anna Nagar', 'Chennai', 'Tamil Nadu'),
('Priya', 'Sharma', 'priya@gmail.com', '9876543211', '1997-08-20', 'KK Nagar', 'Madurai', 'Tamil Nadu'),
('Rahul', 'Raj', 'rahul@gmail.com', '9876543212', '1999-01-15', 'RS Puram', 'Coimbatore', 'Tamil Nadu'),
('Divya', 'Mohan', 'divya@gmail.com', '9876543213', '1996-11-03', 'Srirangam', 'Trichy', 'Tamil Nadu'),
('Karthik', 'Suresh', 'karthik@gmail.com', '9876543214', '2000-03-25', 'Palayamkottai', 'Tirunelveli', 'Tamil Nadu');


-- 3. ACCOUNTS
INSERT INTO accounts
(customer_id, branch_id, account_number, account_type, balance, account_status)
VALUES
(1, 1, 'ACC100001', 'SAVINGS', 50000.00, 'ACTIVE'),
(2, 2, 'ACC100002', 'SAVINGS', 75000.00, 'ACTIVE'),
(3, 3, 'ACC100003', 'CURRENT', 120000.00, 'ACTIVE'),
(4, 4, 'ACC100004', 'SAVINGS', 35000.00, 'ACTIVE'),
(5, 5, 'ACC100005', 'CURRENT', 90000.00, 'ACTIVE');


-- 4. TRANSACTIONS
INSERT INTO transactions
(account_id, transaction_date, transaction_type, amount, description)
VALUES
(1, '2026-08-01 10:15:00', 'DEPOSIT', 10000.00, 'Salary credit'),
(1, '2026-08-03 14:20:00', 'WITHDRAWAL', 2000.00, 'ATM withdrawal'),
(2, '2026-08-05 09:30:00', 'DEPOSIT', 15000.00, 'Cash deposit'),
(2, '2026-08-07 16:45:00', 'WITHDRAWAL', 5000.00, 'ATM withdrawal'),
(3, '2026-08-10 11:00:00', 'DEPOSIT', 25000.00, 'Business income'),
(3, '2026-08-12 13:30:00', 'WITHDRAWAL', 8000.00, 'Business expense'),
(4, '2026-08-15 10:00:00', 'DEPOSIT', 7000.00, 'Salary credit'),
(5, '2026-08-18 15:15:00', 'DEPOSIT', 20000.00, 'Business income');


-- 5. LOANS
INSERT INTO loans
(customer_id, branch_id, loan_type, loan_amount, interest_rate, loan_status, start_date, end_date)
VALUES
(1, 1, 'HOME', 500000.00, 7.50, 'ACTIVE', '2025-01-15', '2035-01-15'),
(2, 2, 'PERSONAL', 100000.00, 10.50, 'ACTIVE', '2026-02-01', '2029-02-01'),
(3, 3, 'BUSINESS', 750000.00, 8.25, 'ACTIVE', '2025-06-10', '2030-06-10'),
(4, 4, 'CAR', 300000.00, 9.00, 'ACTIVE', '2026-01-20', '2031-01-20'),
(5, 5, 'PERSONAL', 150000.00, 11.00, 'ACTIVE', '2026-03-15', '2029-03-15');


-- 6. LOAN PAYMENTS
INSERT INTO loan_payments
(loan_id, payment_date, payment_amount, payment_status)
VALUES
(1, '2026-07-01', 15000.00, 'SUCCESS'),
(1, '2026-08-01', 15000.00, 'SUCCESS'),
(2, '2026-07-05', 5000.00, 'SUCCESS'),
(2, '2026-08-05', 5000.00, 'SUCCESS'),
(3, '2026-07-10', 25000.00, 'SUCCESS'),
(3, '2026-08-10', 25000.00, 'SUCCESS'),
(4, '2026-07-15', 8000.00, 'SUCCESS'),
(5, '2026-08-15', 6000.00, 'SUCCESS');


-- 7. CARDS
INSERT INTO cards
(customer_id, account_id, card_number, card_type, card_status, issue_date, expiry_date)
VALUES
(1, 1, '4111111111111111', 'DEBIT', 'ACTIVE', '2025-01-10', '2030-01-10'),
(2, 2, '4222222222222222', 'DEBIT', 'ACTIVE', '2025-02-15', '2030-02-15'),
(3, 3, '4333333333333333', 'CREDIT', 'ACTIVE', '2025-03-20', '2030-03-20'),
(4, 4, '4444444444444444', 'DEBIT', 'ACTIVE', '2025-04-25', '2030-04-25'),
(5, 5, '4555555555555555', 'CREDIT', 'ACTIVE', '2025-05-30', '2030-05-30');


-- 8. CARD TRANSACTIONS
INSERT INTO card_transactions
(card_id, transaction_date, transaction_type, amount, merchant_name)
VALUES
(1, '2026-08-02 12:30:00', 'PURCHASE', 1500.00, 'Amazon'),
(1, '2026-08-06 18:20:00', 'PURCHASE', 800.00, 'Reliance'),
(2, '2026-08-08 11:15:00', 'PURCHASE', 2500.00, 'Flipkart'),
(2, '2026-08-11 20:00:00', 'PURCHASE', 1200.00, 'Swiggy'),
(3, '2026-08-13 14:10:00', 'PURCHASE', 5000.00, 'Apple Store'),
(4, '2026-08-16 16:30:00', 'PURCHASE', 1800.00, 'Amazon'),
(5, '2026-08-19 19:45:00', 'PURCHASE', 3500.00, 'Myntra');


-- 9. BENEFICIARIES
INSERT INTO beneficiaries
(customer_id, beneficiary_name, account_number, bank_name)
VALUES
(1, 'Ravi Kumar', 'BEN100001', 'SBI'),
(1, 'Priya Sharma', 'BEN100002', 'HDFC'),
(2, 'Arun Kumar', 'BEN100003', 'ICICI'),
(3, 'Divya Mohan', 'BEN100004', 'Axis Bank'),
(4, 'Karthik Suresh', 'BEN100005', 'SBI');


-- 10. ACCOUNT PAYMENTS
INSERT INTO account_payments
(account_id, beneficiary_id, payment_date, amount, payment_status)
VALUES
(1, 1, '2026-08-04 10:30:00', 5000.00, 'SUCCESS'),
(1, 2, '2026-08-09 15:20:00', 3000.00, 'SUCCESS'),
(2, 3, '2026-08-12 11:45:00', 7000.00, 'SUCCESS'),
(3, 4, '2026-08-14 16:00:00', 10000.00, 'SUCCESS'),
(4, 5, '2026-08-17 13:30:00', 4000.00, 'SUCCESS');


-- CHECK ALL TABLES
SELECT 'branches' AS table_name, COUNT(*) AS records FROM branches
UNION ALL
SELECT 'customers', COUNT(*) FROM customers
UNION ALL
SELECT 'accounts', COUNT(*) FROM accounts
UNION ALL
SELECT 'transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'loans', COUNT(*) FROM loans
UNION ALL
SELECT 'loan_payments', COUNT(*) FROM loan_payments
UNION ALL
SELECT 'cards', COUNT(*) FROM cards
UNION ALL
SELECT 'card_transactions', COUNT(*) FROM card_transactions
UNION ALL
SELECT 'beneficiaries', COUNT(*) FROM beneficiaries
UNION ALL
SELECT 'account_payments', COUNT(*) FROM account_payments;

SELECT * FROM branches;
SELECT * FROM customers;
SELECT * FROM accounts;
SELECT * FROM transactions;
SELECT * FROM loans;
SELECT * FROM loan_payments;
SELECT * FROM cards;
SELECT * FROM card_transactions;
SELECT * FROM beneficiaries;
SELECT * FROM account_payments;