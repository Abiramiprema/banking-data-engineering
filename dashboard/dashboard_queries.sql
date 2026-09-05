
SELECT COUNT(DISTINCT customer_id) AS active_customers
FROM Workspace.default.fact_transactions;

SELECT SUM(amount) AS total_transaction_amount
FROM Workspace.default.fact_transactions;

SELECT
    'Credit' AS transaction_category,
    SUM(credit_amount) AS total_amount
FROM Workspace.default.fact_transactions

UNION ALL

SELECT
    'Debit' AS transaction_category,
    SUM(debit_amount) AS total_amount
FROM Workspace.default.fact_transactions;

SELECT
    date,
    SUM(amount) AS transaction_amount
FROM Workspace.default.fact_transactions
GROUP BY date
ORDER BY date;

SELECT
    branch_id,
    SUM(amount) AS transaction_amount
FROM Workspace.default.fact_transactions
GROUP BY branch_id
ORDER BY transaction_amount DESC;

SELECT
    l.loan_id,
    l.loan_amount -
        COALESCE(SUM(p.payment_amount), 0) AS outstanding_amount
FROM Workspace.default.dim_loan l
LEFT JOIN Workspace.default.fact_loan_payments p
    ON l.loan_id = p.loan_id
GROUP BY
    l.loan_id,
    l.loan_amount
ORDER BY outstanding_amount DESC;
