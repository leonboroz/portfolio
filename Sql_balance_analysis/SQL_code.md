```sql
WITH first_payments AS ( -- Шаг 1
    SELECT user_id,
           DATE_TRUNC('day', MIN(transaction_datetime)) AS first_payment_date
    FROM skyeng_db.payments
    WHERE status_name = 'success'
          AND id_transaction IS NOT NULL
    GROUP BY 1
),

all_dates AS ( -- Шаг 2
    SELECT DISTINCT DATE_TRUNC('day', class_start_datetime) AS dt
    FROM skyeng_db.classes
    WHERE DATE_TRUNC('year', class_start_datetime) = '2016-01-01'
),

payments_by_dates AS ( -- Шаг 3
    SELECT user_id,
           DATE_TRUNC('day', transaction_datetime) AS payments_day,
           SUM(classes) AS transaction_balance_change
    FROM skyeng_db.payments
    WHERE status_name = 'success'
          AND id_transaction IS NOT NULL
    GROUP BY 1,2
),

all_dates_by_user AS ( -- Шаг 4
    SELECT user_id, dt
    FROM first_payments
    JOIN all_dates ON dt >= first_payment_date
    ORDER BY 1,2
),

classes_by_dates AS ( -- Шаг 5
    SELECT user_id,
           DATE_TRUNC('day', class_start_datetime) AS class_date,
           COUNT(id_class) * (-1) AS classes
    FROM skyeng_db.classes
    WHERE class_type != 'trial'
          AND class_status IN ('success', 'failed_by_student')
    GROUP BY 1,2
),

payments_by_dates_cumsums AS ( -- Шаг 6
    SELECT all_dates_by_user.user_id,
           dt,
           COALESCE(transaction_balance_change, 0) AS transaction_balance_change,
           SUM(COALESCE(transaction_balance_change, 0)) OVER 
               (PARTITION BY all_dates_by_user.user_id ORDER BY dt) 
               AS transaction_balance_change_cum
    FROM all_dates_by_user
    LEFT JOIN payments_by_dates 
        ON all_dates_by_user.user_id = payments_by_dates.user_id
       AND dt = payments_day
),

classes_by_dates_dates_cumsum AS ( -- Шаг 7
    SELECT all_dates_by_user.user_id,
           dt,
           COALESCE(classes, 0) AS classes,
           SUM(COALESCE(classes, 0)) OVER 
               (PARTITION BY all_dates_by_user.user_id ORDER BY dt) 
               AS classes_cs
    FROM all_dates_by_user
    LEFT JOIN classes_by_dates 
        ON all_dates_by_user.user_id = classes_by_dates.user_id
       AND all_dates_by_user.dt = classes_by_dates.class_date
),

balances AS ( -- Шаг 8
    SELECT payments_by_dates_cumsums.user_id,
           payments_by_dates_cumsums.dt,
           transaction_balance_change,
           transaction_balance_change_cum,
           classes,
           classes_cs,
           classes_cs + transaction_balance_change_cum AS balance
    FROM payments_by_dates_cumsums
    INNER JOIN classes_by_dates_dates_cumsum 
        ON payments_by_dates_cumsums.user_id = classes_by_dates_dates_cumsum.user_id
       AND payments_by_dates_cumsums.dt = classes_by_dates_dates_cumsum.dt
)

SELECT dt,
       SUM(transaction_balance_change) AS transaction_balance_change_all,
       SUM(transaction_balance_change_cum) AS transaction_balance_change_cum_all,
       SUM(classes) AS classes_all,
       SUM(classes_cs) AS classes_cs_all,
       SUM(balance) AS balance_all
FROM balances
GROUP BY dt
ORDER BY dt
LIMIT 1000;
```
