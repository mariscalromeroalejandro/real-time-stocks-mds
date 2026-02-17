SELECT 
symbol, 
current_price,
change_amount,
change_percent
FROM (
    SELECT *,
        row_number() over (partition by symbol order by fetched_at desc) as row_num
    FROM {{ ref('silver_clean_stock_quotes') }}
) t
WHERE row_num = 1 