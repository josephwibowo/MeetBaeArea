SELECT CASE WHEN MAX(loaded_until) IS NULL
            THEN '1000-01-01'
	        ELSE MAX(loaded_until)
            END
FROM prod._transactions
WHERE prod._transactions.status = 'Success';