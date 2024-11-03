DROP TABLE IF EXISTS temp_generated_trx;
CREATE TABLE temp_generated_trx AS
WITH main_data AS (
    SELECT
        '{"BCA", "MANDIRI", "BRI", "BNI", "DANAMON"}'::TEXT[] AS bank

)
, base_data AS (
    SELECT
        gen_random_uuid()                                                   AS trxID
         , (floor(random() * 1000) * 1000)::NUMERIC(12,2)                   AS amount
         , CASE
               WHEN round(random()) = 1 THEN 'DEBIT'
               ELSE 'CREDIT'
        END                                                                 AS type
         , md.bank[1 + floor((random() * array_length(md.bank, 1)))::int]   AS bank
         , t                                                                AS transactionTime
    FROM main_data AS md,
         generate_series(
                 NOW() AT TIME ZONE 'Asia/Jakarta' - INTERVAL '7 DAYS',
                 NOW() AT TIME ZONE 'Asia/Jakarta',
                 INTERVAL '1 SECONDS'
         ) AS t
)
SELECT
    *
FROM base_data
WHERE amount > 0;

DROP TABLE IF EXISTS temp_system_trx;
CREATE TABLE temp_system_trx AS
SELECT
    tgt.trxID
     , tgt.amount
     , tgt.type
     , tgt.transactionTime
FROM temp_generated_trx tgt
ORDER BY RANDOM() LIMIT 10000;

DROP TABLE IF EXISTS temp_bank_trx;
CREATE TABLE temp_bank_trx AS
SELECT
    LOWER(tgt.bank) || '-' || gen_random_uuid()             AS uniqueIdentifier
     , tgt.amount
     , tgt.type
     , tgt.bank
     , tgt.transactionTime::DATE                            AS date
FROM temp_generated_trx tgt
ORDER BY RANDOM() LIMIT 10000;