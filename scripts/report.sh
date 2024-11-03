#!/usr/bin/env bash

_report() {
  while getopts ":s:b:f:t:" opt; do
    case ${opt} in
      s ) system_trx_path=$OPTARG;;
      b ) bank_trx_path=$OPTARG;;
      f ) from=$OPTARG;;
      t ) to=$OPTARG;;
      \? ) echo "Usage: cmd [-w] [-f] [-t]";;
    esac
  done

  if [ -z "$system_trx_path" ] || [ -z "$system_trx_path" ]; then
    echo "System TRX Path required"
    exit 1
  fi

  if [ -z "$bank_trx_path" ] || [ -z "$bank_trx_path" ]; then
    echo "Bank TRX Path required"
    exit 1
  fi

  if [ -z "$from" ] || [ -z "$from" ]; then
    echo "From Date required (YYYY/MM/DD)"
    exit 1
  fi

  if [ -z "$to" ] || [ -z "$to" ]; then
    echo "To Date required (YYYY/MM/DD)"
    exit 1
  fi

  local SYSTEM_TRX_CSV="${system_trx_path}"/*.csv;
  local BANK_TRX_CSV="${bank_trx_path}"/*.csv;

  export PGPASSWORD=oprekable;
  export PGOPTIONS="--client-min-messages=warning";

  local reconcile_file=""

  # Import system transaction
  for file in ${SYSTEM_TRX_CSV}; do
    reconcile_file="$(basename ${file})"

    cat $file | sed -e 's#$#,'"$(basename ${file})"'#g' | psql -h localhost -p 15432 -U oprekable oprekable -c "
      CREATE TABLE IF NOT EXISTS import_system_trx (
        trxid TEXT,
        amount NUMERIC(12,2),
        type TEXT,
        transactiontime TIMESTAMP WITHOUT TIME ZONE,
        filename         TEXT
      );

      COPY import_system_trx(trxid, amount, type, transactiontime, filename) FROM STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');

      CREATE TABLE IF NOT EXISTS system_trx (
        trxid TEXT PRIMARY KEY,
        amount NUMERIC(12,2),
        type TEXT,
        transactiontime TIMESTAMP WITHOUT TIME ZONE,
        filename         TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_system_trx ON system_trx (transactiontime, type, amount, filename);

      INSERT INTO system_trx(
        trxid,
        amount,
        type,
        transactiontime,
        filename
      )
      SELECT
        trxid,
        amount,
        type,
        transactiontime,
        filename
      FROM import_system_trx
      ON CONFLICT(trxid) DO NOTHING;

      DROP TABLE IF EXISTS import_system_trx;
    " >&/dev/null;

  done;

  # Import bank transaction (multiple files)
  for file in ${BANK_TRX_CSV}; do

    cat $file | sed -e 's#$#,'"$(basename ${file})"'#g' | sed -e 's#$#,'"${reconcile_file}"'#g' | psql -h localhost -p 15432 -U oprekable oprekable -c "
      CREATE TABLE IF NOT EXISTS import_bank_trx
      (
          uniqueidentifier TEXT,
          amount           NUMERIC(12, 2),
          date              DATE,
          filename         TEXT,
          reconcilefile     TEXT
      );

      COPY import_bank_trx(uniqueidentifier, amount, date, filename, reconcilefile) FROM STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');

      CREATE TABLE IF NOT EXISTS bank_trx (
        uniqueidentifier  TEXT PRIMARY KEY,
        amount            NUMERIC(12,2),
        type              TEXT,
        bank              TEXT,
        date              DATE,
        reconcilefile     TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_bank_trx ON bank_trx (date, type, amount, reconcilefile);

      INSERT INTO bank_trx(
        uniqueidentifier,
        amount,
        type,
        date,
        bank,
        reconcilefile
      )
      SELECT
          uniqueidentifier
          , ABS(amount)::NUMERIC(12,2)    AS amount
          , CASE
              WHEN amount < 0 THEN 'DEBIT'
              ELSE 'CREDIT'
          END                                 AS type
          , date
          , UPPER(SPLIT_PART(filename, '_', 1))  AS bank
          , reconcilefile
      FROM import_bank_trx
      ON CONFLICT(uniqueidentifier) DO NOTHING;

      DROP TABLE IF EXISTS import_bank_trx;
    " >&/dev/null;

  done;

  psql -h localhost -p 15432 -U oprekable oprekable << EOF >&/dev/null
CREATE TABLE IF NOT EXISTS reconciliation_map (
    trxid TEXT PRIMARY KEY,
    uniqueidentifier TEXT
);

CREATE INDEX IF NOT EXISTS idx_reconciliation_map ON reconciliation_map (trxid, uniqueidentifier);
EOF

  tput reset;

  echo "----------------------------------------------------------------";
  echo "RECONCILIATION SUMMARY";
  echo "----------------------------------------------------------------";
  echo "";

  # Calculate to matching System transactions vs Bank Statements and store the mapping to reconciliation_map table
  psql -x -h localhost -p 15432 -U oprekable oprekable << EOF
WITH main_data AS (
    SELECT
        '${reconcile_file}'::TEXT     AS filename
        , '${from}'::DATE             AS fromdate
        , '${to}'::DATE               AS todate
)
, base_search AS (
    SELECT
        st.trxid
        , bt.uniqueidentifier
        , st.amount
        , st.type
        , bt.bank
        , bt.date
        , st.transactiontime
    FROM main_data md, system_trx st
    INNER JOIN bank_trx bt ON
        bt.date = st.transactiontime::DATE
            AND bt.type = st.type
            AND bt.amount = st.amount
            AND bt.reconcilefile = st.filename
    WHERE
        st.filename = md.filename
        AND st.transactiontime::DATE <= md.todate AND st.transactiontime::DATE >= md.fromdate
)
, with_counter AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY bs.date, bs.amount, bs.type, bs.trxID ORDER BY bs.uniqueIdentifier) AS r_system
         , ROW_NUMBER() OVER (PARTITION BY bs.date, bs.amount, bs.type, bs.uniqueIdentifier ORDER BY bs.trxID) AS r_bank
         , bs.*
    FROM base_search bs
)
, matched_trx AS (
    SELECT
        wc.*
    FROM with_counter wc
    WHERE wc.r_system = wc.r_bank
    ORDER BY date, amount, trxID, uniqueIdentifier, transactiontime
)
, insert_reconciliation_map AS (
    INSERT INTO reconciliation_map(
        trxid,
        uniqueidentifier
    )
    SELECT
        trxid,
        uniqueidentifier
    FROM matched_trx
    ON CONFLICT(trxid)
        DO UPDATE
        SET uniqueidentifier = excluded.uniqueidentifier
)
, summary_data AS (
    SELECT
        (SELECT COUNT(*) FROM system_trx WHERE filename = md.filename AND transactiontime::DATE <= md.todate AND transactiontime::DATE >= md.fromdate)  AS "Total number of transactions processed"
         , (SELECT COUNT(*) FROM matched_trx)  AS "Total number of matched transactions"
         , (SELECT SUM(amount) FROM system_trx WHERE filename = md.filename AND transactiontime::DATE <= md.todate AND transactiontime::DATE >= md.fromdate)  AS "Sum amount all transactions"
         , (SELECT SUM(amount) FROM matched_trx)  AS "Sum amount matched transactions"
    FROM main_data md
)
SELECT
    sd."Total number of transactions processed"
    , sd."Total number of matched transactions"
    , (sd."Total number of transactions processed" - sd."Total number of matched transactions")         AS "Total number of unmatched transactions"
    , sd."Sum amount all transactions"::NUMERIC(12,2)
    , sd."Sum amount matched transactions"::NUMERIC(12,2)
    , (sd."Sum amount all transactions" - sd."Sum amount matched transactions")::NUMERIC(12,2)          AS "Total discrepancies"
FROM summary_data sd;
EOF

  local report_missing_system_trx=$(pwd)/reports
  mkdir -p "${report_missing_system_trx}"
  report_missing_system_trx="${report_missing_system_trx}"/missing_system_trx.csv

  local report_missing_bank_trx=$(pwd)/reports
  mkdir -p "${report_missing_bank_trx}"

  local report_missing_bank_trx_bca="${report_missing_bank_trx}"/missing_bank_trx_bca.csv
  local report_missing_bank_trx_mandiri="${report_missing_bank_trx}"/missing_bank_trx_mandiri.csv
  local report_missing_bank_trx_bri="${report_missing_bank_trx}"/missing_bank_trx_bri.csv
  local report_missing_bank_trx_danamon="${report_missing_bank_trx}"/missing_bank_trx_danamon.csv

  echo "-------------------------------------------------------------------------------------------------------------";
  echo "Missing System Transaction Details\t\t: ${report_missing_system_trx}";
  echo "Missing Bank Statement Details - BCA\t\t: ${report_missing_bank_trx_bca}";
  echo "Missing Bank Statement Details - BRI\t\t: ${report_missing_bank_trx_bri}";
  echo "Missing Bank Statement Details - MANDIRI\t: ${report_missing_bank_trx_mandiri}";
  echo "Missing Bank Statement Details - DANAMON\t: ${report_missing_bank_trx_danamon}";
  echo "-------------------------------------------------------------------------------------------------------------";
  echo "";
  echo "";

  # Generate report for Missing System Transaction Details
  psql -h localhost -p 15432 -U oprekable oprekable -c "
    WITH main_data AS (
      SELECT
          '${reconcile_file}'::TEXT     AS filename
          , '${from}'::DATE             AS fromdate
          , '${to}'::DATE               AS todate
    )
    , base_search_system_trx AS (
        SELECT
            st.trxid
            , st.amount
            , st.type
            , st.transactiontime
            , st.filename
        FROM main_data md, system_trx st
        WHERE
            st.filename = md.filename
            AND st.transactiontime::DATE <= md.todate AND st.transactiontime::DATE >= md.fromdate
    )
    SELECT
        bsst.trxid
        , bsst.amount
        , bsst.type
        , bsst.transactiontime
        , bsst.filename
    FROM base_search_system_trx bsst
    LEFT JOIN reconciliation_map rm ON
        rm.trxid = bsst.trxid
    WHERE rm.trxid IS NULL
    ORDER BY bsst.transactiontime, bsst.amount;
  " | tee "${report_missing_system_trx}" >&/dev/null;

  # Generate report for Missing Bank Statement Details - BCA
  psql -h localhost -p 15432 -U oprekable oprekable -c "
    WITH main_data AS (
      SELECT
          '${reconcile_file}'::TEXT     AS filename
          , '${from}'::DATE             AS fromdate
          , '${to}'::DATE               AS todate
          , 'BCA'::TEXT                 AS bank
    )
  , base_search_bank_trx AS (
      SELECT
          bt.uniqueidentifier
          , bt.amount
          , bt.type
          , bt.bank
          , bt.date
          , bt.reconcilefile
      FROM main_data md, bank_trx bt
      WHERE
          bt.reconcilefile = md.filename
          AND bt.bank = md.bank
          AND bt.date <= md.todate AND bt.date >= md.fromdate
  )
  SELECT
      bsbt.uniqueidentifier
      , bsbt.amount
      , bsbt.type
      , bsbt.bank
      , bsbt.date
      , bsbt.reconcilefile
  FROM base_search_bank_trx bsbt
  LEFT JOIN reconciliation_map rm ON
      rm.uniqueidentifier = bsbt.uniqueidentifier
  WHERE rm.trxid IS NULL
  ORDER BY bsbt.date, bsbt.amount;
  " | tee "${report_missing_bank_trx_bca}" >&/dev/null;

  # Generate report for Missing Bank Statement Details - BRI
  psql -h localhost -p 15432 -U oprekable oprekable -c "
    WITH main_data AS (
      SELECT
          '${reconcile_file}'::TEXT     AS filename
          , '${from}'::DATE             AS fromdate
          , '${to}'::DATE               AS todate
          , 'BRI'::TEXT                 AS bank
    )
  , base_search_bank_trx AS (
      SELECT
          bt.uniqueidentifier
          , bt.amount
          , bt.type
          , bt.bank
          , bt.date
          , bt.reconcilefile
      FROM main_data md, bank_trx bt
      WHERE
          bt.reconcilefile = md.filename
          AND bt.bank = md.bank
          AND bt.date <= md.todate AND bt.date >= md.fromdate
  )
  SELECT
      bsbt.uniqueidentifier
      , bsbt.amount
      , bsbt.type
      , bsbt.bank
      , bsbt.date
      , bsbt.reconcilefile
  FROM base_search_bank_trx bsbt
  LEFT JOIN reconciliation_map rm ON
      rm.uniqueidentifier = bsbt.uniqueidentifier
  WHERE rm.trxid IS NULL
  ORDER BY bsbt.date, bsbt.amount;
  " | tee "${report_missing_bank_trx_bri}" >&/dev/null;

  # Generate report for Missing Bank Statement Details - DANAMON
  psql -h localhost -p 15432 -U oprekable oprekable -c "
    WITH main_data AS (
      SELECT
          '${reconcile_file}'::TEXT     AS filename
          , '${from}'::DATE             AS fromdate
          , '${to}'::DATE               AS todate
          , 'DANAMON'::TEXT             AS bank
    )
  , base_search_bank_trx AS (
      SELECT
          bt.uniqueidentifier
          , bt.amount
          , bt.type
          , bt.bank
          , bt.date
          , bt.reconcilefile
      FROM main_data md, bank_trx bt
      WHERE
          bt.reconcilefile = md.filename
          AND bt.bank = md.bank
          AND bt.date <= md.todate AND bt.date >= md.fromdate
  )
  SELECT
      bsbt.uniqueidentifier
      , bsbt.amount
      , bsbt.type
      , bsbt.bank
      , bsbt.date
      , bsbt.reconcilefile
  FROM base_search_bank_trx bsbt
  LEFT JOIN reconciliation_map rm ON
      rm.uniqueidentifier = bsbt.uniqueidentifier
  WHERE rm.trxid IS NULL
  ORDER BY bsbt.date, bsbt.amount;
  " | tee "${report_missing_bank_trx_danamon}" >&/dev/null;

  # Generate report for Missing Bank Statement Details - MANDIRI
  psql -h localhost -p 15432 -U oprekable oprekable -c "
    WITH main_data AS (
      SELECT
          '${reconcile_file}'::TEXT     AS filename
          , '${from}'::DATE             AS fromdate
          , '${to}'::DATE               AS todate
          , 'MANDIRI'::TEXT             AS bank
    )
  , base_search_bank_trx AS (
      SELECT
          bt.uniqueidentifier
          , bt.amount
          , bt.type
          , bt.bank
          , bt.date
          , bt.reconcilefile
      FROM main_data md, bank_trx bt
      WHERE
          bt.reconcilefile = md.filename
          AND bt.bank = md.bank
          AND bt.date <= md.todate AND bt.date >= md.fromdate
  )
  SELECT
      bsbt.uniqueidentifier
      , bsbt.amount
      , bsbt.type
      , bsbt.bank
      , bsbt.date
      , bsbt.reconcilefile
  FROM base_search_bank_trx bsbt
  LEFT JOIN reconciliation_map rm ON
      rm.uniqueidentifier = bsbt.uniqueidentifier
  WHERE rm.trxid IS NULL
  ORDER BY bsbt.date, bsbt.amount;
  " | tee "${report_missing_bank_trx_mandiri}" >&/dev/null;

}

$*

