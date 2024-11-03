#!/usr/bin/env bash

_generate_sample() {
  if [ -z "${1}" ]; then
      local BASE_PATH="."
  else
      local BASE_PATH="${1}"
  fi

  mkdir -p "${BASE_PATH}/sample/system_trx";
  mkdir -p "${BASE_PATH}/sample/bank_trx";
  rm -rf "${BASE_PATH}"/sample/system_trx/*.csv;
  rm -rf "${BASE_PATH}"/sample/bank_trx/*.csv;

  export PGPASSWORD=oprekable;
  export PGOPTIONS="--client-min-messages=warning";

  # Generate series of fake data for 7 days back, will produce 1 trx per second
  psql -h localhost -p 15432 -U oprekable oprekable -f "${BASE_PATH}/scripts/generate_start.sql" >&/dev/null;

  # Pick only 10000 data for system_trx and save it to csv file
  psql -h localhost -p 15432 -U oprekable oprekable -c "
  COPY temp_system_trx TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');
  " | tee "${BASE_PATH}/sample/system_trx/$(date +%s).csv" >&/dev/null;

  # Pick only 10000 data for temp_bank_trx and dump csv file with BCA bank only
  psql -h localhost -p 15432 -U oprekable oprekable -c "
  COPY (
    SELECT
      uniqueidentifier,
      (amount * (CASE WHEN type = 'DEBIT' THEN -1 ELSE 1 END))::DECIMAL(12,2) AS amount,
      date
    FROM temp_bank_trx
    WHERE bank = 'BCA'
  ) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');
  " | tee "${BASE_PATH}/sample/bank_trx/bca_$(date +%s).csv" >&/dev/null;

  # Pick only 10000 data for temp_bank_trx and dump csv file with MANDIRI bank only
  psql -h localhost -p 15432 -U oprekable oprekable -c "
  COPY (
    SELECT
      uniqueidentifier,
      (amount * (CASE WHEN type = 'DEBIT' THEN -1 ELSE 1 END))::DECIMAL(12,2) AS amount,
      date
    FROM temp_bank_trx
    WHERE bank = 'MANDIRI'
  ) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');
  " | tee "${BASE_PATH}/sample/bank_trx/mandiri_$(date +%s).csv" >&/dev/null;

  # Pick only 10000 data for temp_bank_trx and dump csv file with BRI bank only
  psql -h localhost -p 15432 -U oprekable oprekable -c "
  COPY (
    SELECT
      uniqueidentifier,
      (amount * (CASE WHEN type = 'DEBIT' THEN -1 ELSE 1 END))::DECIMAL(12,2) AS amount,
      date
    FROM temp_bank_trx
    WHERE bank = 'BRI'
  ) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');
  " | tee "${BASE_PATH}/sample/bank_trx/bri_$(date +%s).csv" >&/dev/null;

  # Pick only 10000 data for temp_bank_trx and dump csv file with DANAMON bank only
  psql -h localhost -p 15432 -U oprekable oprekable -c "
  COPY (
    SELECT
      uniqueidentifier,
      (amount * (CASE WHEN type = 'DEBIT' THEN -1 ELSE 1 END))::DECIMAL(12,2) AS amount,
      date
    FROM temp_bank_trx
    WHERE bank = 'DANAMON'
  ) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');
  " | tee "${BASE_PATH}/sample/bank_trx/danamon_$(date +%s).csv" >&/dev/null;

  # Drop temporary tables
  psql -h localhost -p 15432 -U oprekable oprekable -f "${BASE_PATH}/scripts/generate_end.sql" >&/dev/null;

  tput reset;

  echo "----------------------------------------------------------------";
  echo "SAMPLE GENERATED";
  echo "----------------------------------------------------------------";
  echo "";
  echo "System Transaction CSV path\t: ${BASE_PATH}/sample/system_trx";
  echo "Bank Statement CSV path\t\t: ${BASE_PATH}/sample/bank_trx";
  echo "";
}

$*

