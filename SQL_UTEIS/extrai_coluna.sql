SELECT
  column_name
FROM
  `rj-iplanrio.transporte_taxirio_staging.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'paymentmethods'
ORDER BY
  ordinal_position;
