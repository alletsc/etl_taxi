WITH races AS (
  SELECT
    r1._id,
    r1.driver AS motorista,
    r1.city,
    r1.score AS avaliacao,
    r1.billing_associatedPaymentMethod,
    r1.associatedDiscount,
    r1.event,
    FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP_SUB(r1.createdAt, INTERVAL 3 HOUR)) AS data_corrida,
    FORMAT_TIMESTAMP("%H:%M:%S", TIMESTAMP_SUB(r1.createdAt, INTERVAL 3 HOUR)) AS hora_corrida,
    (r1.billing_finalPrice_totalToPay/100) as preco_corrida_com_pedagio,
    (r1.billing_finalPrice_totalPriceToll/100) as valor_pedagio,
    (SELECT
        CASE
            WHEN (r2.billing_finalPrice_totalWithDiscount/100) > 6 THEN (r2.billing_finalPrice_totalWithDiscount/100)
            ELSE 6
        END
      FROM `rj-iplanrio.transporte_taxirio_staging.races2023-2024` r2
      WHERE r2._id = r1._id) AS preco_com_desconto_calculado,
    (r1.billing_finalPrice_totalWithDiscount/100) AS preco_com_desconto_bruto,
    (r1.billing_finalPrice_totalWithoutDiscount/100) AS preco_sem_desconto
  FROM `rj-iplanrio.transporte_taxirio_staging.races2023-2024` r1
),
cities AS (
  SELECT
    _id,
    name AS nome_cidade
  FROM rj-iplanrio.transporte_taxirio_staging.cities
),
joined_races AS (
  SELECT
    races.*,
    mp.descricao AS metodo_pagamento,
    da.descricao AS desconto_associado,
    sc.descricao AS status_corrida,
    1-((races.preco_corrida_com_pedagio - races.valor_pedagio) / races.preco_sem_desconto) AS desconto_efetivo,
    c.nome_cidade
  FROM races
  LEFT JOIN rj-iplanrio.transporte_taxirio_staging.metodos_pagamento mp ON races.billing_associatedPaymentMethod = mp._id
  LEFT JOIN rj-iplanrio.transporte_taxirio_staging.descontos_associados da ON races.associatedDiscount = da._id
  LEFT JOIN rj-iplanrio.transporte_taxirio_staging.status_corrida sc ON races.event = sc._id
  LEFT JOIN cities c ON races.city = c._id
)

SELECT
  *
FROM joined_races
WHERE status_corrida NOT IN ("Indefinido")
