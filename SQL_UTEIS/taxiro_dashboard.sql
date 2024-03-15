WITH races AS (
  SELECT
  _id,
  city,
  driver as motorista,
  score as avaliacao,
  CASE
      WHEN billing_associatedPaymentMethod = "5ef0b65815f5d3cae84f9862" THEN "Mercado Pago"
      WHEN billing_associatedPaymentMethod = "5a26edc4b3e31e6203d0ba97" THEN "Cartão de débito"
      WHEN billing_associatedPaymentMethod = "5a26ecd5b3e31e6203d0ac11" THEN "Dinheiro"
      WHEN billing_associatedPaymentMethod = "617ab4e7a89e0d3f6649f925" THEN "PIX"
      WHEN billing_associatedPaymentMethod = "5a26ed84b3e31e6203d0b6d9" THEN "Cartão de crédito"
      WHEN billing_associatedPaymentMethod = "62bf49abb7fa45275fc8bd86" THEN "Arariboia"
      WHEN billing_associatedPaymentMethod = "5aeb5abb019fbeb1a71d1cc1" THEN "Corporativo Prefeitura RJ"
      WHEN billing_associatedPaymentMethod = "5aeb5abb019fbeb1a71d1cc0" THEN "Cartão de Crédito"
      WHEN billing_associatedPaymentMethod = "5a99619d331ab758dc58e444" THEN "Corporativo"
      ELSE "Indefinido"
    END AS metodo_pagamento,
   CASE
      WHEN associatedDiscount = "59c424f09c4b6aa933bbed94" THEN "10%"
      WHEN associatedDiscount = "59c424f09c4b6aa933bbed95" THEN "20%"
      WHEN associatedDiscount = "59c424f09c4b6aa933bbed96" THEN "30%"
      WHEN associatedDiscount = "59c424f09c4b6aa933bbed97" THEN "40%"
      WHEN associatedDiscount = "59c424f09c4b6aa933bbed93" THEN "Tarifa Normal"
      ELSE "Indefinido"
    END AS desconto_associado,
    CASE
      WHEN billing_associatedPaymentMethod = "5ef0b65815f5d3cae84f9862" THEN "QR CODE"
      WHEN billing_associatedPaymentMethod = "5a26edc4b3e31e6203d0ba97" THEN "Pagamento no táxi"
      WHEN billing_associatedPaymentMethod = "5a26ecd5b3e31e6203d0ac11" THEN "Pagamento no táxi"
      WHEN billing_associatedPaymentMethod = "617ab4e7a89e0d3f6649f925" THEN "Pagamento no táxi"
      WHEN billing_associatedPaymentMethod = "5a26ed84b3e31e6203d0b6d9" THEN "Pagamento no táxi"
      WHEN billing_associatedPaymentMethod = "62bf49abb7fa45275fc8bd86" THEN "Pagamento no táxi"
      WHEN billing_associatedPaymentMethod = "5aeb5abb019fbeb1a71d1cc1" THEN "Pagamento no aplicativo"
      WHEN billing_associatedPaymentMethod = "5aeb5abb019fbeb1a71d1cc0" THEN "Pagamento no aplicativo"
      WHEN billing_associatedPaymentMethod = "5a99619d331ab758dc58e444" THEN "Pagamento no aplicativo"
      ELSE "Indefinido"
    END AS tipo_pagamento,
    CASE
      WHEN event IN ("5b2b9080b5595a3a2fbd8b31","5b2b9080b5595a3a2fbd8b3c") THEN "Finalizada"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b48","630cd45decfde68291af4115","630cfb48ecfde682919990d9","5b50e7365fef9b0ac0a1bd79","630cfb33ecfde68291990f07","5b2b9080b5595a3a2fbd8b47","5b2b9080b5595a3a2fbd8b46","5b2b9080b5595a3a2fbd8b50","630cd44becfde68291aec097") THEN "Retificadora"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b45","5b2b9080b5595a3a2fbd8b4f","5b2b9080b5595a3a2fbd8b38","5b2b9080b5595a3a2fbd8b4e") THEN "Estornada"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b32","630cd433ecfde68291ae20a3", "630cfb23ecfde6829198a9f0") THEN "Cancelada pelo Taxista"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b33", "5b2b9080b5595a3a2fbd8b4b") THEN "Passageiro Cancela Antes começar"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b34","630cd41decfde68291ad916a", "630cfb13ecfde682919848d6") THEN "Passageiro Cancela Depois começar"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b44","5b2b9080b5595a3a2fbd8b4c", "5b2b9080b5595a3a2fbd8b4d", "5b2b9080b5595a3a2fbd8b43", "5b2b9080b5595a3a2fbd8b42") THEN "Faturada"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b3d", "5b2b9080b5595a3a2fbd8b3f", "5b2b9080b5595a3a2fbd8b40", "5b2b9080b5595a3a2fbd8b41", "5c533bfa8ab8ac610018a360", "5b2b9080b5595a3a2fbd8b3e", "5c533be78ab8ac610018a321", "5c533bf18ab8ac610018a336" ) THEN "Inválida"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b3a", "64f0dcddf0e55d564b3a19cd") THEN "Não atendida"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b3b") THEN "Aguardando Taxista"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b35") THEN "Em Andamento"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b39") THEN "Em Aberto"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b37", "5b2b9080b5595a3a2fbd8b36") THEN "Em análise"
      WHEN event IN ("5b4d4061add99882d9042c1a") THEN "Erro pagamento"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b49") THEN "Pagamento não autorizado"
      WHEN event IN ("5b2b9080b5595a3a2fbd8b4a") THEN "Pré autorização sem retorno"
      ELSE "Indefinido"
    END AS status_corrida,
  FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP_SUB(createdAt, INTERVAL 3 HOUR)) AS data_corrida,
  FORMAT_TIMESTAMP("%H:%M:%S", TIMESTAMP_SUB(createdAt, INTERVAL 3 HOUR)) AS hora_corrida,
  (billing_finalPrice_totalToPay/100) as preco_corrida_com_pedagio,
  (billing_finalPrice_totalPriceToll/100) as valor_pedagio,
  (SELECT
      CASE
          WHEN (r2.billing_finalPrice_totalWithDiscount/100)  > 6 THEN (r2.billing_finalPrice_totalWithDiscount/100)
          ELSE 6
      END
    FROM `rj-iplanrio.transporte_taxirio_staging.races2023-2024` r2
    WHERE r2._id = r1._id) AS preco_com_desconto_calculado,
    (billing_finalPrice_totalWithDiscount/100) AS preco_com_desconto_bruto,
    (billing_finalPrice_totalWithoutDiscount/100) AS preco_sem_desconto,
  event
  FROM `rj-iplanrio.transporte_taxirio_staging.races2023-2024` r1
),
cities AS (
  SELECT
  _id,
  name AS nome_cidade
  FROM rj-iplanrio.transporte_taxirio_staging.cities
)

SELECT
  races._id,
  races.motorista,
  races.metodo_pagamento,
  races.desconto_associado,
  races.tipo_pagamento,
  races.status_corrida,
  races.data_corrida,
  races.hora_corrida,
  races.avaliacao,
  races.preco_corrida_com_pedagio,
  races.valor_pedagio,
  races.preco_com_desconto_bruto,
  races.preco_com_desconto_calculado,
  1-((races.preco_corrida_com_pedagio - races.valor_pedagio)  / races.preco_sem_desconto) AS desconto_efetivo,
  races.preco_sem_desconto,
  cities.nome_cidade
FROM races
LEFT JOIN cities ON races.city = cities._id
WHERE races.status_corrida NOT IN ("Indefinido")
