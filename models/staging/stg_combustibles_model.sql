{{ config(materialized='table') }}

SELECT
    indice_tiempo,
    region,
    producto,
    precio
FROM {{ ref('stg_combustibles') }}
