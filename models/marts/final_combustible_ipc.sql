{{ config(materialized='table') }}

WITH ipc_expanded AS (
    SELECT indice_tiempo, 'pampeana' AS region, ipc_ng_pampeana AS ipc_region FROM {{ ref('stg_ipc_model') }}
    UNION ALL
    SELECT indice_tiempo, 'nea', ipc_ng_nea FROM {{ ref('stg_ipc_model') }}
    UNION ALL
    SELECT indice_tiempo, 'noa', ipc_ng_noa FROM {{ ref('stg_ipc_model') }}
    UNION ALL
    SELECT indice_tiempo, 'cuyo', ipc_ng_cuyo FROM {{ ref('stg_ipc_model') }}
    UNION ALL
    SELECT indice_tiempo, 'patagonia', ipc_ng_patagonia FROM {{ ref('stg_ipc_model') }}
)

SELECT
    c.indice_tiempo,
    c.region,
    c.producto,
    c.precio,
    i.ipc_region
FROM {{ ref('stg_combustibles_model') }} c
JOIN ipc_expanded i
    ON c.indice_tiempo = i.indice_tiempo AND c.region = i.region
ORDER BY c.indice_tiempo
