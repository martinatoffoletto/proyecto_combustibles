{{ config(materialized='table') }}

SELECT
    indice_tiempo,
    ipc_ng_pampeana,
    ipc_ng_nea,
    ipc_ng_noa,
    ipc_ng_cuyo,
    ipc_ng_patagonia
FROM {{ ref('stg_ipc') }}
