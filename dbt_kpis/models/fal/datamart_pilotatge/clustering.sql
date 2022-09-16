
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 82727d321287f0baa8d488dde78152b2

Script dependencies:

{{ ref('widetable_socies') }}

*/

SELECT * FROM {{ this }}
