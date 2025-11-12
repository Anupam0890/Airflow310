SELECT *
FROM {{ ref('example_seed') }}
WHERE id IS NOT NULL