
  create view "airflow"."public"."example_model__dbt_tmp"
    
    
  as (
    SELECT *
FROM "airflow"."public"."example_seed"
WHERE id IS NOT NULL
  );