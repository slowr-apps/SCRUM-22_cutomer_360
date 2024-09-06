
insert into ds_gads.company_category
SELECT
    intermediate_table.company_name,
    CASE
      WHEN intermediate_table.timely_response_count / intermediate_table.total_complaints >= 0.8 THEN 'good'
      WHEN intermediate_table.timely_response_count / intermediate_table.total_complaints >= 0.5 THEN 'average'
      ELSE 'poor'
    END AS category,
    current_timestamp()
  FROM
    ds_gads.w_company_score as intermediate_table;