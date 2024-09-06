
insert ds_gads.w_company_score
SELECT
    complaints.company_name,
    count(CASE
      WHEN complaints.timely_response THEN 1
      ELSE CAST(NULL as INT64)
    END) AS timely_response_count,
    count(*) AS total_complaints
  FROM
    `dk4learning-433311.ds_ent.complaints` AS complaints
  GROUP BY 1;