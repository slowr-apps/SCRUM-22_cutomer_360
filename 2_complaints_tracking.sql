
insert into ds_gads.complaints_tracking
SELECT
    company.company_name,
    count(CASE
      WHEN DATE_DIFF(CURRENT_DATE(), complaints.date_sent_to_company, DAY) <= 30 THEN 1
      ELSE CAST(NULL as INT64)
    END) AS complaints_30_days,
    count(CASE
      WHEN DATE_DIFF(CURRENT_DATE(), complaints.date_sent_to_company, DAY) <= 90 THEN 1
      ELSE CAST(NULL as INT64)
    END) AS complaints_90_days,
    count(CASE
      WHEN DATE_DIFF(CURRENT_DATE(), complaints.date_sent_to_company, DAY) <= 180 THEN 1
      ELSE CAST(NULL as INT64)
    END) AS complaints_180_days,
    count(CASE
      WHEN DATE_DIFF(CURRENT_DATE(), complaints.date_sent_to_company, DAY) <= 365 THEN 1
      ELSE CAST(NULL as INT64)
    END) AS complaints_365_days,
    CURRENT_DATETIME() as complaints_timestamp
  FROM
    `dk4learning-433311.ds_ent.company` AS company
    left JOIN `dk4learning-433311.ds_ent.complaints` AS complaints ON company.company_name = complaints.company_name
  GROUP BY 1;
