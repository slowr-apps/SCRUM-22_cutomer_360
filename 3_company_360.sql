
insert into ds_gads.company_360
SELECT
    complaints_tracking.company_name,
    company_category.category,
    complaints_tracking.complaints_30_days,
    complaints_tracking.complaints_90_days,
    complaints_tracking.complaints_180_days,
    complaints_tracking.complaints_365_days,
    complaints_tracking.complaints_timestamp
    
  FROM
    `dk4learning-433311.ds_gads.complaints_tracking` AS complaints_tracking
    LEFT OUTER JOIN `dk4learning-433311.ds_gads.company_category` AS company_category ON complaints_tracking.company_name = company_category.company_name;