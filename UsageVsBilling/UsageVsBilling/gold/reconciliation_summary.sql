create or refresh materialized view telecom.gold.reconciliation_summary
as(select  business_date, 
        mismatch_type, 
        severity,
        count(*) as record_count,
        sum(revenue_impact) as total_revenue_impact,
        avg(revenue_impact) as avg_revenue_impact,
        count(distinct msisdn_normalized) as unique_customers_affected
from telecom.gold.reconciliation_detail
group by business_date, mismatch_type, severity)