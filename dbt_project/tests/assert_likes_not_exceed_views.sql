-- Singular test: a business rule, not a generic column test. A video can't
-- accumulate more likes than views, so any row where total_likes exceeds
-- total_views indicates a real upstream data problem (or a bug in the
-- aggregation), not just a missing/null value. dbt tests fail when this
-- query returns any rows.

select
    category_id,
    trending_date,
    total_likes,
    total_views
from {{ ref('stg_category_daily_summary') }}
where total_likes > total_views
