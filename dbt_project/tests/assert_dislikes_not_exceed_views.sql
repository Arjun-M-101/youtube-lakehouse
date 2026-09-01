-- Singular test: same business rule as assert_likes_not_exceed_views.sql,
-- for dislikes. A video can't accumulate more dislikes than views, so any
-- row where total_dislikes exceeds total_views indicates a real upstream
-- data problem, not just a missing/null value.

select
    category_id,
    trending_date,
    region,
    total_dislikes,
    total_views
from {{ ref('stg_category_daily_summary') }}
where total_dislikes > total_views
