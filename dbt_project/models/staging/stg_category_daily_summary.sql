-- Staging: cast types explicitly and rename for consistency. Deliberately
-- thin — the real transformation work already happened in the Glue jobs
-- (that's what transform_logic.py's unit tests cover). dbt's job here is
-- validation of what already landed in the warehouse, not re-doing the ETL.

select
    category_id::integer                as category_id,
    trending_date::date                 as trending_date,
    region::varchar(2)                  as region,
    video_count::integer                as video_count,
    total_views::bigint                 as total_views,
    total_likes::bigint                 as total_likes,
    total_comments::bigint              as total_comments,
    total_dislikes::bigint              as total_dislikes,
    avg_views_per_video::decimal(18, 2) as avg_views_per_video,
    avg_engagement_ratio::decimal(9, 6) as avg_engagement_ratio

from {{ source('gold', 'category_daily_summary') }}
