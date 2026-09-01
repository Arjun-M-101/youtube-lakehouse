-- Mart: business-ready table for dashboards. Joins the staging rollup with
-- human-readable category names and ranks categories within each
-- (day, region) by total views — this is the table QuickSight/whatever BI
-- tool points at.

with staged as (
    select * from {{ ref('stg_category_daily_summary') }}
),

categories as (
    select * from {{ ref('youtube_categories') }}
)

select
    staged.trending_date,
    staged.region,
    staged.category_id,
    categories.category_name,
    staged.video_count,
    staged.total_views,
    staged.total_likes,
    staged.total_comments,
    staged.total_dislikes,
    staged.avg_views_per_video,
    staged.avg_engagement_ratio,
    rank() over (
        partition by staged.trending_date, staged.region
        order by staged.total_views desc
    ) as views_rank_in_day

from staged
left join categories
    on staged.category_id = categories.category_id
