{{ config(severity='warn') }}
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('zip_states') }} z
    where z.zip_code = u.zip_code
      and z.state = u.state
)
