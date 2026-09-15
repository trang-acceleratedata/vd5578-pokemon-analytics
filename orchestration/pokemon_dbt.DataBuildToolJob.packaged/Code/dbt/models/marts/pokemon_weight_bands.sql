-- Grouped through a CTE rather than an ordinal: Fabric Warehouse T-SQL has no
-- `group by 1`, and repeating the CASE in the GROUP BY is easy to get wrong.
with banded as (

    select
        case
            when weight_hg < 100 then 'light'
            when weight_hg < 500 then 'medium'
            else 'heavy'
        end as weight_band,
        base_experience
    from {{ ref('stg_pokemon') }}

)

select
    weight_band,
    count(*) as pokemon_count,
    avg(cast(base_experience as float)) as avg_base_experience
from banded
group by weight_band
