select
    id as pokemon_id,
    name as pokemon_name,
    cast(height as int) as height_dm,
    cast(weight as int) as weight_hg,
    cast(base_experience as int) as base_experience
from {{ source('bronze', 'pokemon') }}
