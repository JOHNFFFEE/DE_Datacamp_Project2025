select 
Name, 
`Value` as total_population
from {{ref('population')}}
