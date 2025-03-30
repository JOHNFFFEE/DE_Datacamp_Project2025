select 
Name, 
GEC,
A3 as alpha_3_code,
STANAG as iso_code,
A2 as alpha_2_code,
NUM
from {{ref('country_lookup')}}