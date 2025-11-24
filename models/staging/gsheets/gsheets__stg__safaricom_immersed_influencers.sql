select * 
from
{{ source('staging', 'gsheets__safaricom_immersed_influencers')}}
