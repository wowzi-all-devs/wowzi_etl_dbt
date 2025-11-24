select
 * 
from
{{ source('staging', 'gsheets__safaricom_poc_influencers')}}