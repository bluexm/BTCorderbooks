# Get data from the morph.io api
import requests

# We're always asking for json because it's the easiest to deal with
morph_api_url = "https://api.morph.io/bluexm/hkjobadsDS/data.json"

# Keep this key secret!
morph_api_key = "u33cEwwcB1GIsdMaFJS4"

r = requests.get(morph_api_url, params={
  'key': morph_api_key,
  'query': "select * from obooks"
})

print (r.json())