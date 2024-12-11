# Imports
import requests


import extract_location

# Dynamic Variables
city = "São Paulo"
state = "São Paulo"

result = extract_location.get_location(city, state)

print(result)