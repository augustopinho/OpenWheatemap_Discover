import extract_location
import extract_weather

api_key = "d200182684adb0b606788a22747cffd6"
city = "São Paulo"
state = "São Paulo"

# Capturando lat, lon e country
location = extract_location.get_location(city, state, api_key)

# Tratando location
dict_location = location[0]
lat, lon = [dict_location['lat'], dict_location['lon']]

# Extraindo 
weather_now = extract_weather.get_weather_now(lat, lon, api_key)

print(weather_now)