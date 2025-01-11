# Import pages
import extract_location_city
import extract_weather
import location_nominatim

# Open Weathermap API key
api_key = "d200182684adb0b606788a22747cffd6"

address = input("Digite o logradouro que você está interessado: ")
city = input("Digite a cidade que você está interessado: ")
state = input("Digite o estado que você está interessado: ")
country = input("Digite o pais que você está interessado: ")

# Colocando em só uma string as variaveis acima.
string_input = f"{country}, {state}, {city}, {address}"

# Capturando lat e lon, atraves do endereço.
lat, lon = location_nominatim.get_coordinates_nominatim(string_input)

# Extraindo clima agora.
weather_now = extract_weather.get_weather_now(lat, lon, api_key)

print(weather_now)
