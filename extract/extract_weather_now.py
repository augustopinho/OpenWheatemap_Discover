import requests

def get_weather_now(lat, lon, api_key):
    '''A partir da Latitude e Longitude pega os dados climaticos apartir do openweathermap.'''

    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"

    response = requests.get(url)

    if response.status_code == 200:
        json = response.json()

        return json
    
    else:
        print("Função: get_weather_now ; Status: Erro, requisição da API não efetuada corretamente.")

# Exemplos de entrada:
# {'lon': -23.5507, 'lat': -23.5507} # Localização  Centro SP
# {'lon': -46.6875, 'lat': -23.6894} # Minha Localização SP


# Exemplo de saída:
# {
#     'coord': {'lon': -46.6898, 'lat': -23.6867},
#     'weather': [{'id': 803, 'main': 'Clouds', 'description': 'broken clouds', 'icon': '04n'}],
#     'base': 'stations',
#     'main': {
#         'temp': 295.68,
#         'feels_like': 296.34,
#         'temp_min': 294.1,
#         'temp_max': 295.82,
#         'pressure': 1016,
#         'humidity': 90,
#         'sea_level': 1016,
#         'grnd_level': 928
#     },
#     'visibility': 10000,
#     'wind': {'speed': 1.03, 'deg': 0},
#     'clouds': {'all': 75},
#     'dt': 1737771531,
#     'sys': {'type': 1, 'id': 8446, 'country': 'BR', 'sunrise': 1737707991, 'sunset': 1737755859},
#     'timezone': -10800,
#     'id': 3464739,
#     'name': 'Diadema',
#     'cod': 200
# }