import requests

def get_weather_now(lat, lon, api_key):

    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"

    response = requests.get(url)

    if response.status_code == 200:
        json = response.json()

        return json
    
    else:
        print("Função: get_weather_now ; Status: Erro, requisição da API não efetuada corretamente.")

# {'lon': -23.5507, 'lat': -23.5507} # Localização  Centro SP
# {'lon': -46.6875, 'lat': -23.6894} # Minha Localização SP