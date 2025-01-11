import requests

def get_coordinates_nominatim(address):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,  
        "format": "json",  
        "limit": 1  
    }

    headers = {
        "User-Agent": "augustopinho@outlook.com"  
    }

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()

        if data:
            latitude = data[0]["lat"]
            longitude = data[0]["lon"]
            return latitude, longitude
        
        else:
            print('Localidade não encontrada!')

    else:
        raise Exception(f"Erro na requisição: {response.status_code}")

endereco = "São Paulo, Brasil"
latitude, longitude = get_coordinates_nominatim(endereco)
print(latitude, longitude)