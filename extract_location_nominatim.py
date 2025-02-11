import requests

def get_coordinates_nominatim(address, email):
    '''Pegando a Latitude e Longitude baseada no endereço (input) por meio da API openstreetmap.'''

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,  
        "format": "json",  
        "limit": 1  
    }

    headers = {
        "User-Agent": email 
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

# Exemplo de entrada:
# "Brasil, São Paulo, São Paulo, Miguel Yunes"

# Exemplo de saída:
# -23.5507, -46.6875