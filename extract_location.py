import requests

def get_lat_lot(data, state):
    '''Função para extrair do JSON a Lat, Lot e o País. Funcionada de acordo com os argumentos de cidade e estado.
    É feito assim, pois pode ter cidades com o mesmos nomes, e o 'state' define melhor a cidade espécificada.'''

    try:

        result = [
            {"state": item['state'], "lat": item['lat'], "lon": item['lon'], "country": item['country']}
            for item in data if item.get('state') == state
        ]

        return result

    except:
        print("Função: get_lat_lot ; Status: Erro, 'state' não encontrado.")



def get_location(city, state, api_key):
    '''Puxando JSON por meio da API openweathermap, retornando a Lat e Lot (Localização), de acordo com as variaveis de cidade e estado'''

    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=5&appid={api_key}"

    reponse = requests.get(url)

    if reponse.status_code == 200:
        json = reponse.json()

        result = get_lat_lot(json, state)

        return result

    else:
        print("Função: get_location ; Status: Erro, requisição da API não efetuada corretamente.")