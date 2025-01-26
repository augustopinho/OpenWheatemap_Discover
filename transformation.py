# Função para achatar o JSON
def flatten_json(data):
    '''A função recebe como entrada um JSON com uma estrutura potencialmente aninhada.'''

    # Dicionário para armazenar os dados achatados
    flat_data = {
        "coord_lon": data['coord']['lon'],
        "coord_lat": data['coord']['lat'],
        "weather_id": data['weather'][0]['id'],
        "weather_main": data['weather'][0]['main'],
        "weather_description": data['weather'][0]['description'],
        "weather_icon": data['weather'][0]['icon'],
        "base": data['base'],
        "temp": data['main']['temp'],
        "feels_like": data['main']['feels_like'],
        "temp_min": data['main']['temp_min'],
        "temp_max": data['main']['temp_max'],
        "pressure": data['main']['pressure'],
        "humidity": data['main']['humidity'],
        "sea_level": data['main'].get('sea_level'),
        "grnd_level": data['main'].get('grnd_level'),
        "visibility": data['visibility'],
        "wind_speed": data['wind']['speed'],
        "wind_deg": data['wind']['deg'],
        "clouds_all": data['clouds']['all'],
        "dt": data['dt'],
        "sys_type": data['sys']['type'],
        "sys_id": data['sys']['id'],
        "sys_country": data['sys']['country'],
        "sys_sunrise": data['sys']['sunrise'],
        "sys_sunset": data['sys']['sunset'],
        "timezone": data['timezone'],
        "id": data['id'],
        "name": data['name'],
        "cod": data['cod']
    }

    return flat_data