# Import pages
import extract_location_city
import extract_weather
import location_nominatim

# Global Variables

# Open Weathermap API key
api_key = "d200182684adb0b606788a22747cffd6"

# Opções de escolha (Futuramente colocar em uma interface gráfica, provávelmente Streamlit)
print('''
Digite 1 ou 2:
      
Opção 1 (digite 1):
Funciona para captar os dados somente no centro da cidade, portanto 
a latitude e longitude estarão na mesma posição (Centro).

Dados de Entrada:
- City
- State

Dados de Saída:
Dados clímaticos do centro da cidade específicada.      

----------------------------------------------------------------------
      
Opção 2 (digite 2):
Funciona para capturar os dados de acordo com o endereço oferecido.

Dados de Entrada
- Address
- City
- State
- Country

Dados de Saída:
Dados climaticos do posto mais próximo do endereço oferecido.
''')

# Escolhendo Opção de trazer os dados
option = input("Escolha a opção: ")

# Confirindo se foi digitado corretamente!
if option not in ["1", "2"]:
    print('''Erro! Você digitou um carácter ínvalido,
          só pode ser escolhido a opção 1 (digite 1) e a
          opção 2 (digite 2).''')
    exit()

if option == "1": # Variaveis de endereço (API penweathermap location)
    city = input("Digite a cidade que você está interessado: ")
    state = input("Digite o estado que você está interessado: ")

    # Capturando lat, lon e country (Primeira Opção: City e State)
    location_city = extract_location_city.get_location(city, state, api_key)

    # Tratando location
    dict_location_city = location_city[0]
    lat, lon = [dict_location_city['lat'], dict_location_city['lon']]

else:
    address = input("Digite o logradouro que você está interessado: ")
    city = input("Digite a cidade que você está interessado: ")
    state = input("Digite o estado que você está interessado: ")
    country = input("Digite o pais que você está interessado: ")
    
    # Colocando em só uma string as variaveis acima
    string_input = f"{country}, {state}, {city}, {address}"

    lat, lon = location_nominatim.get_coordinates_nominatim(string_input)


# Extraindo
weather_now = extract_weather.get_weather_now(lat, lon, api_key)

print(weather_now)
