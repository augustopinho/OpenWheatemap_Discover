# Import Módolos
import location_nominatim
import extract_weather_now
import transformation

# Definindo variaveis
api_key_open_weathermap = "d200182684adb0b606788a22747cffd6"
email_nominatim = "augustopinho@outlook.com"



# # Capturando informações do localização
# address = input("Digite o logradouro que você está interessado: ")
# city = input("Digite a cidade que você está interessado: ")
# state = input("Digite o estado que você está interessado: ")
# country = input("Digite o pais que você está interessado: ")

# # Colocando em só uma string as variaveis acima.
# string_input = f"{country}, {state}, {city}, {address}"

string_input = "Brasil, São Paulo, Maua, Rua Ozerias Rodrigues de Oliveira"

# Capturando lat e lon, atraves do endereço.
lat, lon = location_nominatim.get_coordinates_nominatim(string_input, email_nominatim)

# Extraindo clima agora.
weather_now_data = extract_weather_now.get_weather_now(lat, lon, api_key_open_weathermap)

# Achatando, transformando e printando os dados extraidos
df = transformation.start_session_spark_and_transformations(weather_now_data)


# # Configurar cliente S3
# s3 = boto3.client(
#     's3',
#     endpoint_url='http://<seu-endereco>:9000',
#     aws_access_key_id='MINIO_ROOT_USER',
#     aws_secret_access_key='MINIO_ROOT_PASSWORD'
# )