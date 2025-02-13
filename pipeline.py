# Import Módolos
import extract_location_nominatim
import extract_weather_now
import transformation
import driver_postgres

# Definindo variaveis (APIs e Banco de dados)
api_key_open_weathermap = "d200182684adb0b606788a22747cffd6"
email_nominatim = "augustopinho@outlook.com"



# INPUTS #

# Inputando informações de localização
# print("--------------------------INPUT DOS DADOS------------------------------")
# address = input("Digite o logradouro que você está interessado: ")
# city = input("Digite a cidade que você está interessado: ")
# state = input("Digite o estado que você está interessado: ")
# country = input("Digite o pais que você está interessado: ")

# # Colocando em só uma string as variaveis acima.
# string_input = f"{country}, {state}, {city}, {address}"

string_input = "Brasil, São Paulo, São Paulo, Rua Miguel Yunes"



# CRIAÇÃO DE TABELAS NOS SGBDs #

# Verifica/Cria a tabela no postgres
driver_postgres.create_table_psycopg2()



# EXTRAÇÕES POR MEIO DAS APIs #

# Capturando lat e lon, atraves do endereço
lat, lon = extract_location_nominatim.get_coordinates_nominatim(string_input, email_nominatim)

# Extraindo clima agora
weather_now_data = extract_weather_now.get_weather_now(lat, lon, api_key_open_weathermap)



# TRANSFORMAÇÕES PELO SPARK #

# Criando a SparkSession
spark = transformation.create_session_spark()

# Achatando o JSON e criando o DataFrame Spark
df = transformation.flatten_json_and_create_df(weather_now_data, spark)

# Altera as colunas de temperatura e dropa as colunas de código.
df = transformation.alters_columns(df)