# Import Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Import Módolos
import location_nominatim
import extract_weather_now
import transformation

# Open Weathermap API key
api_key = "d200182684adb0b606788a22747cffd6"



# # Capturando informações do localização
# address = input("Digite o logradouro que você está interessado: ")
# city = input("Digite a cidade que você está interessado: ")
# state = input("Digite o estado que você está interessado: ")
# country = input("Digite o pais que você está interessado: ")

# # Colocando em só uma string as variaveis acima.
# string_input = f"{country}, {state}, {city}, {address}"
string_input = "Brasil, São Paulo, São Paulo, Miguel Yunes"

# Capturando lat e lon, atraves do endereço.
lat, lon = location_nominatim.get_coordinates_nominatim(string_input)

# Extraindo clima agora.
weather_now_data = extract_weather_now.get_weather_now(lat, lon, api_key)



# Iniciar a sessão Spark
# Iniciar a sessão Spark: SparkSession.builder
# Nome da sessão Spark: appName("ProcessWeatherJSON") 
# Garante a sessão Spark seja iniciada sem váriaveis de sessão: .getOrCreate()

spark = SparkSession.builder \
        .appName("ProcessWeatherJSON") \
        .getOrCreate() 



# Achatar o JSON
flattened_data = transformation.flatten_json(weather_now_data)

# Criar o DataFrame
df = spark.createDataFrame([flattened_data])

# Mostrar o esquema e os dados
df.show(truncate=False, vertical=True)