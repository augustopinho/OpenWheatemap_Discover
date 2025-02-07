# Import Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

def flatten_json(data):
    '''A função recebe como entrada um JSON com uma estrutura aninhada e retornar um json mais estruturado.'''

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

    print('JSON achatado com sucesso!')
    return flat_data

def alters_columns(df):
    '''A função recebe um DataFrame e retorna um DataFrame com as colunas alteradas.'''
    
    # Altera as colunas de temperatura, converte de Kelvins para Celcius, 
    # e excluindo colunas de código, id e não tão relevantes (sys_sunrise', 'sys_sunset' , 'sys_type')
    df = df.withColumn('temp', df.temp - 273.15) \
            .withColumn('temp_min', df.temp_min - 273.15) \
            .withColumn('temp_max', df.temp_max - 273.15) \
            .withColumn('feels_like', df.feels_like - 273.15) \
            .drop('cod', 'id', 'sys_id', 'weather_id', 'sys_sunrise', 'sys_sunset' , 'sys_type') 

    print('Colunas alteradas com sucesso!')
    return df

def start_session_spark_and_transformations(data):
    
    # Iniciar sessão Spark
    print("Iniciando sessão Spark..")

    # Iniciar a sessão Spark: SparkSession.builder
    # Nome da sessão Spark: appName("ProcessWeatherJSON") 
    # Garante a sessão Spark seja iniciada sem váriaveis de sessão: .getOrCreate()
    spark = SparkSession.builder \
            .appName("ProcessWeatherJSON") \
            .getOrCreate() 
    
    # Achatando o JSON
    flatten_json_data = flatten_json(data)

    # Criar o DataFrame a partir do JSON achatado
    df = spark.createDataFrame([flatten_json_data])

    # Alterando colunas e tirando colunas não necessarias
    df = alters_columns(df)

    df.show(vertical=True, truncate=False)

    print('Alteração efetuada com sucesso!')

    return df
