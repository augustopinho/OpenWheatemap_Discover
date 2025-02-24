# Import Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

def create_session_spark():
    '''A função inicializa e configura a sessão Spark.'''

    # Iniciar sessão Spark
    print("Iniciando sessão Spark...")

    try:
        spark = SparkSession.builder \
        .appName("TestePostgres") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.5.jar") \
        .getOrCreate()

    except:
        print('Erro na inicialização do Spark')

    return spark


def flatten_json_and_create_df(data, spark):
    '''A função recebe como entrada um JSON com uma estrutura aninhada e retornar um json mais estruturado, posteriormente é feito o DataFrame.'''

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

    df = spark.createDataFrame([flat_data])

    return df

def alters_columns(df):
    '''A função recebe um DataFrame e retorna um DataFrame com as colunas alteradas de temperatura e dropa as colunas de código e id.'''
    
    # Altera as colunas de temperatura, converte de Kelvins para Celcius,
    # Alterando o Dtype de dt (data)
    # e excluindo colunas de código e id
    df = df.withColumn('temp', df.temp - 273.15) \
            .withColumn('temp_min', df.temp_min - 273.15) \
            .withColumn('temp_max', df.temp_max - 273.15) \
            .withColumn('feels_like', df.feels_like - 273.15) \
            .withColumn("dt", from_unixtime(col("dt")).cast("timestamp")) \
            .withColumn("sys_sunrise", from_unixtime(col("sys_sunrise")).cast("timestamp")) \
            .withColumn("sys_sunset", from_unixtime(col("sys_sunset")).cast("timestamp")) \
            .drop('cod', 'id', 'sys_id', 'weather_id') 
    
    print('Alteração de colunas efetuada com sucesso!')

    df.show(vertical=True, truncate=False)

    return df

