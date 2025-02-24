import psycopg2
from pyspark.sql import SparkSession


def create_table_psycopg2():
    '''
    Cria uma conexão do postgres com o psycopg2, depois verifica se a tabela 'tb_stats_weather' já está criada,
    se não, cria a tabela.
    '''

    print('...')

    try:
        conn = psycopg2.connect(user="postgres",
                                password="42pinhoAB!",
                                host="localhost",
                                port="5432",
                                database="open_weather_map" 
                                )
        
        print("Conexão do psycopg2 efetuada com sucesso!")

        cur = conn.cursor()

        cur.execute('''
                    CREATE TABLE IF NOT EXISTS tb_stats_weather(
                        base VARCHAR(20)
                    ,   clouds_all INTEGER
                    ,   coord_lat NUMERIC(9,6)
                    ,   coord_lon NUMERIC(9,6)
                    ,   dt TIMESTAMP
                    ,   feels_like NUMERIC(22,19)
                    ,   grnd_level INTEGER
                    ,   humidity INTEGER
                    ,   name VARCHAR(30)
                    ,   pressure INTEGER
                    ,   sea_level INTEGER
                    ,   sys_country VARCHAR(3)
                    ,   sys_sunrise TIMESTAMP
                    ,   sys_sunset TIMESTAMP
                    ,   sys_type INTEGER
                    ,   temp NUMERIC(22,19)
                    ,   temp_max NUMERIC(22,19)
                    ,   temp_min NUMERIC(22,19)
                    ,   timezone INTEGER
                    ,   visibility INTEGER
                    ,   weather_description VARCHAR(40)
                    ,   weather_icon VARCHAR(10)
                    ,   weather_main VARCHAR(20)
                    ,   wind_deg INTEGER
                    ,   wind_speed NUMERIC(7,3)
                    );
                ''')
        
        print('Tabela verifica/criada com sucesso!')
        conn.commit()


    except Exception as e:
        print(f"Ocorreu o seguinte erro na verificação/criação da tabela: {e}")


    finally:
        if conn is not None:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")

            print('...')



def write_postgres(df):
    
    # JDBC URL configurações
    jdbc_url = "jdbc:postgresql://localhost:5432/open_weather_map"

    # Propriedades do PostgreSQL
    properties = {
        "user": "postgres",
        "password": "42pinhoAB!",
        "driver": "org.postgresql.Driver"
    }

    # Teste a leitura de uma tabela
    df.write.jdbc(
        url=jdbc_url,
        table="tb_stats_weather",
        mode="append",
        properties=properties
    )
