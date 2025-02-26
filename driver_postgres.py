import psycopg2
from pyspark.sql import SparkSession

class drive_postgres:
    '''
    Criação da classe (drive_postgres) para facilitar o uso dos métodos relacionado ao PostgreSQL.
    Além disso, os métodos dentro dessa classe, tem o intuito de facilitar a pesistencia dos dados no SGBD PostgreSQL.
    '''

    def __init__(self):
        
        # Definindo variaveis/propriedades dos SGBDs (PostgreSQL)
        self.user = "postgres"
        self.password = "42pinhoAB!"
        self.host = "localhost"
        self.port = "5432"
        self.database = "open_weather_map"
        self.table = "tb_stats_weather"

    def create_table_psycopg2(self):
        '''
        Cria uma conexão do postgres com o psycopg2, depois verifica se a tabela 'tb_stats_weather' já está criada,
        se não, cria a tabela.
        '''

        print('...')

        # Tentando connectar ao PostgreSQL
        try:
            conn = psycopg2.connect(user=self.user,
                                    password=self.password,
                                    host=self.host,
                                    port=self.port,
                                    database=self.database 
                                    )
            
            print("Conexão do psycopg2 efetuada com sucesso!")

            # Criando o curso do psycopg2
            cur = conn.cursor()

            # Executando o código via SQL
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

            # Encerrando o cursor e a conexão do postgreSQL via psycopg2
            if conn is not None:
                cur.close()
                conn.close()
                print("PostgreSQL connection is closed")

                print('...')

        # Demostrando o erro de SQL ou de conexão.
        except Exception as e:
            print(f"Ocorreu o seguinte erro na verificação/criação da tabela: {e}")

    def write_postgres(self, df):
        '''
        Persistindo o Dataframe do Spark no PostgreSQL por meio do JDBC.
        '''
        
        # JDBC URL configurações
        jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

        # Propriedades do PostgreSQL
        properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }

        # Teste a leitura de uma tabela
        df.write.jdbc(
            url=jdbc_url,
            table=self.table,
            mode="append",
            properties=properties
        )
