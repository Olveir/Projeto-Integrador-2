import psycopg2
from psycopg2 import OperationalError

def execute_sql_script():
    conn_params = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",   
        "password": "12345",  
        "dbname": "postgres"       
    }

    sql_script = '''
    DROP TABLE IF EXISTS fato_leito;
    CREATE TABLE public.fato_leito (
        cod_cnes BIGINT,
        nm_estabelecimento VARCHAR(255) NOT NULL,
        qtd_leitos_existentes INT NOT NULL, 
        qtd_leitos_sus INT NOT NULL, 
        qtd_uti_total_exist INT NOT NULL, 
        qtd_uti_total_sus INT NOT NULL, 
        qtd_uti_adulto_exist INT NOT NULL, 
        qtd_uti_adulto_sus INT NOT NULL, 
        qtd_uti_pediatrico_exist INT NOT NULL, 
        qtd_uti_pediatrico_sus INT NOT NULL, 
        qtd_uti_neonatal_exist INT NOT NULL, 
        qtd_uti_neonatal_sus INT NOT NULL, 
        qtd_uti_queimado_exist INT NOT NULL, 
        qtd_uti_queimado_sus INT NOT NULL, 
        qtd_uti_coronariana_exist INT NOT NULL, 
        qtd_uti_coronariana_sus INT NOT NULL, 
        dscr_tipo_unidade VARCHAR(255) NOT NULL,
        anomes BIGINT NOT NULL,
        PRIMARY KEY (cod_cnes, anomes)
    );
    '''
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**conn_params)
        
        cursor = conn.cursor()
        
        for command in sql_script.split(";"):
            if command.strip():
                cursor.execute(command)
        
        conn.commit()
        print("Tabelas criadas com sucesso!")

    except OperationalError as e:
        print(f"Erro de conexão: {e}")
    except psycopg2.Error as e:
        print(f"Ocorreu um erro ao executar o script: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    execute_sql_script()