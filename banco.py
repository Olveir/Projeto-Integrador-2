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

    sql_script = """
    DROP TABLE IF EXISTS Fato_Estabelecimento;
    DROP TABLE IF EXISTS Dim_Estabelecimento;
    DROP TABLE IF EXISTS Dim_Localizacao;
    DROP TABLE IF EXISTS Dim_Organizacao;
    DROP TABLE IF EXISTS Dim_Turno;
    DROP TABLE IF EXISTS Dim_Servicos;
    DROP TABLE IF EXISTS Dim_Tempo;

    CREATE TABLE dim_Estabelecimento (
        cod_unidade VARCHAR(100) PRIMARY KEY,
        nm_razao_social VARCHAR(255),
        nm_fantasia VARCHAR(255),
        num_cnpj BIGINT, 
        num_cnpj_entidade BIGINT, 
        email VARCHAR(255),
        num_telefone VARCHAR(40),
        cod_motivo_desab INT
    );

    CREATE TABLE dim_Localizacao (
        cod_unidade VARCHAR(100) PRIMARY KEY,
        cod_cep BIGINT,
        endereco VARCHAR(255),
        numero BIGINT,
        bairro VARCHAR(100),
        latitude NUMERIC(18, 15),
        longitude NUMERIC(18, 15),
        cod_ibge BIGINT,
        cod_uf BIGINT
    );

    CREATE TABLE dim_Organizacao (
        cod_cnes BIGINT PRIMARY KEY,
        tp_unidade BIGINT,
        tp_gestao CHAR(1),
        cod_esfera_administrativa CHAR(1),
        dscr_esfera_administrativa VARCHAR(50),
        cod_natureza_jur BIGINT,
        cod_atividade BIGINT,
        cod_nivel_hierarquia BIGINT,
        dscr_nivel_hierarquia VARCHAR(255),
        cod_natureza_organizacao BIGINT,
        dscr_natureza_organizacao VARCHAR(255)
    );

    CREATE TABLE dim_Turno (
        cod_turno_atendimento BIGINT PRIMARY KEY,
        dscr_turno_atendimento TEXT
    );

    CREATE TABLE dim_Servicos (
        cod_cnes BIGINT PRIMARY KEY,
        st_faz_atendimento_ambulatorial_sus BOOLEAN,
        st_centro_cirurgico BOOLEAN,
        st_centro_obstetrico BOOLEAN,
        st_centro_neonatal BOOLEAN,
        st_atendimento_hospitalar BOOLEAN,
        st_servico_apoio BOOLEAN,
        st_atendimento_ambulatorial BOOLEAN
    );

    CREATE TABLE fato_Estabelecimento (
        cod_cnes BIGINT NOT NULL PRIMARY KEY,
        cod_turno_atendimento BIGINT NOT NULL,
        data_extracao TIMESTAMP NOT NULL,
        cod_unidade VARCHAR(100),
        
        CONSTRAINT fk_fato_dim_estabelecimento FOREIGN KEY (cod_unidade) REFERENCES dim_Estabelecimento(cod_unidade),
        CONSTRAINT fk_fato_dim_localizacao FOREIGN KEY (cod_unidade) REFERENCES dim_Localizacao(cod_unidade),
        CONSTRAINT fk_fato_dim_organizacao FOREIGN KEY (cod_cnes) REFERENCES dim_Organizacao(cod_cnes),
        CONSTRAINT fk_fato_dim_turno FOREIGN KEY (cod_turno_atendimento) REFERENCES dim_Turno(cod_turno_atendimento),
        CONSTRAINT fk_fato_dim_servicos FOREIGN KEY (cod_cnes) REFERENCES dim_Servicos(cod_cnes)
    );
    """

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