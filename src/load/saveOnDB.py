import os
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import date

class SaveOnDB:
    def __init__(self):
        self.conexao = None
        self.cursor = None
        load_dotenv()

        self.host = os.getenv('host')
        self.user = os.getenv('user')
        self.password = os.getenv('password')
        self.database = os.getenv('database')
        self.pasta_csv = './reports'

    def conectar(self):
        # Conexão com o banco de dados MySQL usando SQLAlchemy
        try:
            connection_string = f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}"
            self.engine = create_engine(connection_string)
            print('Conexão bem-sucedida!')
        except psycopg2.Error as e:
            print('Erro ao conectar ao banco de dados:', e)

    def desconectar(self):
        # Fechar a conexão com o banco de dados
        if self.engine:
            self.engine.dispose()
            print('Conexão encerrada.')

    def salvar_csv_no_db(self):
        # Obter o dia de hoje
        hoje = date.today().strftime("%Y-%m-%d")
        
        print("Emilly: ", self.pasta_csv)
        # Verificar se a pasta com a data de hoje existe
        pasta_hoje = os.path.join(self.pasta_csv, hoje)
        if not os.path.isdir(pasta_hoje):
            print(f"A pasta com a data de hoje ({hoje}) não foi encontrada.")
            return

        # Lista todos os arquivos CSV na pasta com a data de hoje
        arquivos_csv = [
            arquivo for arquivo in os.listdir(pasta_hoje) if arquivo.endswith(".csv")
        ]

        # Itera sobre os arquivos CSV e salva no banco de dados
        for arquivo in arquivos_csv:
            # Carrega o arquivo CSV em um DataFrame do pandas
            caminho_arquivo = os.path.join(pasta_hoje, arquivo)
            df = pd.read_csv(caminho_arquivo, encoding='latin1')

            # Obtém o nome da tabela a partir do nome do arquivo CSV (sem a extensão)
            nome_tabela = os.path.splitext(arquivo)[0]

            # Cria a tabela no banco de dados
            df.to_sql(nome_tabela, self.engine, if_exists='replace', index=False)
            print(f"Dados do arquivo CSV '{nome_tabela}' foi adicionado na tabela '{nome_tabela}' do banco MySQL.")

