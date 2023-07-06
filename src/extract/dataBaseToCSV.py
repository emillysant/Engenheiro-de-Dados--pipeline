import psycopg2
import csv
import os
from datetime import date

class DatabaseToCSV:
    def __init__(self):
        self.connection = None
        self.host = 'localhost'
        self.port = 5432
        self.database = 'northwind'
        self.user = 'northwind_user'
        self.password = 'thewindisblowing'

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print('Conexão bem-sucedida!')
        except psycopg2.Error as e:
            print('Erro ao conectar ao banco de dados:', e)

    def close(self):
        if self.connection:
            self.connection.close()
            print('Conexão encerrada.')

    def export_tables_to_csv(self):
        if not self.connection:
            print('Nenhuma conexão com o banco de dados estabelecida.')
            return

        cursor = self.connection.cursor()

        try:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
            table_names = cursor.fetchall()

            output_dir = f"./reports/{date.today()}"
            os.makedirs(output_dir, exist_ok=True)

            for table_name in table_names:
                csv_file = os.path.join(output_dir, f"{table_name[0]}.csv")

                with open(csv_file, 'w', newline='') as file:
                    writer = csv.writer(file)

                    cursor.execute(f"SELECT * FROM {table_name[0]};")
                    rows = cursor.fetchall()

                    column_names = [desc[0] for desc in cursor.description]
                    writer.writerow(column_names)

                    for row in rows:
                        writer.writerow(row)

                print(f"Arquivo CSV '{csv_file}' criado com sucesso para a tabela '{table_name[0]}' do postegres.")

        except psycopg2.Error as e:
            print('Erro ao exportar tabelas para CSV:', e)

        cursor.close()
