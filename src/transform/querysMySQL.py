import mysql.connector
import os
from dotenv import load_dotenv


class MySQLQuery:
    def __init__(self):
        self.connection = None
        load_dotenv() 

        self.host = os.getenv('host')
        self.user = os.getenv('user')
        self.password = os.getenv('password')
        self.database = os.getenv('database')

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def execute_query(self):
        try:
            cursor = self.connection.cursor()

            # Consulta SQL que junta as tabelas "order" e "order_details" usando a coluna "order_id"
            query = """
            SELECT *
            FROM `orders`
            INNER JOIN order_details ON `orders`.order_id = order_details.order_id
            """

            cursor.execute(query)
            result = cursor.fetchall()

            # Exibe o resultado da consulta
            for row in result:
                print(row)

        except mysql.connector.Error as error:
            print("Ocorreu um erro ao executar a consulta:", error)

        finally:
            if cursor:
                cursor.close()

