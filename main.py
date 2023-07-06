from src.extract.csvToCSV import CsvToCSV
from src.extract.dataBaseToCSV import DatabaseToCSV
from src.load.saveOnDB import SaveOnDB
from src.transform.querysMySQL import MySQLQuery
import time
from datetime import date

while True:
    data_hoje = date.today()
    print("Executando pipeline... ", data_hoje)
    time.sleep(5)
    # 1 - Extract files
    print("Copiando arquivos CSV para a pasta reports-",data_hoje)
    time.sleep(5)
    CsvToCSV.order_details();

    print("Copiando arquivos do banco Postegres e salvando em reports-",data_hoje)
    time.sleep(5)
    db_connection = DatabaseToCSV()
    db_connection.connect()
    db_connection.export_tables_to_csv()
    db_connection.close()

    # # 2 - Loading csv data
    print("Salvando dados locais no Banco MySQL")
    time.sleep(5)
    save_on_db = SaveOnDB()
    save_on_db.conectar()
    save_on_db.salvar_csv_no_db()
    save_on_db.desconectar()

    # # 3 - Transform - Juntando tabelas orders com order_details
    print("Realizando consulta... Juntando tabelas orders com orde_detail")
    time.sleep(5)
    dbQuerys = MySQLQuery()
    dbQuerys.connect()
    dbQuerys.execute_query()
    dbQuerys.disconnect()

    # # 4 - Garantindo q o programa seja executado a cada 24hrs
    time.sleep(24*60*60 -25)


