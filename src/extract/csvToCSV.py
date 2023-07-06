import pandas as pd
import glob
import os
import datetime

class CsvToCSV():

    def order_details():
        diretorio = './data'
        all_files = glob.glob(os.path.join(diretorio, "order_details*.csv"))

        # order_id,product_id,unit_price,quantity,discount
        df = pd.read_csv(all_files[0], names=["order_id", "product_id", "unit_price", "quantity", "discount"], delimiter=',', header=0)
        # iterar sobre os arquivos restantes e concatená-los ao dataframe
        for f in all_files[1:]:
            temp_df = pd.read_csv(f, names=["order_id", "product_id", "unit_price", "quantity", "discount"], delimiter=',', header=None)
            df = pd.concat([df, temp_df])

        # Obter a data atual
        data_atual = datetime.date.today()

        # Criar o caminho completo para a pasta
        caminho_pasta = f"./reports/{data_atual}"

        # Verificar se a pasta já existe, caso contrário, criar a pasta
        if not os.path.exists(caminho_pasta):
            os.makedirs(caminho_pasta)

        # Criar o caminho completo para o arquivo
        caminho_arquivo = os.path.join(caminho_pasta, "order_details.csv")

        # Salvar o dataframe como CSV na pasta com a data de hoje
        df.to_csv(caminho_arquivo, index=False)

        print(f"Arquivo salvo em: {caminho_arquivo}")