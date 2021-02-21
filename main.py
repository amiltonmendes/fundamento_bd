"""
__author__ = "Amilton Lobo Mendes Júnior"
__email__ = "amilton.mendes@gmail.com"
"""
import os
from openpyxl import load_workbook

from unidecode import unidecode

import pandas as pd



from utils import  prepara_arquivo_download,Conexao,descompacta_arquivo

class CagedDBConfig():
    def __init__(self,usuario='projeto_bd',senha='teste123',base='fundamentos_bd'):
        self.conexao=Conexao(usuario,senha,base)


    def cria_tabela(self,con, sheet, sheet_data):
        """
        Tenta remover do banco de dados a tabela com as informações de domínio constantes do arquivo de layout do
        novo CAGED e, em seguida, as recria e carrega os dados

        Parameters
        ----------
        :param con: Conexão com o banco de dados utilizada para a realização das operações no banco
        :param sheet: String com o nome da planilha no arquivo de layout. Esse nome será utilizado como nome da tabela final no BD
        :param sheet_data: Pandas dataframe com as informações da planilha do excel contendo os valores de domínio para esta informação
        :return:
        """
        con = self.conexao.get_con()

        cursor = con.cursor()
        sql = ''
        nome_tabela = unidecode(sheet)
        print('Criando tabela ' + nome_tabela.upper())
        cursor.execute('DROP TABLE IF EXISTS ' + nome_tabela.upper())

        sql += 'CREATE TABLE ' + nome_tabela.upper() + ' (\n'
        for coluna in sheet_data.columns:
            sql += '  ' + unidecode(coluna).lower() + ' VARCHAR(150),\n'
        sql += '  CONSTRAINT pk_' + nome_tabela + ' PRIMARY KEY(' + unidecode(sheet_data.columns[0]).lower() + ')\n'
        sql += ');'
        cursor.execute(sql)
        con.commit()
        cursor.close()

        ##Insere as linhas das planilhas com informações do domínio no banco ( tabelas acessórias )
        sheet_data.rename(columns=lambda s: unidecode(s).lower(),inplace=True)
        engine = self.conexao.get_engine()
        sheet_data.to_sql(nome_tabela,con=engine,if_exists='append',index=False)



    def cria_tabela_principal(self,con, sheet_data):
        """
        Tenta remover do banco de dados a tabela com as informações das operações do Novo CAGED para, em seguida, as recriar

        Parameters
        ----------
        :param con: Conexão com o banco de dados utilizada para a realização das operações no banco
        :param sheet_data: Pandas dataframe com as informações do layout dos dados do novo CAGED
        :return:
        """
        con = self.conexao.get_con()

        cursor = con.cursor()
        sql = ''
        nome_tabela = 'CAGED'
        print('Criando tabela ' + nome_tabela)
        cursor.execute('DROP TABLE IF EXISTS ' + nome_tabela)

        sql += 'CREATE TABLE ' + nome_tabela + ' (\n'
        sql += '    id INT PRIMARY KEY AUTO_INCREMENT \n'
        for coluna in sheet_data['Variável']:
            sql += ',  ' + unidecode(coluna).lower() + ' VARCHAR(50)\n'

        for coluna in sheet_data['Variável']:
            if coluna == 'fonte':
                sql += ',  FOREIGN KEY (fonte) REFERENCES  FONTE_DESL(codigo) \n'
            elif (coluna not in ['competência', 'saldomovimentação', 'idade', 'horascontratuais', 'salário']):
                sql += ',  FOREIGN KEY (' + unidecode(coluna).lower() + ') REFERENCES ' + unidecode(
                    coluna).upper() + '(codigo) \n'
        sql += ');'
        cursor.execute(sql)
        con.commit()
        cursor.close()

    def create_tables(self,path_layout):
        """
        Cria uma tabela para cada aba da planilha de layout e, para as planilhas diferentes da 'Layout, insere os dados
        :param path_layout:
        :return:
        """
        layout = load_workbook(path_layout)
        con = self.conexao.get_con()

        cursor = con.cursor()
        sql = ''

        nome_tabela = 'CAGED'
        cursor.execute('DROP TABLE IF EXISTS ' + nome_tabela)
        con.commit()
        cursor.close()

        for sheet in layout.sheetnames:

            if sheet != 'Layout':
                df_sheet = pd.read_excel(path_layout, sheet_name=sheet, engine='openpyxl')
                self.cria_tabela(con, sheet, df_sheet)
        df_sheet = pd.read_excel(path_layout, sheet_name='Layout', engine='openpyxl', skiprows=1)
        self.cria_tabela_principal(con, df_sheet)
        con.disconnect()


    def prepara_bases(self):
        if prepara_arquivo_download(os.getcwd()+'\config'\
                ,'ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/Movimentações/Layout Novo Caged Movimentação.xlsx'\
                ,'\layout_caged.xlsx'):
            self.create_tables(os.getcwd()+'\config\layout_caged.xlsx')
    def insere_dados_caged(self,lista_meses,head=None):
        url = 'ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/Movimentações/2020/Dezembro/CAGEDMOV2020'
        for mes in lista_meses:
            url_mes = url+mes+'.7z'
            prepara_arquivo_download(os.getcwd()+'\\tmp' , url_mes , '\caged_2020'+mes+'7z')
            descompacta_arquivo(os.getcwd()+'/tmp/caged_2020'+mes+'7z',os.getcwd()+'/data')
            if head == None:
                df_caged = pd.read_csv(os.getcwd()+'/data/CAGEDMOV2020'+mes+'.txt',sep=';')
            else:
                df_caged = pd.read_csv(os.getcwd()+'/data/CAGEDMOV2020'+mes+'.txt',sep=';',nrows=head)
            df_caged.rename(columns=lambda s: unidecode(s).lower(), inplace=True)
            engine = self.conexao.get_engine()
            df_caged.head().to_sql('caged', con=engine, if_exists='append', index=False)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    caged = CagedDBConfig()
    caged.prepara_bases()
    caged.insere_dados_caged(['01','02','03'],head=5)
