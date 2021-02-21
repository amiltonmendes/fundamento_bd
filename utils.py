import wget
import os
import py7zr
import mysql.connector
from sqlalchemy import create_engine


class Conexao:
    """
    Classe singleton para armazenar a conexão com o banco e engine desse banco no sqlalchemy
    """
    def __init__(self,usuario,senha,banco):
        self.USUARIO_BD=usuario
        self.SENHA_BD=senha
        self.BANCO_PROJETO=banco
        self.con=None
    def get_con(self):
        """
        Recupera a conexão do banco aberta ou cria uma e a retorna
        :return:
        """
        if self.con == None:
            mydb = mysql.connector.connect(
                host="localhost",
                user=self.USUARIO_BD,
                password=self.SENHA_BD,
                database=self.BANCO_PROJETO
            )
        return mydb
    def get_engine(self):
        return create_engine('mysql+mysqlconnector://'+self.USUARIO_BD+':'+self.SENHA_BD+'@localhost/'+self.BANCO_PROJETO)
def descompacta_arquivo(source,dest):
    """
    Função para a descompactação do arquivo no formato 7zip
    :param source: arquivo 7zip
    :param dest: pasta onde os arquivos compatados serão extraídos
    :return:
    """
    archive = py7zr.SevenZipFile(source, mode='r')
    archive.extractall(path=dest)
    archive.close()



def prepara_arquivo_download(dest_path, url, dest_file):
    """
    Função responsável por realizar o download do arquivo

    :param dest_path: pasta de destino onde o arquivo será salvo
    :param url: endereço onde o arquivo está hospedado
    :param dest_file: local onde o arquivo será armazenado no computador
    """
    path = dest_path
    if not os.path.exists(path):
        try:
            os.mkdir(path)
        except OSError:
            print("Criação da pasta o onde arquivo contendo o layout seria salvo %s falhou" % path)
        else:
            print("Diretório criado com sucesso %s " % path)
    if not os.path.exists(path + '/' + dest_file):
        download_file(url,
                      path + dest_file)
    return True


def download_file(url, arquivo):
    """
    Função para o download de arquivo e armazenamento

    Parameters
    ----------
    :param url: str, endereço do arquivo a ser baixado
    :param arquivo: str, path com o destino do arquivo
    """
    print('Iniciando o download do arquivo \'' + arquivo + '\' a partir da url ' + url)
    wget.download(url, arquivo)
