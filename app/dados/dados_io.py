# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

"""
  Estrutura de Dados para o Gerenciador 
  
"""

class MockDadosIO:
    _spark = None

    # Linux localization
    _diretorio_linux = '/home/jovyan/work/geofusion/'
    _diretorio_linux = '/home/lisboa/geofusion/'

    _diretorio_windows = 'C:\\dump\\gf\\'
    _diretorio = ''

    def __init__(self, job, so):
        if MockDadosIO._spark is None:
            MockDadosIO._spark = SparkSession.builder.appName(job).getOrCreate()

        # Setup diretorio de leitura
        if so == 0:
            MockDadosIO._diretorio = MockDadosIO._diretorio_linux
        else:
            MockDadosIO._diretorio = MockDadosIO._diretorio_windows

    def spark_session(self):
        return MockDadosIO._spark

    # Bairros
    def bairros(self):
        return MockDadosIO._spark.read.option("encoding", "utf-8").csv(
            MockDadosIO._diretorio + 'bairros.csv', header=True, sep=',')

    # Concorrentes
    def concorrentes(self):
        return MockDadosIO._spark.read.option("encoding", "utf-8").csv(
            MockDadosIO._diretorio + 'concorrentes.csv', header=True, sep=',')

    # Eventos fluxos
    def eventos_de_fluxo(self):
        return MockDadosIO._spark.read.option("encoding", "utf-8").csv(
            MockDadosIO._diretorio + 'eventos_de_fluxo.csv', header=True, sep=',')

    # Populacao
    def populacao(self):
        return MockDadosIO._spark.read.option("encoding", "utf-8").json(
            MockDadosIO._diretorio + 'populacao.json')

    def gravar(self, df, nome):
        df.repartition(1).write.csv(  MockDadosIO._diretorio + nome, header=True, mode='overwrite', sep='|')


class ProdDadosIO:
    _spark = None

    def __init__(self, job):
        if ProdDadosIO._spark is None:
            ProdDadosIO._spark = SparkSession.builder.appName(job) \
                .config("hive.exec.dynamic.partition", "true") \
                .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                .enableHiveSupport() \
                .getOrCreate()

    def spark_session(self):
        return ProdDadosIO._spark

    # Bairros
    def bairros(self):
        return ProdDadosIO._spark.read.table('raw_gf.bairros')

    # Concorrentes
    def concorrentes(self):
        return ProdDadosIO._spark.read.table('raw_gf.concorrentes')

    # Eventos fluxos
    def eventos_de_fluxo(self):
        return ProdDadosIO._spark.read.table('raw_gf.eventos_de_fluxo')

    # Populacao
    def populacao(self):
        return ProdDadosIO._spark.read.table('raw_gf.populacao')

    def gravar(self, df, nome):
        df.write.option("compression", "zlib").option("encoding", "UTF-8").mode(
            "overwrite").format("orc").option(
            "header", "false").insertInto(nome, overwrite=True)