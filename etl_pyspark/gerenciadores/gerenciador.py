# -*- coding: utf-8 -*-
import regras.sql as adobe_sql
import regras._udf_comum as udf_comum
import pyspark.sql.functions as f
from pyspark.sql.functions import date_format, dayofmonth, hour, col, max as max_

"""
    Classe Gerenciadora de regras logicas 
    
    Construtor:
        Argumentos:
            param1 (self): Referencia para o proprio objeto.
            param2 (DadosIO): objeto da classe de dados que possui uma conexao com o Spark.
    
        Logica: recupera os Dataframes originais necessarios (cliente, pedido, etc...)
"""


class Gerenciador:
    # Conexao (session) do Spark e acesso a dados
    _dados_io = None
    _spark_session = None

    # Dataframes originais
    df_bairros = None
    df_concorrentes = None
    df_eventos_de_fluxo = None
    df_populacao = None

    def __init__(self, _dados_io):
        self._dados_io = _dados_io
        self._spark_session = _dados_io.spark_session()

        # Dataframes
        self.df_bairros = _dados_io.bairros()
        self.df_concorrentes = _dados_io.concorrentes()
        self.df_eventos_de_fluxo = _dados_io.eventos_de_fluxo()
        self.df_populacao = _dados_io.populacao()

    # Processamento
    def do_gf(self):
        # A densidade demográfica de um bairro é uma informação muito
        # importante para nossos clientes e é uma informação que precisa ser
        # calculada. A densidade demográfica de um bairro é o resultado da divisão
        # da população do bairro pela área do bairro.

        df_densidade_demografica = self.df_populacao.join(self.df_bairros,
                                                          (self.df_populacao.codigo == self.df_bairros.codigo), 'left') \
            .select(self.df_bairros["codigo"].alias("codigo_bairro"),
                    self.df_bairros["nome"].alias("nome_bairro"),
                    self.df_bairros["area"],
                    self.df_populacao["populacao"],
                    (self.df_populacao["populacao"] / self.df_bairros["area"]).alias('dens_demografica'))

        # Análise do Fluxo de Pessoas por concorrentes
        df_fluxo_pessoas = self.df_eventos_de_fluxo \
            .select('codigo_concorrente',
                    'datetime',
                    dayofmonth("datetime").alias('dia'),
                    hour("datetime").alias('hora'),
                    udf_comum.udf_get_periodo_dia(hour("datetime").alias('hora')).alias('periodo'),
                    date_format("datetime", 'u').alias('semana_id'),
                    date_format("datetime", 'E').alias('semana_nome')) \
            .groupBy('codigo_concorrente', 'semana_id', 'semana_nome', 'periodo') \
            .agg(f.count('codigo_concorrente').alias('qtd_visitas')) \
            .sort(col('codigo_concorrente'), col('semana_id').asc(), col('periodo').asc())

        # Neste case seu desafio será implementar um serviço REST que retorne
        # informações dos concorrentes como código, nome, endereço, preço
        # praticado, fluxo médio de pessoas por dia da semana e por período do dia,
        # bairro e a população e a densidade demográfica do bairro.

        df_final = self.df_concorrentes.join(df_densidade_demografica,
                                        (self.df_concorrentes.codigo_bairro == df_densidade_demografica.codigo_bairro),
                                        'left') \
            .join(df_fluxo_pessoas,
                  (self.df_concorrentes.codigo == df_fluxo_pessoas.codigo_concorrente), 'left') \
            .select(self.df_concorrentes['codigo'].alias('codigo_concorrente'),
                    self.df_concorrentes['nome'],
                    self.df_concorrentes['categoria'],
                    self.df_concorrentes['endereco'],
                    self.df_concorrentes['municipio'],
                    self.df_concorrentes['uf'],
                    self.df_concorrentes['codigo_bairro'],
                    df_densidade_demografica['area'],
                    df_densidade_demografica['populacao'],
                    df_densidade_demografica['dens_demografica'],
                    df_fluxo_pessoas['semana_id'],
                    df_fluxo_pessoas['semana_nome'],
                    df_fluxo_pessoas['periodo'],
                    df_fluxo_pessoas['qtd_visitas'])


        # Filtro para visualizar o resultado final do processamento

        # df_final.select('codigo_concorrente', 'endereco', 'qtd_visitas', 'semana_nome', 'periodo', 'dens_demografica') \
        #     .filter(df_final['qtd_visitas'].isNotNull()) \
        #     .sort(col('codigo_concorrente'), col('semana_id').asc(), col('periodo').asc()) \
        #     .show(n=10)

        self._dados_io.gravar(df_final, 'app_gf.final_concorrente')

    # Chamada padrão do Gerenciador para leitura conforme os parametros de execucao do servico
    def validacoes(self):
        self.do_gf()
