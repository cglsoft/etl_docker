# -*- coding: utf-8 -*-


import pyspark.sql.functions as f
import regras._udf_comum as udf_comum
from pyspark.sql.types import *

'''
    Arquivo para criar funcoes que usam SparkSQL para realizar transformacoes que retornam Dataframes.
    Evite usar sql em String.
'''


def adobe_traffic(df_traffic, df_deparadepto):
    df_depara = (df_deparadepto
                 .filter((df_deparadepto['tabela'] == 'departamento') & (df_deparadepto['campo'] == 'iddepartamento'))
                 .select(df_deparadepto['de'],
                         df_deparadepto['para']
                         )
                 .withColumn('server', f.lit(None).cast(StringType()))
                 )

    return (df_traffic
            .join(df_depara, df_traffic['iddepartament'] == df_depara['de'], 'left')
            .select(f.trim(df_traffic['id']).alias('id'),
                    f.trim(df_traffic['createdat']).alias('createdat'),
                    f.trim(df_traffic['reportsuiteid']).alias('reportsuiteid'),
                    f.trim(df_traffic['reportsuitename']).alias('reportsuitename'),
                    df_depara['server'],
                    f.trim(df_traffic['data']).alias('data'),
                    f.trim(df_traffic['hour']).alias('hour'),
                    f.trim(df_traffic['iddepartament']).alias('iddepartamento'),
                    f.trim(df_depara['para']).alias('nomedepartamento'),
                    f.trim(df_traffic['channel']).alias('channel'),
                    f.trim(df_traffic['visits']).alias('visits'),
                    f.trim(df_traffic['uniquevisitors']).alias('uniquevisitors'),
                    f.trim(df_traffic['pageviews']).alias('pageviews'),
                    f.trim(df_traffic['orders']).alias('orders'),
                    f.trim(df_traffic['ordersserialized']).alias('ordersserialized'),
                    f.trim(df_traffic['revenue']).alias('revenue'),
                    f.trim(df_traffic['bouncerate']).alias('bouncerate'),
                    f.trim(df_traffic['exits']).alias('exits'),  # vai mudar para exits na raw_parceiro
                    f.trim(df_traffic['productviewserialized']).alias('productviewserialized'),
                    f.trim(df_traffic['cartaddserialized']).alias('cartaddserialized'),
                    f.trim(df_traffic['checkoutserialized']).alias('checkoutserialized'),
                    f.trim(df_traffic['checkoutaddresslist']).alias('checkoutaddresslist'),
                    f.trim(df_traffic['checkoutpayment']).alias('checkoutpayment'),
                    f.trim(df_traffic['checkoutconfirmation']).alias('checkoutconfirmation'),
                    f.trim(df_traffic['bounces']).alias('bounces'),
                    f.trim(df_traffic['entries']).alias('entries')
                    )
            )


# Execução de h/h
def adobe_canalMkt(df_canalmkt, df_adobe_depara_CanaisMkt, df_adobe_depara_gestao):
    # Left join(CanalMkt, deparacanaismkt) adiciona campo nomecanalmis aos campos da raw(CanalMkt)
    # Tratamento do depara_gestao, rename de campos
    df_depara_adobe = (df_adobe_depara_gestao
                       .select(f.trim(df_adobe_depara_gestao['nomedepartamento']).alias('nomedepartamento'),
                               f.trim(df_adobe_depara_gestao['nomelinha']).alias('nomelinha'),
                               f.trim(df_adobe_depara_gestao['sublinha']).alias('nomesublinha'),
                               f.trim(df_adobe_depara_gestao['no_gerencia']).alias('no_gerencia'),
                               f.trim(df_adobe_depara_gestao['gerencia']).alias('gerencia'),
                               f.trim(df_adobe_depara_gestao['categoria']).alias('categoria'),
                               f.trim(df_adobe_depara_gestao['subcategoria']).alias('subcategoria')
                               )
                       )

    # Atualizacao campo em branco
    # df_canalmkt = df_canalmkt.fillna('::unspecified::', 'canalmis')

    df_canalmkt = df_canalmkt.withColumn('canalmis_key',
                                         f.when(f.length(f.trim(f.col('canalmis'))) != 0, df_canalmkt['canalmis'])
                                         .otherwise(f.lit('::unspecified::').cast(StringType())))

    df_depara_canaisMkt = (df_canalmkt
                           .join(df_adobe_depara_CanaisMkt,
                                 df_canalmkt['canalmis_key'] == df_adobe_depara_CanaisMkt['canal_mis'], 'left')
                           .select(f.trim(df_canalmkt['id']).alias('id'),
                                   f.trim(df_canalmkt['createdat']).alias('createdat'),
                                   f.trim(df_canalmkt['bandeira']).alias('reportsuiteid'),
                                   f.trim(df_canalmkt['data']).alias('data'),
                                   f.trim(df_canalmkt['data2']).alias('data2'),
                                   f.trim(df_canalmkt['hour']).alias('hour'),
                                   f.trim(df_canalmkt['iddepartamento']).alias('iddepartamento'),
                                   f.trim(df_canalmkt['nomedepartamento']).alias('nomedepartamento'),
                                   f.trim(df_canalmkt['idlinha']).alias('idlinha'),
                                   f.trim(df_canalmkt['nomelinha']).alias('nomelinha'),
                                   f.trim(df_canalmkt['idsublinha']).alias('idsublinha'),
                                   f.trim(df_canalmkt['nomesublinha']).alias('nomesublinha'),
                                   f.trim(df_canalmkt['canalmis_key']).alias('canalmis'),
                                   f.trim(df_adobe_depara_CanaisMkt['nome_midia']).alias('nomecanalmis'),
                                   f.trim(df_canalmkt['visits']).alias('visits'),
                                   f.trim(df_canalmkt['pedidos']).alias('pedidos'),
                                   f.trim(df_canalmkt['receita']).alias('receita'),
                                   f.trim(df_canalmkt['unidades']).alias('unidades'),
                                   f.trim(df_canalmkt['productview']).alias('productview'),
                                   f.trim(df_canalmkt['carrinhoadicionar']).alias('carrinhoadicionar'),
                                   f.trim(df_canalmkt['checkout']).alias('checkout'),
                                   f.trim(df_canalmkt['checkoutenderecolistar']).alias(
                                       'checkoutenderecolistar'),
                                   f.trim(df_canalmkt['checkoutpagamento']).alias('checkoutpagamento')
                                   )
                           )

    # Left join(df_deparaCanaisMkt, df_depara_adobe) append campos depara nrgerencia, gerencia, categoria e subcategoria
    df_canalmkt_final = (df_depara_canaisMkt
                         .join(df_depara_adobe, ['nomedepartamento', 'nomelinha', 'nomesublinha'], 'left')
                         .select(df_depara_canaisMkt['id'],
                                 df_depara_canaisMkt['createdat'],
                                 df_depara_canaisMkt['reportsuiteid'],
                                 df_depara_canaisMkt['data'],
                                 df_depara_canaisMkt['data2'],
                                 df_depara_canaisMkt['hour'],
                                 df_depara_canaisMkt['iddepartamento'],
                                 df_depara_canaisMkt['nomedepartamento'],
                                 df_depara_canaisMkt['idlinha'],
                                 df_depara_canaisMkt['nomelinha'],
                                 df_depara_canaisMkt['idsublinha'],
                                 df_depara_canaisMkt['nomesublinha'],
                                 df_depara_canaisMkt['canalmis'],
                                 df_depara_canaisMkt['nomecanalmis'],
                                 df_depara_adobe['no_gerencia'].alias('nrgerencia'),
                                 df_depara_adobe['gerencia'],
                                 df_depara_adobe['categoria'],
                                 df_depara_adobe['subcategoria'],
                                 df_depara_canaisMkt['visits'],
                                 df_depara_canaisMkt['pedidos'],
                                 df_depara_canaisMkt['receita'],
                                 df_depara_canaisMkt['unidades'],
                                 df_depara_canaisMkt['productview'],
                                 df_depara_canaisMkt['carrinhoadicionar'],
                                 df_depara_canaisMkt['checkout'],
                                 df_depara_canaisMkt['checkoutenderecolistar'],
                                 df_depara_canaisMkt['checkoutpagamento']
                                 )
                         .withColumn('reportsuitename',
                                     f.lit(f.substring(df_depara_canaisMkt['reportsuiteid'], 0, 8)).cast(StringType()))
                         .withColumn('hora2', udf_comum.udf_get_hour(f.lit(df_depara_canaisMkt['hour'])))
                         .withColumn('server', f.lit(None).cast(StringType()))
                         )

    return (df_canalmkt_final
            .select(df_canalmkt_final['id'],
                    df_canalmkt_final['createdat'],
                    df_canalmkt_final['reportsuiteid'],
                    f.trim(df_canalmkt_final['reportsuitename']).alias('reportsuitename'),
                    df_canalmkt_final['server'],
                    df_canalmkt_final['data2'].alias('data'),
                    f.trim(df_canalmkt_final['hora2']).alias('hour'),
                    df_canalmkt_final['iddepartamento'],
                    df_canalmkt_final['nomedepartamento'],
                    df_canalmkt_final['idlinha'],
                    df_canalmkt_final['nomelinha'],
                    df_canalmkt_final['idsublinha'],
                    df_canalmkt_final['nomesublinha'],
                    df_canalmkt_final['canalmis'],
                    df_canalmkt_final['nomecanalmis'],
                    df_canalmkt_final['nrgerencia'],
                    df_canalmkt_final['gerencia'],
                    df_canalmkt_final['categoria'],
                    df_canalmkt_final['subcategoria'],
                    df_canalmkt_final['visits'],
                    df_canalmkt_final['pedidos'],
                    df_canalmkt_final['receita'],
                    df_canalmkt_final['unidades'],
                    df_canalmkt_final['productview'],
                    df_canalmkt_final['carrinhoadicionar'],
                    df_canalmkt_final['checkout'],
                    df_canalmkt_final['checkoutenderecolistar'],
                    df_canalmkt_final['checkoutpagamento'],
                    df_canalmkt_final['data2'].alias('dataprocessamento')
                    )
            )


# from app_viaunica.page
def adobe_page(df_page, df_deparadepto):
    df_depara = (df_deparadepto
                 .filter(
        (df_deparadepto['tabela'] == 'departamento') & (df_deparadepto['campo'] == 'iddepartamento'))
                 .select(df_deparadepto['de'],
                         df_deparadepto['para']
                         )
                 )

    df_adb_page = (df_page
                   .join(df_depara, df_page['iddepartament'] == df_depara['de'], 'left')
                   .select(df_page['id'],
                           df_page['reportsuitename'],
                           df_page['data'],
                           df_page['createdat'],
                           df_page['iddepartament'].alias('iddepartamento'),
                           df_depara['para'].alias('nomedepartamento'),
                           df_page['pagename'],
                           df_page['visits'],
                           df_page['entries'],
                           df_page['exits'],
                           df_page['bounces']

                           )
                   .withColumn('reportsuiteid', f.lit(None).cast(StringType()))
                   .withColumn('server', f.lit(None).cast(StringType()))
                   )

    return (df_adb_page
            .select(df_adb_page['id'],
                    df_adb_page['reportsuiteid'],
                    df_adb_page['reportsuitename'],
                    df_adb_page['server'],
                    df_adb_page['data'],
                    df_adb_page['iddepartamento'],
                    df_adb_page['nomedepartamento'],
                    df_adb_page['pagename'],
                    df_adb_page['visits'],
                    df_adb_page['entries'],
                    df_adb_page['exits'],
                    df_adb_page['bounces'],
                    df_adb_page['createdat']
                    )
            )


# app_viaunica.termo != raw_parceiro.termo
def adobe_termo(df_termo):
    df_adb_termo = (df_termo
                    .select(df_termo['id'],
                            df_termo['reportsuitename'],
                            df_termo['data'],
                            df_termo['hour'],
                            df_termo['createdat'],
                            df_termo['termo'],
                            df_termo['quantidade']
                            )
                    .withColumn('reportsuiteid', f.lit(None).cast(StringType()))
                    .withColumn('server', f.lit(None).cast(StringType()))
                    )

    return (df_adb_termo
            .select(df_adb_termo['id'],
                    df_adb_termo['reportsuiteid'],
                    df_adb_termo['reportsuitename'],
                    df_adb_termo['server'],
                    df_adb_termo['data'],
                    df_adb_termo['hour'],
                    df_adb_termo['termo'],
                    df_adb_termo['quantidade'],
                    df_adb_termo['createdat']

                    )
            )


def adobe_conversaosku(df_conversaosku, df_deparadepto):
    df_depara = (df_deparadepto
                 .filter((df_deparadepto['tabela'] == 'departamento') & (df_deparadepto['campo'] == 'iddepartamento'))
                 .select(df_deparadepto['de'],
                         df_deparadepto['para']
                         )
                 .withColumn('reportsuiteid', f.lit(None).cast(StringType()))
                 .withColumn('server', f.lit(None).cast(StringType()))
                 )

    return (df_conversaosku
            .join(df_depara, df_conversaosku['iddepartament'] == df_depara['de'], 'left')
            .select(f.trim(df_conversaosku['id']).alias('id'),
                    f.trim(df_depara['reportsuiteid']).alias('reportsuiteid'),
                    f.trim(df_conversaosku['reportsuitename']).alias('reportsuitename'),
                    f.trim(df_depara['server']).alias('server'),
                    f.trim(df_conversaosku['data']).alias('data'),
                    f.trim(df_conversaosku['iddepartament']).alias('iddepartamento'),
                    f.trim(df_depara['para']).alias('nomedepartamento'),
                    f.trim(df_conversaosku['sku']).alias('sku'),
                    f.trim(df_conversaosku['orders']).alias('orders'),
                    f.trim(df_conversaosku['revenue']).alias('revenue'),
                    f.trim(df_conversaosku['visits']).alias('visits'),
                    f.trim(df_conversaosku['createdat']).alias('createdat')
                    )
            )


def adobe_traffic_by_channel(df_traffic_by_channel):
    return (df_traffic_by_channel
            .select(df_traffic_by_channel['id'],
                    df_traffic_by_channel['createdat'],
                    df_traffic_by_channel['reportsuiteid'],
                    df_traffic_by_channel['reportsuitename'],
                    df_traffic_by_channel['server'],
                    df_traffic_by_channel['data'],
                    df_traffic_by_channel['hour'],
                    df_traffic_by_channel['channel'],
                    df_traffic_by_channel['visits'],
                    df_traffic_by_channel['orders'],
                    df_traffic_by_channel['ordersserialized'],
                    df_traffic_by_channel['revenue']
                    )
            .withColumn('dataprocessamento', f.trunc(f.current_date(), 'month'))
            )


def adobe_traffic_by_department(df_traffic_by_department, df_deparadepto):
    df_depara = (df_deparadepto
                 .filter((df_deparadepto['tabela'] == 'departamento') & (df_deparadepto['campo'] == 'iddepartamento'))
                 .select(df_deparadepto['de'],
                         df_deparadepto['para']
                         )
                 )

    return (df_traffic_by_department
            .join(df_depara, df_traffic_by_department['iddepartament'] == df_depara['de'], 'left')
            .select(df_traffic_by_department['id'],
                    df_traffic_by_department['createdat'],
                    df_traffic_by_department['reportsuiteid'],
                    df_traffic_by_department['reportsuitename'],
                    df_traffic_by_department['server'],
                    df_traffic_by_department['data'],
                    df_traffic_by_department['hour'],
                    df_traffic_by_department['iddepartament'],
                    f.trim(df_depara['para']).alias('namedepartment'),
                    df_traffic_by_department['visits'],
                    df_traffic_by_department['orders'],
                    df_traffic_by_department['ordersserialized'],
                    df_traffic_by_department['revenue']
                    )
            .withColumn('dataprocessamento', f.trunc(f.current_date(), 'month'))
            )
