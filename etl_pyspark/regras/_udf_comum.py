# -*- coding: utf-8 -*-

import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime

"""
    UDF = user defined function
    
    Arquivo para criar funcoes que retornam uma udf usando outra funcao python
"""

# UDF Periodo do dia
def get_periodo_dia(hour):
    return (
        "(1)manha" if 5 <= hour <= 11
        else
        "(2)tarde" if 12 <= hour <= 17
        else
        "(3)noite" if 18 <= hour <= 22
        else
        "(4)madrugada"
    )
udf_get_periodo_dia = udf(get_periodo_dia, StringType())