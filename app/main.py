# Projeto GF - CLAUDIO 12/03/2019
# -*- coding: utf-8 -*-

import sys, os


from dados import dados_io
from gerenciadores import gerenciador

# Base MOCK - DataLake
_producao = False

# Parametrizacao PROCESSAMENTO CSV
# 0 - Linux
# 1 - Windows

if not _producao:
    _dados_io = dados_io.MockDadosIO('GF', 0)
else:
    _dados_io = dados_io.ProdDadosIO('GF')

_main_gerenciador = gerenciador.Gerenciador(_dados_io)
_main_gerenciador.validacoes()