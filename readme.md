# GEOFUSION  Processo Seletivo

##  Engenheiro de Dados

###  Desafio Técnico

### FEV/2019 - CGLSOFT - CLAUDIO FEV/2019

[![N|Solid](arquitetura.png)](https://github.com/cglsoft)


O objetivo da equipe GeoFusion é apresentar a solução de arquitetura de serviços escaláveis e otimizados em 
BigData para os clientes do setor de alimentação (restaurantes, pizzarias, bares, etc.).

Esta arquitetura poderá estar hospedadas nos serviços de nuvens dos fornecedores:
 - AWS Amazon 
 - Google Cloud
 
 
Com base no porte do cliente, poderão ser ofertados dois modelos de processamentos:

  - Batch - Online com NoSQL HIVE para atender estas demandas
  - Real-time com NoSQL para grandes volumes de dados com o HBASE/Cassandra

Para o processamento (ETL), serão utilizados as tecnologias opensource Apache Spark PYSPARK rodando
em cluster no Ambiente HADOOP.

Todo o processo de deploy e distribuição das soluções irão utilizar container LINUX/Docker nos serviços de 
nuvens acima citados.

A equipe comercial poderá oferecer esta arquitetura nos cenários :
 - Real-time, 
 - batch e Online. 

O Datalake com as informações dos clientes poderão ser consumidas via Micro Serviços, e ou
diretamente com as ferramentas de visualização de dados como Tableaut, Jupyter Notebook.
 

Esta solução irá entregar Micro Serviços REST API com a utilização das tecnologias Python 
com Flask, onde os parceiros poderão consumir os serviços conforme os planos contratados.

## Implantação da solução

### Modelagem Star schema para gravacao no Datalake




[![N|Solid](starchema.png)](https://github.com/cglsoft)

 


### Repositório GIT

Acessar o diretório de projetos e baixar o repositório do [GIT GEOFUSION DESAFIO](https://github.com/cglsoft/).

Passos para instalação:

```sh
$ git clone git clone https://github.com/cglsoft/gf_desafio.git
$ cd gf_desafio
```

### Deploy DOCKER Container PYSPARK/JUPYTERNOTEBOOK

Para rodar a solução o primeiro passo será criar executar o container em Docker/PYSPARK/JUPYTERNOTEBOOK
Acessar o diretório build_docker para e executar o script abaixo para a criação da imagem:

```sh
$ docker build -t jupyter_cgl:1.0 . 
```

Container Docker HUB o repositório do [HUB](https://hub.docker.com/).

Estando no diretório gf_desafio executar os comandos abaixo:

```sh
$ docker run -it -p 8888:8888 -v $PWD:/home/jovyan/work --name spark jupyter/pyspark-notebook 
```

Apos o deploy do serviço será fornecido o TOKEN que deverá ser copiado conforme tela abaixo:

[![N|Solid](tokenacesso.png)](http://127.0.0.1:8888)


No navegador de sua preferência, acessar o endereço do Jupyter Notebook: [Jupyter Notebook](http://127.0.0.1:8888).

```sh
127.0.0.1:8888
```

###Importante acessar a pasta work/gf_desafio

Nesta pasta temos o arquivo com o código ETL para carga das informações:

Veja [ETL Carga Datalake](https://github.com/cglsoft/gf_desafio/blob/master/GF%20Case.ipynb)


FIM













