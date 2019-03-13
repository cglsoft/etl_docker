Case #01 - Modelagem de dados

A equipe de produtos da Geofusion tem recebido feedbacks da equipe
comercial e eles dizem que alguns clientes do setor de alimentação
(restaurantes, pizzarias, bares, etc.) estão procurando soluções que os
ajudem a entender melhor seus concorrentes. Eles gostariam de saber
qual é a faixa de preço praticada pelos concorrentes, como é o fluxo de
pessoas nesses locais, qual é a população e a densidade demográfica dos
bairros onde os concorrentes estão, e etc.
A Geofusion decidiu investir no desenvolvimento de uma solução para
resolver os problemas desses clientes. A intenção da empresa é inserir
esta solução nos produtos já existentes.
Atualmente um dos nossos desafios é criar serviços de dados escaláveis,
otimizados para o armazenamento e para o acesso aos dados. Diante disso,
seu primeiro desafio será definir um modelo de dados que otimize o
armazenamento e o acesso a estes dados.
Como resultado, você deve enviar uma descrição da arquitetura de dados
proposta, incluindo o(s) modelo(s) de dados e as tecnologias sugeridas.
Você precisará enviar um documento no formato pdf.
Os parágrafos a seguir irão te contextualizar quanto as regras de negócio e
os arquivos que devem ser utilizados durante o desenvolvimento.

Regras de negócio:

Nossos Analistas analisaram os dados de fluxo de pessoas e concluíram
que a melhor forma de apresentar essa informação seria segmentando o
fluxo por dias da semana e períodos do dia (manhã, tarde e noite), ou seja,
os clientes precisam saber quantas pessoas em média frequentam seus
concorrentes em cada dia da semana e em cada período do dia.

Para encontrar fluxo médio de pessoas é preciso considerar os eventos dos
mesmos dias da semana e dos mesmos períodos do dia.

A densidade demográfica de um bairro é uma informação muito
importante para nossos clientes e é uma informação que precisa ser
calculada. A densidade demográfica de um bairro é o resultado da divisão
da população do bairro pela área do bairro.

Arquivos:

Os arquivos que você deve utilizar durante o desenvolvimento são de
diferentes fontes, da Geofusion, do IBGE e de um parceiro e foram
enviados em anexo. Segue a descrição de cada um:

eventos_de_fluxo.csv: contém os dados do fluxo de pessoas. São
eventos registrados a partir dos celulares de pessoas que
permanecem mais de 5 minutos em um estabelecimento comercial.
Esses dados são enviados diariamente para a Geofusion. Este
arquivo possui uma pequena amostra dos dados (248.590 de
eventos), entretanto, o volume real passa dos 15 milhões de eventos.
O arquivo possui 3 atributos:

populacao.json: contém a quantidade de habitantes por bairro. Esse
arquivo contém 2 atributos:

bairros.csv: contém as informações dos bairros. Esse arquivo
contém 5 atributos:

concorrentes.csv : contém os dados de concorrentes. Esse arquivo
contém 8 atributos:

