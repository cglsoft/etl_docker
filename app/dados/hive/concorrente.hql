drop table if exists app_gf.concorrente;

CREATE EXTERNAL TABLE app_gf.concorrente(
id string,
createdat timestamp,
reportsuiteid string,
reportsuitename string,
server string,
data date,
hour int,
iddepartamento int,
nomedepartamento string,
idlinha string,
nomelinha string,
idsublinha string,
nomesublinha string,
canalmis string,
nomecanalmis string,
nrgerencia int,
gerencia string,
categoria string,
subcategoria string,
visits int,
pedidos int,
receita decimal(16,4),
unidades int,
productview int,
carrinhoadicionar int,
checkout int,
checkoutenderecolistar int,
checkoutpagamento int)
PARTITIONED BY (dataprocessamento date)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'/app/viaunica/concorrente/'
TBLPROPERTIES ('orc.compress' = 'ZLIB');