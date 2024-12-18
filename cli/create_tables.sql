CREATE SCHEMA hive.trusted WITH (location='s3a://ppgti/trusted/');

CREATE TABLE hive.trusted.indicadores (
  localidade  varchar,
  res         map(varchar, int),
  sigla       varchar,
  nome        varchar
) WITH (
  external_location='s3a://ppgti/trusted/', --caminho da pasta que está o arquivo.
  format='AVRO'
);
