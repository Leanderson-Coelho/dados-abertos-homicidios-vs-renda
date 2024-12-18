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

CREATE SCHEMA hive.refined WITH (location='s3a://ppgti/refined/');

CREATE TABLE hive.refined.renda_vs_homicidios (
  sigla varchar,
  ano varchar,
  renda int,
  homicidios int
) WITH (
  external_location='s3a://ppgti/refined/',
  format='AVRO'
);

-- select * from hive.refined.renda_vs_homicidios;
-- drop table hive.refined.renda_vs_homicidios;