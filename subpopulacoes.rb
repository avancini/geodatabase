## Arquivo de Cálculo de Subpopulações
#encoding: utf-8

require 'rubygems'
require 'pg'
require 'yaml'
require_relative 'functions'


## Conexão
yml= YAML.load_file("config.yml")
conn = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])


## Funcao para corrigir os remanescentes
#remanescentes = corrigeRemanescentes(conn)

## Insere os gids de subpopulacoes e rodovias associadas
#createSubpopulacaoRodovia(conn)

## Calcular as metricas dentro da subpopulacao
#calculoMetricasSubpopulacao(conn)

## Insere os dados de mineracao na tabela de relacionamento com a subpopulacao
#insereRelMineracao(conn)

## funcao para testar os poligonos de mineracao que estavam dando erro de topologia no ST_UNION
#testeMineracao(conn)

## Calcula a area total minerada da subpopulacao
=begin
subpop = getSubpopGid(conn)
for x in (0..subpop.count-1)
   puts "#{x+1} de #{subpop.count}"
   calculoAreaMinerada(conn,subpop[x])
end
=end


## Insere os relacionamentos de subpopulacao e ucs
#insereRelSubpopUcs(conn)

## calcula a area de remanescentes protegidos por ucs
=begin
	subpop = getSubpopGid(conn)
	for x in (0..subpop.count-1)
	   puts "#{x+1} de #{subpop.count}"
	   calculoAreaRemanescenteUcs(conn, subpop[x])
	end
=end


=begin
## Listar as espécies (apeas o id)
especies = getSpeciesId(conn)

## Remover espécies que não fazem parte do corredos central da Mata Atlântica
#removeEspecies(conn,especies)

y = especies.count - 1

for x in (0..y)
# puts "Especie - #{especies[x]} - #{x} de #{y}"
   ## Lista as ocorrências de uma espécie
#   ocorrencias = getOccurrenceRegisterById(conn,especies[x])

   ## Calcula a maior distância entre os pontos da espécie e divide por 10 = (raio)
#   raio =  getDistance(conn, ocorrencias)/10

   ## Cria os polígonos das subpopulações
#   insert = insertSubpopulacoes(conn,especies[x],raio)

   ## Cria o poligono de EOO
#   eoo = createEOO(conn,especies[x],ocorrencias)

   ## Cria o poligono de AOO
#   aoo = createAOO(conn,especies[x])

   ## Inserir dados da Flora do Brasil no banco de dados
#   fb = insertFB(conn)

   ## Corrigir dados de AOO (multipolygon to polygon)
#   corrigeGrid(conn)

   ## Corrigir dados de remanescentes
#   corrigeRemanescentes(conn)
   ## Inserir remanescentes na tabela remanescente_especie
#   insertRemanescentes(conn,especies[x])
end
=end



