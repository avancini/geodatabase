## Arquivo que cria as tebalas e realiza os cálculo para cada espécie
#encoding: utf-8

require 'rubygems'
require 'pg'
require 'yaml'
require_relative 'functions'


## Conexão
yml= YAML.load_file("config.yml")
conn = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])


## Função que cria as tabelas das espécies e ocorrências
#createTaxonTables(conn)

## Função que cria as tebelas para armazenar os resultados
#createAnalisysTables(conn)


## Listar as espécies (id) e executa todos os cálculos marcando o tempo de cada operação
especies = getSpeciesId(conn)

for x in (0..especies.count-1)
	puts "Especie - #{especies[x]} - #{x+1} de #{especies.count}"
	
#	insertSubpopulacoes(conn, especies[x])
#createEOO(conn, especies[x])
createAOO(conn, especies[x])


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




