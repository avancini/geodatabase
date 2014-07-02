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

## Insere os gis de subpopulacoes e rodovias associadas
createSubpopulacaoRodovia(conn)



=begin
## Listar as espécies (apeas o id)
especies = getSpeciesId(conn)

y = especies.count - 1

for x in (0..y)
 puts "Especie - #{especies[x]} - #{x} de #{y}"
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


