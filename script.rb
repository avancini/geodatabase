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
createAnalisysTables(conn)


## Listar as espécies (id) e executa todos os cálculos marcando o tempo de cada operação
especies = getSpeciesId(conn)
for x in (0..especies.count-1)
	puts "     ----------     Especie - #{especies[x]} - #{x+1} de #{especies.count}     ----------"
	
	insertIdTempos(conn,especies[x])

	insertEoo(conn, especies[x])
	insertAoo(conn, especies[x])

	insertSubpopulacoes(conn, especies[x])

	insertRemanescentes(conn,especies[x])
	insertSubpopRod(conn,especies[x])
	insertSubpopMin(conn,especies[x])
	insertSubpopUc(conn,especies[x])
	#insertSubpopTi(conn,especies[x])

	calculateMetrics(conn,especies[x])
end
