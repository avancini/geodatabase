## Arquivo de leitura de CSV
## Cria um vetor de hash (id / familia/ nome)

#encoding: utf-8
require 'csv'
require 'pg'
require 'rubygems'
require 'fileutils'

conn = PG::Connection.new( "192.168.33.20", 5432, nil, nil, 'mbti', 'cncflora', 'cncflora')


# Inclusão de espécies
#conn.prepare("insert_especies", "insert into geo.especies (id, familia, genero, especie, tipo, infranome, autor) values ($1, $2, $3, $4, $5, $6, $7)")

#b = 0

#CSV.foreach('tmp/especies_final.csv', :col_sep => ";", :headers => true) do |row|
#	espeecie = Hash[row.headers.zip(row.fields)]

#	conn.exec_prepared("insert_especies", [
#	espeecie["id"], espeecie["familia"], espeecie["genero"], espeecie["especie"], espeecie["tipo"], espeecie["infranome"], espeecie["autor"]])
#b+=1
#puts b
#end


b = 0

# Inclusão de ocorrencias
conn.prepare("insert_ocorrencias", "insert into geo.ocorrencias (codigoCncflora, id, codigoColecao, familia, genero, especie, tipo, infranome, numeroCatalogo, numeroColetor, coletor, anoColeta, mesColeta, diaColeta, determinador, estado, municipio, localidade, longitude, latitude, longCncflora, latCncflora, precCncflora, metodoCncflora, obsCncflora, revisado, valido, longitudeGis, latitudeGis) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)")

b = 0

CSV.foreach('tmp/ocorrencias_final_utf8.csv', :col_sep => ";", :headers => true) do |row|
	ocorrencia = Hash[row.headers.zip(row.fields)]

	conn.exec_prepared("insert_ocorrencias", [
	ocorrencia["codigocncflora"], ocorrencia["id"], ocorrencia["codigocolecao"], ocorrencia["familia"], ocorrencia["genero"], ocorrencia["especie"], ocorrencia["tipo"], ocorrencia["infranome"], ocorrencia["numerocatalogo"], ocorrencia["numerocoletor"], ocorrencia["coletor"], ocorrencia["anocoleta"], ocorrencia["mescoleta"], ocorrencia["diacoleta"], ocorrencia["determinador"], ocorrencia["estado"], ocorrencia["municipio"], ocorrencia["localidade"], ocorrencia["longitude"].gsub(",","."), ocorrencia["latitude"].gsub(",","."), ocorrencia["longcncflora"].gsub(",","."), ocorrencia["latcncflora"].gsub(",","."), ocorrencia["preccncflora"], ocorrencia["metodocncflora"], ocorrencia["obscncflora"], ocorrencia["revisado"], ocorrencia["valido"], ocorrencia["longitudegis"].gsub(",","."), ocorrencia["latitudegis"].gsub(",",".")
	])
b+=1
puts b
end





