## Arquivo de importacao dos registros de ocorrencias
#encoding: utf-8

require 'rubygems'
require 'pg'
require 'yaml'
require_relative 'functions'

yml= YAML.load_file("config.yml")
conn = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])

conn2 = PG::Connection.new(yml["ip2"], yml["port2"], nil, nil, yml["database2"], yml["user2"], yml["password2"])
id = []
conn.exec("select id from geo.especies").each do |row|
id.push(row['id'])
end


for x in (0..id.count-1)
codigocncflora = []
coordenadas = []

	puts "------------------------------------ especie #{id[x]} ----------------------------------------"
	conn2.exec("select codigocncflora, id, st_astext(coordenadas) as coordenadas from registros where id = #{id[x]}").each do |row|
		#codigocncflora.push(row['codigocncflora'])
		coordenadas.push(row['coordenadas'])
	end
	coordenadas.uniq.each do |coordenadas|
		conn.exec("insert into geo.ocorrencias(id, geom) values (#{id[x]}, st_transform(st_geomfromtext('#{coordenadas}',4326), 102033))")
        		puts "ocorrencia #{coordenadas}"
	end
end






