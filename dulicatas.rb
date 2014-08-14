## Arquivo para remover espécies com menos de 3 ocorrências distintas
#encoding: utf-8

require 'rubygems'
require 'pg'
require 'yaml'
require_relative 'functions'

yml= YAML.load_file("config.yml")
conn = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])



## Listar as espécies do sistema
def getSpeciesId(conn)
	especies = []
	#menos = 0
	#mais = 0
	conn.exec("select id from geo.especies order by Id;").each do |row|
		especies.push(row['id'])
	end

	for x in (0..especies.count-1)
		conn.exec("select count(distinct(geom)) as total from geo.ocorrencias where id = #{especies[x]};").each do |row|
			#puts "#{especies[x]} = #{row['total']}"
			if row['total'].to_i > 2 then
				#mais += 1
		
			else
				#menos += 1 
				conn.exec("Delete from geo.ocorrencias where id = #{especies[x]};") 
				conn.exec("Delete from geo.especies where id = #{especies[x]};")	
			end
		end
	end
	#puts mais
	#puts menos
end

#getSpeciesId(conn)


## deleta arquivos de remanescentes
def deletaArquivos1(conn)
        gid = []
        conn.exec("select gid from geo.remanescentes;").each do |row|
                gid.push(row['gid'])
        end

        for x in (0..gid.count-1)
                conn.exec("delete from geo.remanescentes where ((not st_intersects(st_setsrid(geom,4326), (select st_setsrid(st_union(geom),4326) from geo.estados where gid in (2, 5, 3, 10, 15, 9)))) and (gid = #{gid[x]}));")
	puts "remanescente - #{x} de #{gid.count-1}"
        end
end

deletaArquivos1(conn)

## deleta arquivos de rodovia
def deletaArquivos2(conn)
        gid = []
        conn.exec("select gid from geo.rodovias;").each do |row|
                gid.push(row['gid'])
        end

        for x in (0..gid.count-1)
                conn.exec("delete from geo.rodovias where ((not st_intersects(st_setsrid(geom_buffer,4326), (select st_setsrid(st_union(geom),4326) from geo.estados where gid in (2, 5, 3, 10, 15, 9)))) and (gid = #{gid[x]}));")
        puts "rodovia - #{x} de #{gid.count-1}"
        end
end

deletaArquivos2(conn)

## deleta arquivos de mineração
def deletaArquivos3(conn)
        gid = []
        conn.exec("select gid from geo.mineracao;").each do |row|
                gid.push(row['gid'])
        end

        for x in (0..gid.count-1)
                conn.exec("delete from geo.mineracao where ((not st_intersects(st_setsrid(geom,4326), (select st_setsrid(st_union(geom),4326) from geo.estados where gid in (2, 5, 3, 10, 15, 9)))) and (gid = #{gid[x]}));")
        puts "mineracao - #{x} de #{gid.count-1}"
        end
end

deletaArquivos3(conn)

## deleta arquivos de grid
def deletaArquivos4(conn)
        gid = []
        conn.exec("select gid from geo.grid;").each do |row|
                gid.push(row['gid'])
        end

        for x in (0..gid.count-1)
                conn.exec("delete from geo.grid where ((not st_intersects(st_setsrid(geom,4326), (select st_setsrid(st_union(geom),4326) from geo.estados where gid in (2, 5, 3, 10, 15, 9)))) and (gid = #{gid[x]}));")
        puts "grid - #{x} de #{gid.count-1}"
        end
end

deletaArquivos4(conn)

## deleta arquivos de ucs
def deletaArquivos5(conn)
        gid = []
        conn.exec("select gid from geo.ucs;").each do |row|
                gid.push(row['gid'])
        end

        for x in (0..gid.count-1)
                conn.exec("delete from geo.ucs where ((not st_intersects(st_setsrid(geom,4326), (select st_setsrid(st_union(geom),4326) from geo.estados where gid in (2, 5, 3, 10, 15, 9)))) and (gid = #{gid[x]}));")
        puts "uc - #{x} de #{gid.count-1}"
        end
end

deletaArquivos5(conn)

## deleta arquivos de terra_indigena
def deletaArquivos6(conn)
        gid = []
        conn.exec("select gid from geo.terra_indigena;").each do |row|
                gid.push(row['gid'])
        end

        for x in (0..gid.count-1)
                conn.exec("delete from geo.terra_indigena where ((not st_intersects(st_setsrid(geom,4326), (select st_setsrid(st_union(geom),4326) from geo.estados where gid in (2, 5, 3, 10, 15, 9)))) and (gid = #{gid[x]}));")
        puts "terra_indigena - #{x} de #{gid.count-1}"
        end
end

deletaArquivos6(conn)

## deleta arquivos de municipios
def deletaArquivos7(conn)
        gid = []
        conn.exec("select gid from geo.municipios;").each do |row|
                gid.push(row['gid'])
        end

        for x in (0..gid.count-1)
                conn.exec("delete from geo.municipios where ((not st_intersects(st_setsrid(geom,4326), (select st_setsrid(st_union(geom),4326) from geo.estados where gid in (2, 5, 3, 10, 15, 9)))) and (gid = #{gid[x]}));")
        puts "municipio - #{x} de #{gid.count-1}"
        end
end

deletaArquivos7(conn)

















