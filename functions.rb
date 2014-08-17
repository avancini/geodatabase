## ALL FUNCTIONS SCRIPT
#encoding: utf-8
require 'rubygems'
require 'pg'
require 'yaml'
require_relative 'functions'





## CREATE TAXON TABLES (SPECIES AND OCCURRENCES) !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def createTaxonTables(conn)

	conn.exec("DROP TABLE geo.ocorrencias;")
	conn.exec("DROP TABLE geo.especies;")


	## Table: geo.especies
	conn.exec("CREATE TABLE geo.especies
			(id integer NOT NULL,
			  familia character varying(50) DEFAULT NULL::character varying,
			  genero character varying(50) DEFAULT NULL::character varying,
			  especie character varying(50) DEFAULT NULL::character varying,
			  tipo character varying(50) DEFAULT NULL::character varying,
			  infranome character varying(50) DEFAULT NULL::character varying,
			  autor character varying(100) DEFAULT NULL::character varying,
			  id_fb character varying(255),
			  lifeform_fb character varying(255),
			  CONSTRAINT especies_pkey PRIMARY KEY (id));")
	conn.exec("ALTER TABLE geo.especies OWNER TO cncflora;")


	## Table: geo.ocorrencias
	conn.exec("CREATE TABLE geo.ocorrencias
			(codigocncflora integer NOT NULL,
			  id integer,
			  codigocolecao character varying(25) DEFAULT NULL::character varying,
			  familia character varying(50) DEFAULT NULL::character varying,
			  genero character varying(50) DEFAULT NULL::character varying,
			  especie character varying(50) DEFAULT NULL::character varying,
			  tipo character varying(10) DEFAULT NULL::character varying,
			  infranome character varying(50) DEFAULT NULL::character varying,
			  numerocatalogo character varying(15) DEFAULT NULL::character varying,
			  numerocoletor character varying(15) DEFAULT NULL::character varying,
			  coletor character varying(255) DEFAULT NULL::character varying,
			  anocoleta character varying(10) DEFAULT NULL::character varying,
			  mescoleta character varying(10) DEFAULT NULL::character varying,
			  diacoleta character varying(10) DEFAULT NULL::character varying,
			  determinador character varying(255) DEFAULT NULL::character varying,
			  estado character varying(255) DEFAULT NULL::character varying,
			  municipio character varying(255) DEFAULT NULL::character varying,
			  localidade text,
			  longitude double precision DEFAULT 0::double precision,
			  latitude double precision DEFAULT 0::double precision,
			  longcncflora double precision DEFAULT 0::double precision,
			  latcncflora double precision DEFAULT 0::double precision,
			  preccncflora character varying(50) DEFAULT NULL::character varying,
			  metodocncflora character varying(50) DEFAULT NULL::character varying,
			  obscncflora text,
			  revisado smallint DEFAULT 0::smallint,
			  valido smallint DEFAULT 0::smallint,
			  longitudegis double precision DEFAULT 0::double precision,
			  latitudegis double precision DEFAULT 0::double precision,
			  geom geometry(Point,4326),
			  CONSTRAINT ocorrencias_pkey PRIMARY KEY (codigocncflora),
			  CONSTRAINT ocorrencias_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.ocorrencias OWNER TO cncflora;")
end


## CREATE ANALISYS TABLES    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! (refazer apenas as consultas, colocando todos os drop table antes)
def createAnalisysTables(conn)
	

	## Table: geo.eoo
	conn.exec("DROP TABLE IF EXISTS geo.eoo;")
	conn.exec("CREATE TABLE geo.eoo(
			  gid serial NOT NULL,
			  id integer NOT NULL,
			  geom geometry(Polygon,4326),
			  CONSTRAINT eoo_pkey PRIMARY KEY (id),
			  CONSTRAINT eoo_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.eoo OWNER TO cncflora;")



	## Table: geo.aoo
        conn.exec("DROP TABLE IF EXISTS geo.aoo;")
        conn.exec("CREATE TABLE geo.aoo
                        (gid serial NOT NULL,
                          id integer,
                          geom geometry(Polygon,4326),
                          CONSTRAINT aoo_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
        conn.exec("ALTER TABLE geo.aoo OWNER TO cncflora;")



	## Table: geo.subpopulacoes
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_remanescente;")
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_rodovia;")
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_mineracao;")
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_uc;")
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_terra_indigena;")

	conn.exec("DROP TABLE IF EXISTS geo.subpopulacoes;")
	conn.exec("CREATE TABLE geo.subpopulacoes(
			  gid serial NOT NULL,
			  id integer,
			  geom geometry(Polygon,4326),
			  area_total double precision DEFAULT 0.0,			  
			  area_remanescente double precision DEFAULT 0.0,
			  area_rodovia double precision DEFAULT 0.0,
			  remanescentes_sob_rodovia double precision DEFAULT 0.0,
			  area_minerada double precision DEFAULT 0.0,			  
			  area_uc double precision DEFAULT 0.0,
			  area_remanescente_uc double precision DEFAULT 0.0,
			  area_terra_indigena double precision DEFAULT 0.0,
			  area_remanescente_terra_indigena double precision DEFAULT 0.0,
			  porcentagem_remanescente double precision DEFAULT 0.0,
			  porcentagem_rodovia double precision DEFAULT 0.0,
			  porcentagem_minerada double precision DEFAULT 0.0,
			  porcentagem_remanescente_rodovia double precision DEFAULT 0.0,
			  porcentagem_remanescente_uc double precision DEFAULT 0.0,
			  porcentagem_remanescente_terra_indigena double precision DEFAULT 0.0,
			  CONSTRAINT subpopulacoes_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacoes_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.subpopulacoes OWNER TO cncflora;")



	## Table: geo.subpopulacao_remanescente (relacionamento entre subpopulação, remanescentes e espécies. Antiga tabela remanescente_especie)
	conn.exec("CREATE TABLE geo.subpopulacao_remanescente(
			  gid serial,
			  gid_subpop integer,
			  gid_remanescente integer,
			  id integer NOT NULL,
			  CONSTRAINT subpopulacao_remanescente_gid_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_remanescente_gid_remanescente_fkey FOREIGN KEY (gid_remanescente) REFERENCES geo.remanescentes (gid),
			  CONSTRAINT subpopulacao_remanescente_subpop_gid_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid),
			  CONSTRAINT subpopulacao_remanescente_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.subpopulacao_remanescente OWNER TO cncflora;")



	## Table: geo.subpopulacao_rodovia
	conn.exec("CREATE TABLE geo.subpopulacao_rodovia(
			  gid serial NOT NULL,
			  gid_subpop integer,
			  gid_rod integer,
			  CONSTRAINT subpopulacao_rodovia_gid_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_rodovia_gid_rod_fkey FOREIGN KEY (gid_rod) REFERENCES geo.rodovias (gid),
			  CONSTRAINT subpopulacao_rodovia_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid));")
	conn.exec("ALTER TABLE geo.subpopulacao_rodovia OWNER TO cncflora;")



	## Table: geo.subpopulacao_mineracao
	conn.exec("CREATE TABLE geo.subpopulacao_mineracao(
			  gid serial NOT NULL,
			  gid_subpop integer,
			  gid_mineracao integer,
			  CONSTRAINT subpopulacao_mineracao_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_mineracao_gid_mineracao_fkey FOREIGN KEY (gid_mineracao) REFERENCES geo.mineracao (gid),
			  CONSTRAINT subpopulacao_mineracao_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid));")
	conn.exec("ALTER TABLE geo.subpopulacao_mineracao OWNER TO cncflora;")



	## Table: geo.subpopulacao_uc
	conn.exec("CREATE TABLE geo.subpopulacao_uc(
			  gid serial NOT NULL,
			  gid_subpop integer,
			  gid_uc integer,
			  CONSTRAINT subpopulacao_uc_gid_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_uc_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid),
			  CONSTRAINT subpopulacao_uc_gid_uc_fkey FOREIGN KEY (gid_uc) REFERENCES geo.ucs (gid));")
	conn.exec("ALTER TABLE geo.subpopulacao_uc OWNER TO cncflora;")



        ## Table: geo.subpopulacao_terra_indigena
        conn.exec("CREATE TABLE geo.subpopulacao_terra_indigena(
                          gid serial NOT NULL,
                          gid_subpop integer,
                          gid_terra_indigena integer,
                          CONSTRAINT subpopulacao_terra_indigena_gid_pkey PRIMARY KEY (gid),
                          CONSTRAINT subpopulacao_terra_indigena_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid),
                          CONSTRAINT subpopulacao_terra_indigena_gid_uc_fkey FOREIGN KEY (gid_terra_indigena) REFERENCES geo.terra_indigena (gid));")
        conn.exec("ALTER TABLE geo.subpopulacao_terra_indigena OWNER TO cncflora;")
	puts "CREATE TABLE - OK"
end


## Lista de espécies (id)    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def getSpeciesId(conn)
	especies = []
	conn.exec("select id from geo.especies where id in (5508, 6271, 6606, 7934, 7853, 8423, 4089) order by id;").each do |row|
		especies.push(row['id'])
	end
return especies
end



## Lista ocorrencias (codigocncflora)      !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def getOcorrenciasById(conn,id)
	ocorrencias = []
	conn.exec("select codigocncflora from geo.ocorrencias where id = #{id}").each do |row|
		ocorrencias.push(row['codigocncflora'])
	end
return ocorrencias
end



## Criar poligono de EOO no postgis    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def insertEoo(conn, id)
	conn.exec("insert into geo.eoo(id, geom) VALUES(#{id}, (select ST_ConvexHull(ST_Collect(ST_SetSrid(geom,4326))) from geo.ocorrencias where id =#{id}));")
end



## Criar poligono de AOO no postgis    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def insertAoo(conn, id)
	poligono = ""
	conn.exec("select st_astext(b.geom) from geo.grid b inner join geo.ocorrencias a on st_intersects(b.geom, a.geom) where a.id = #{id} group by a.id, b.geom;").each do |row|
        	poligono = (row['st_astext'])
		conn.exec("insert into geo.aoo(id,geom) values (#{id}, (st_geomfromtext(\'#{poligono}\',4326)));")
	end
#       	conn.exec("insert into geo.aoo(id,geom) values (#{id}, (st_geomfromtext(\'#{poligono}\',4326)));")
end



## Criar poligono de subpopulacoes no postgis   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def insertSubpopulacoes(conn, id)
	distancia = 0
	ocorrencias = getOcorrenciasById(conn,id)
	conn.exec("select codigocncflora from geo.ocorrencias where id = #{id}").each do |row|
		ocorrencias.push(row['codigocncflora'])
	end
	for x in (0..ocorrencias.count-1)
		b = (x + 1)
		for y in (b..ocorrencias.count-1)
			conn.exec("select st_distance_Sphere(
					(select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[x]}),
					(select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[y]})) as meters;").each do |row|
				temp = row['meters'].to_f
				if (temp > distancia) then
					distancia = temp
				end
			end
         	end
	end
	distancia = (distancia/10)
   conn.exec("select st_astext(ST_Buffer_Meters((select ST_Union(geom) from geo.ocorrencias where id = #{id}), #{distancia}));") do |result|
      result.each do |row|
         temp = (row['st_astext'])
          temp = temp.delete "MULTIPOLYGON"
          temp = temp.delete "POLYGON"
          poligonos_temp = temp.split("),(")

          subpopulacoes = []
          y = poligonos_temp.count - 1
          for x in (0..y)
             subpopulacoes[x] = "POLYGON((#{poligonos_temp[x].delete "()"}))"
             conn.exec("INSERT INTO geo.subpopulacoes(id, geom) VALUES (#{id}, (select st_geomFromText(' #{subpopulacoes[x]}',4326)));")
          end
      end
   end
end



## Lista de subpopulacoes (por espécie - id)    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def getSubpopById(conn,id)
	subpop = []
		conn.exec("select gid from geo.subpopulacoes where id = #{id} order by gid;").each do |row|
			subpop.push(row['gid'])
		end
return subpop
end



## Lista todas as subpopulacoes   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def getSubpopGid(conn)
	subpop = []
        conn.exec("select gid from geo.subpopulacoes order by gid;").each do |row|
                subpop.push(row['gid'])
        end
return subpop
end



## Inserir dados de REMANESCENTES na tabela SUBPOPULACAO_REMANESCENTE   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def insertRemanescentes(conn,id)
	gid_subpop = getSubpopById(conn,id)
	for x in (0..gid_subpop.count-1)
		puts "Subpopulacao #{x+1} de #{gid_subpop.count}"
		conn.exec("SELECT distinct(gid) as gid_rem from geo.remanescentes where st_intersects(geom,(select geom from geo.subpopulacoes where gid = #{gid_subpop[x]} and id = #{id})) and legenda = 'Mata';").each do |row|
			if (row['gid_rem']) then
				conn.exec("insert into geo.subpopulacao_remanescente(gid_subpop, gid_remanescente, id) values (#{gid_subpop[x]}, #{row['gid_rem']}, #{id});")
			end
		end
	end
end



## Insere dados de RODOVIAS de uma subpopulacao na tabela SUBPOPULACAO_RODOVIA    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def insertSubpopRod(conn,id)
	gid_subpop = []
	conn.exec("select gid from geo.subpopulacoes where id = #{id};").each do |row|
		gid_subpop.push(row['gid'])
	end
	for x in (0..gid_subpop.count-1) 
		puts "subpopulacao #{x+1} de #{gid_subpop.count}"
		conn.exec("select gid from geo.rodovias where st_intersects(geom_buffer, (select geom from geo.subpopulacoes where gid = #{gid_subpop[x]}))").each do |row|
			if row['gid'] then
				conn.exec("insert into geo.subpopulacao_rodovia(gid_subpop, gid_rod) values (#{gid_subpop[x]}, #{row['gid']})")
			end			
		end
	end
end



## Insere dados de MINERACAO de uma subpopulacao na tabela SUBPOPULACAO_MINERACAO    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def insertSubpopMin(conn,id)
        gid_subpop = []
        conn.exec("select gid from geo.subpopulacoes where id = #{id};").each do |row|
                gid_subpop.push(row['gid'])
        end
        for x in (0..gid_subpop.count-1)
                puts "subpopulacao #{x+1} de #{gid_subpop.count}"
                conn.exec("select gid from geo.mineracao where st_intersects(geom, (select geom from geo.subpopulacoes where gid = #{gid_subpop[x]}))").each do |row|
			if (row['gid']) then               
				conn.exec("insert into geo.subpopulacao_mineracao(gid_subpop, gid_mineracao) values (#{gid_subpop[x]}, #{row['gid']})")
			end
                end
        end
end



## Insere dados de UC de uma subpopulacao na tabela SUBPOPULACAO_UC    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def insertSubpopUc(conn,id)
        gid_subpop = []
        conn.exec("select gid from geo.subpopulacoes where id = #{id};").each do |row|
                gid_subpop.push(row['gid'])
        end
        for x in (0..gid_subpop.count-1)
                puts "subpopulacao #{x+1} de #{gid_subpop.count}"
                conn.exec("select gid from geo.ucs where st_intersects(st_setsrid(geom,4326), (select st_setsrid(geom,4326) from geo.subpopulacoes where gid = #{gid_subpop[x]}))").each do |row|
			if (row['gid']) then
				conn.exec("insert into geo.subpopulacao_uc(gid_subpop, gid_uc) values (#{gid_subpop[x]}, #{row['gid']})")

			end
                end
        end
end



## Insere dados de TERRA_INDIGENA de uma subpopulacao na tabela SUBPOPULACAO_TERRA_INDIGENA    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def insertSubpopTi(conn,id)
        gid_subpop = []
        conn.exec("select gid from geo.subpopulacoes where id = #{id};").each do |row|
                gid_subpop.push(row['gid'])
        end
        for x in (0..gid_subpop.count-1)
                puts "subpopulacao #{x+1} de #{gid_subpop.count}"
                conn.exec("select gid from geo.terra_indigena where st_intersects(st_setsrid(geom,4326), (select st_setsrid(geom,4326) from geo.subpopulacoes where gid = #{gid_subpop[x]}))").each do |row|
			if (row['gid']) then
				conn.exec("insert into geo.subpopulacao_terra_indigena(gid_subpop, gid_terra_indigena) values (#{gid_subpop[x]}, #{row['gid']})")
			end        
                end
        end
end



def calculateMetrics(conn,id)
	gid_subpop = []
	conn.exec("select gid from geo.subpopulacoes where id = #{id} and gid in (select gid_subpop from geo.subpopulacao_remanescente where id = #{id});").each do |row|
		gid_subpop.push(row['gid'])
	end



	for x in (0..gid_subpop.count-1)
		puts "subpopulacao #{x+1} de #{gid_subpop.count}"



		## Calculo da área total da subpopulacao   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		conn.exec("update geo.subpopulacoes set area_total = st_area(geom)*10000 where gid = #{gid_subpop[x]};")


		
		## Calculo da area_remanescente da subpopulacao   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		conn.exec("select st_area(st_intersection(geom, (select st_union(geom) from geo.remanescentes where gid in (select gid_remanescente from geo.subpopulacao_remanescente where gid_subpop = #{gid_subpop[x]}))))*10000 as area_rem from geo.subpopulacoes where gid = #{gid_subpop[x]};").each do |row|
			if (row['area_rem']) then
				conn.exec("update geo.subpopulacoes set area_remanescente = #{row['area_rem']} where gid = #{gid_subpop[x]}")
				puts "area remanescente: #{row['area_rem']}"
			end
		end



		## Calculo da area_rodovia da subpopulacao   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		conn.exec("select st_area(st_intersection(geom, (select st_union(geom_buffer) from geo.rodovias where gid in (select gid_rod from geo.subpopulacao_rodovia where gid_subpop = #{gid_subpop[x]}))))*10000 as area_rod from geo.subpopulacoes where gid = #{gid_subpop[x]};").each do |row|
			if (row['area_rod']) then
				conn.exec("update geo.subpopulacoes set area_rodovia = #{row['area_rod']} where gid = #{gid_subpop[x]}")
				puts "area rodovia: #{row['area_rod']}"
			end
		end



		## Calculo da remanescentes_sob_rodovia da subpopulacao   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		conn.exec("select st_area(st_intersection(st_union(geom), (select st_union(geom_buffer) from geo.rodovias where gid in (select gid_rod from geo.subpopulacao_rodovia where gid_subpop = #{gid_subpop[x]}))))*10000 as rem_rod from geo.remanescentes where gid in (select gid_remanescente from geo.subpopulacao_remanescente where gid_subpop = #{gid_subpop[x]});").each do |row|
			if (row['rem_rod']) then
				conn.exec("update geo.subpopulacoes set remanescentes_sob_rodovia = #{row['rem_rod']} where gid = #{gid_subpop[x]}")
				puts "remanescente sob rodovia: #{row['rem_rod']}"
			end
		end



		## Calculo da area_uc da subpopulacao   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		conn.exec("select st_area(st_intersection(geom, (select st_setsrid(st_union(geom),4326) from geo.ucs where gid in (select gid_uc from geo.subpopulacao_uc where gid_subpop = #{gid_subpop[x]}))))*10000 as area_uc from geo.subpopulacoes where gid = #{gid_subpop[x]};").each do |row|
			if (row['area_uc']) then
				conn.exec("update geo.subpopulacoes set area_uc = #{row['area_uc']} where gid = #{gid_subpop[x]}")
				puts "area rodovia: #{row['area_uc']}"
			end
		end



		## Calculo da area_remanescente_uc da subpopulacao (area remanescente dentro de UC)   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		conn.exec("select st_area(st_intersection(st_setsrid(st_union(geom),4326), (select st_setsrid(st_union(geom),4326) from geo.ucs where gid in (select gid_uc from geo.subpopulacao_uc where gid_subpop = #{gid_subpop[x]}))))*10000 as rem_uc from geo.remanescentes where gid in (select gid_remanescente from geo.subpopulacao_remanescente where gid_subpop = #{gid_subpop[x]});").each do |row|
			if (row['rem_uc']) then
				conn.exec("update geo.subpopulacoes set area_remanescente_uc = #{row['rem_uc']} where gid = #{gid_subpop[x]}")
				puts "remanescente dentro de UC: #{row['rem_uc']}"
			end
		end



		## Calculo da area_terra_indigena da subpopulacao   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		conn.exec("select st_area(st_intersection(geom, (select st_setsrid(st_union(geom),4326) from geo.terra_indigena where gid in (select gid_terra_indigena from geo.subpopulacao_terra_indigena where gid_subpop = #{gid_subpop[x]}))))*10000 as area_ti from geo.subpopulacoes where gid = #{gid_subpop[x]};").each do |row|
			if (row['area_ti']) then
				conn.exec("update geo.subpopulacoes set area_terra_indigena = #{row['area_ti']} where gid = #{gid_subpop[x]}")
				puts "area terra indigena: #{row['area_ti']}"
			end
		end



		## Calculo da area_remanescente_terra_indigena da subpopulacao (area remanescente dentro de terra indigena)   !!!!!!!!!!!
		conn.exec("select st_area(st_intersection(st_union(geom), (select st_setsrid(st_union(geom),4326) from geo.terra_indigena where gid in (select gid_terra_indigena from geo.subpopulacao_terra_indigena where gid_subpop = #{gid_subpop[x]}))))*10000 as rem_ti from geo.remanescentes where gid in (select gid_remanescente from geo.subpopulacao_remanescente where gid_subpop = #{gid_subpop[x]});").each do |row|
			if (row['rem_ti']) then
				conn.exec("update geo.subpopulacoes set area_remanescente_terra_indigena = #{row['rem_ti']} where gid = #{gid_subpop[x]}")
				puts "remanescente dentro de terra indigena: #{row['rem_ti']}"
			end
		end




	end











	conn.exec("update geo.subpopulacoes set porcentagem_remanescente = ((area_remanescente / area_total) * 100) where id = #{id};")
	conn.exec("update geo.subpopulacoes set porcentagem_rodovia = ((area_rodovia / area_total) * 100) where id = #{id};")
	conn.exec("update geo.subpopulacoes set porcentagem_remanescente_rodovia = ((remanescentes_sob_rodovia / area_remanescente) * 100) where id = #{id} and area_remanescente <> 0;")
	conn.exec("update geo.subpopulacoes set porcentagem_remanescente_uc = ((area_remanescente_uc / area_remanescente) * 100) where id = #{id} and area_remanescente <> 0;")
	conn.exec("update geo.subpopulacoes set porcentagem_remanescente_terra_indigena = ((area_remanescente_terra_indigena / area_remanescente) * 100) where id = #{id} and area_remanescente <> 0;")





end

























































## Calcula as metricas de subpopulacao (vegetacao/efeito de rodovias)
def calculoMetricasSubpopulacao(conn)
subpop = []
   
##select st_area(st_union(st_intersection(geom, (select geom from geo.subpopulacoes where gid = 1))))*10000 from geo.remanescentes;
   conn.exec("select gid from geo.subpopulacoes where ((area_remanescente > 0) and (area_rodovia is null) and (gid in (select gid_subpop from geo.subpopulacao_rodovia))) order by gid;") do |result|
      result.each do |row|
         subpop.push(row['gid'])
      end
   end
   for x in (0..subpop.count-1)
      puts "subpopulacao #{subpop[x]}  -->  #{x+1} de #{subpop.count}"
#      conn.exec("update geo.subpopulacoes set area_remanescente = (select st_area(st_union(st_intersection(geom,(select geom from geo.subpopulacoes where gid = #{subpop[x]}))))*10000 from geo.remanescentes) where gid = #{subpop[x]};")

      conn.exec("update geo.subpopulacoes set area_rodovia = (select st_area(st_intersection(st_union(st_intersection(geom,(select geom from geo.subpopulacoes where gid = #{subpop[x]}))),(select st_union(geom_buffer) from geo.rodovias where geo.rodovias.gid in (select gid_rod from geo.subpopulacao_rodovia where gid_subpop = #{subpop[x]}))))*10000 from geo.remanescentes) where gid = #{subpop[x]};")



   end
end


## Insere os dados de mineracao e subpopulacao na tabela de relacionamento
def insereRelMineracao(conn)
subpop = []
   conn.exec("select gid from geo.subpopulacoes order by gid;") do |result|
      result.each do |row|
         subpop.push(row['gid'])
      end
   end
   for x in (0..subpop.count-1)
      puts "#{x+1} de #{subpop.count}" 
      mina = []
      conn.exec("select gid from geo.mineracao where st_intersects(st_setsrid(geom, 4326),(select geom from geo.subpopulacoes where gid = #{subpop[x]})) and (fase like '%LAVRA%' or fase like '%EXTRA%' or fase like '%DISPONIBILIDADE%')") do |result|
         result.each do |row|
            mina.push(row['gid'])
         end
         if mina.count > 0 then
            for y in (0..mina.count-1)
               #puts "   mina #{y+1} de #{mina.count}"
               conn.exec("insert into geo.subpopulacao_mineracao(gid_subpop, gid_mineracao) values (#{subpop[x]}, #{mina[y]});")
            end
         end
      end
   end
end

## Calcula a area minerada para cada subpopulacao
def calculoAreaMinerada(conn, subpop)
   conn.exec("update geo.subpopulacoes set area_minerada = (select st_area(st_intersection(geom, (select st_setsrid(st_union(geom), 4326) from geo.mineracao where gid in (select gid_mineracao from geo.subpopulacao_mineracao where gid_subpop = #{subpop}))))*10000 from geo.subpopulacoes where gid = #{subpop}) where gid = #{subpop};")
end




## Identificar os poligonos de mineração com erros de topologia para serem deletados pois estava dando um erro no ST_UNION
def testeMineracao(conn)
gid = []
   conn.exec("select gid from geo.mineracao order by gid;") do |result|
      result.each do |row|
         gid.push(row['gid'])
      end
   end
   for x in (0..gid.count-1)
      puts "#{gid[x]} = #{x+1} de #{gid.count}"
      conn.exec("select st_intersection(geom, (select st_setsrid(geom, 4326) from geo.estados where gid = 16)) from geo.mineracao where gid = #{gid[x]}")
   end
end


## Insere o relacionamento de subpop com UCs
def insereRelSubpopUcs(conn)
subpop = []
   conn.exec("select gid from geo.subpopulacoes order by gid;") do |result|
      result.each do |row|
         subpop.push(row['gid'])
      end
   end
   for x in (0..subpop.count-1)
      puts "#{x+1} de #{subpop.count}"
      ucs = []
      conn.exec("select gid from geo.ucs where st_intersects(st_setsrid(geom, 4326),(select geom from geo.subpopulacoes where gid = #{subpop[x]})) ") do |result|
         result.each do |row|
            ucs.push(row['gid'])
         end
         if ucs.count > 0 then
            for y in (0..ucs.count-1)
               #puts "   ucs #{y+1} de #{ucs.count}"
               conn.exec("insert into geo.subpopulacao_uc(gid_subpop, gid_uc) values (#{subpop[x]}, #{ucs[y]});")
            end
         end
      end
   end
end



## Calcula a area de remanescente dentro de UCs (((REFAZER)))
def calculoAreaRemanescenteUcs(conn, subpop)
   conn.exec("update geo.subpopulacoes set area_remanescente_uc = (

                 select st_area(st_intersection(st_setsrid(st_union(geom),4326),(select st_setsrid(st_union(geom),4326) from geo.ucs where st_intersects(st_setsrid(geom,4326), (select geom from geo.subpopulacoes where gid = #{subpop})))))*10000



from geo.remanescentes where st_intersects(geom, (select geom from geo.subpopulacoes where gid = #{subpop}))



) where subpopulacoes.gid = #{subpop};")

end







