## ALL FUNCTIONS SCRIPT
#encoding: utf-8
require 'rubygems'
require 'pg'
require 'yaml'
require_relative 'functions'


#conn.exec("")



## CREATE TAXON TABLES (SPECIES AND OCCURRENCES) !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def createTaxonTables(conn)

	## Table: geo.especies
	conn.exec("DROP TABLE geo.especies;")
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
	conn.exec("DROP TABLE geo.ocorrencias;")
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
	
	## Table: geo.aoo
	conn.exec("DROP TABLE IF EXISTS geo.aoo;")
	conn.exec("CREATE TABLE geo.aoo
			(gid serial NOT NULL,
			  id integer,
			  geom geometry(Polygon,4326),
			  CONSTRAINT aoo_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.aoo OWNER TO cncflora;")



	## Table: geo.eoo
	conn.exec("DROP TABLE IF EXISTS geo.eoo;")
	conn.exec("CREATE TABLE geo.eoo(
			  gid serial NOT NULL,
			  id integer NOT NULL,
			  geom geometry(Polygon,4326),
			  CONSTRAINT eoo_pkey PRIMARY KEY (id),
			  CONSTRAINT eoo_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.eoo OWNER TO cncflora;")



	## Table: geo.subpopulacoes
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacoes;")
	conn.exec("CREATE TABLE geo.subpopulacoes(
			  gid serial NOT NULL,
			  id integer,
			  geom geometry(Polygon,4326),
			  area_total double precision,			  
			  area_remanescente double precision,
			  area_rodovia double precision,
			  remanescentes_sob_rodovia double precision,
			  area_minerada double precision,			  
			  area_uc double precision,
			  area_remanescente_uc double precision,
			  area_terra_indigena double precision,
			  area_remanescente_terra_indigena double precision,
			  porcentagem_remanescente double precision,
			  porcentagem_rodovia double precision,
			  porcentagem_minerada double precision,
			  porcentagem_remanescente_rodovia double precision,
			  porcentagem_remanescente_uc double precision,
			  porcentagem_remanescente_terra_indigena double precision,
			  CONSTRAINT subpopulacoes_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacoes_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.subpopulacoes OWNER TO cncflora;")



	## Table: geo.subpopulacao_remanescente (relacionamento entre subpopulação, remanescentes e espécies. Antiga tabela remanescente_especie)
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_remanescente;")
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
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_rodovia;")
	conn.exec("CREATE TABLE geo.subpopulacao_rodovia(
			  gid serial NOT NULL,
			  gid_subpop integer,
			  gid_rod integer,
			  CONSTRAINT subpopulacao_rodovia_gid_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_rodovia_gid_rod_fkey FOREIGN KEY (gid_rod) REFERENCES geo.rodovias (gid),
			  CONSTRAINT subpopulacao_rodovia_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid));")
	conn.exec("ALTER TABLE geo.subpopulacao_rodovia OWNER TO cncflora;")



	## Table: geo.subpopulacao_mineracao
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_mineracao;")
	conn.exec("CREATE TABLE geo.subpopulacao_mineracao(
			  gid serial NOT NULL,
			  gid_subpop integer,
			  gid_mineracao integer,
			  CONSTRAINT subpopulacao_mineracao_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_mineracao_gid_mineracao_fkey FOREIGN KEY (gid_mineracao) REFERENCES geo.mineracao (gid),
			  CONSTRAINT subpopulacao_mineracao_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid));")
	conn.exec("ALTER TABLE geo.subpopulacao_mineracao OWNER TO cncflora;")



	## Table: geo.subpopulacao_uc
	conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_uc;")
	conn.exec("CREATE TABLE geo.subpopulacao_uc(
			  gid serial NOT NULL,
			  gid_subpop integer,
			  gid_uc integer,
			  CONSTRAINT subpopulacao_uc_gid_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_uc_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid),
			  CONSTRAINT subpopulacao_uc_gid_uc_fkey FOREIGN KEY (gid_uc) REFERENCES geo.ucs (gid));")
	conn.exec("ALTER TABLE geo.subpopulacao_uc OWNER TO cncflora;")



        ## Table: geo.subpopulacao_terra_indigena
        conn.exec("DROP TABLE IF EXISTS geo.subpopulacao_terra_indigena;")
        conn.exec("CREATE TABLE geo.subpopulacao_terra_indigena(
                          gid serial NOT NULL,
                          gid_subpop integer,
                          gid_terra_indigena integer,
                          CONSTRAINT subpopulacao_terra_indigena_gid_pkey PRIMARY KEY (gid),
                          CONSTRAINT subpopulacao_terra_indigena_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid),
                          CONSTRAINT subpopulacao_terra_indigena_gid_uc_fkey FOREIGN KEY (gid_terra_indigena) REFERENCES geo.ucs (gid));")
        conn.exec("ALTER TABLE geo.subpopulacao_terra_indigena OWNER TO cncflora;")

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



## Inserir dados de REMANESCENTES na tabela SUBPOPULACAO_REMANESCENTE
def insertRemanescentes(conn,id)
	gid_subpop = getSubpopById(conn,id)
	for x in (0..gid_subpop.count-1)
		puts "Subpopulacao #{x+1} de #{gid_subpop.count}"
#		conn.exec("SELECT s.gid, s.id as especie, r.gid as remanescente from geo.subpopulacoes s inner join geo.remanescentes r on st_intersects(s.geom,r.geom) where s.gid = #{gid_subpop[x]} and r.legenda = 'Mata' group by s.gid, especie, remanescente;").each do |row|
#		conn.exec("select distinct(gid) from geo.remanescentes where st_intersects(geom,(select st_union(geom) from geo.subpopulacoes where gid = #{gid_subpop[x]})) and legenda = 'Mata';").each do |row|
		conn.exec("SELECT distinct(gid) as gid_rem from geo.remanescentes where st_intersects(geom,(select geom from geo.subpopulacoes where gid = #{gid_subpop[x]} and id = #{id})) and legenda = 'Mata';").each do |row|
			if (row['gid_rem']) then
				conn.exec("insert into geo.subpopulacao_remanescente(gid_subpop, gid_remanescente, id) values (#{gid_subpop[x]}, #{row['gid_rem']}, #{id});")
			end
		end
	end
end


















## Inserir informacoes da Flora do Brasil na tabela de especies
def insertFB(conn)
   conn.exec("select id, genus, specificEpithet, infraspecificEpithet from geo.fb_taxon where ((taxonomicStatus = 'NOME_ACEITO') and (genus is not null) and (specificEpithet is not null));") do |result|
      result.each do |row|
         id_fb = (row['id'])
         genus = (row['genus'])
	 specificepithet = (row['specificepithet'])
         infraspecificepithet = (row['infraspecificepithet'])
        
         conn.exec("update geo.especies set id_fb = #{id_fb} where genero = '#{genus}' and especie = '#{specificepithet}' and infranome = '#{infraspecificepithet}';")
         puts "#{id_fb} = #{genus} #{specificepithet} #{infraspecificepithet}"
      end
   end

   conn.exec("select id, lifeform from geo.fb_speciesprofile where lifeform is not null;") do |result|
      result.each do |row|
         id_fb = (row['id'])
         lifeform_fb = (row['lifeform'])
         
         conn.exec("update geo.especies set lifeform_fb = '#{lifeform_fb}' where id_fb = '#{id_fb}';")
         puts "#{id_fb} = #{lifeform_fb}"
      end
   end
end




## Criar poligono de AOO no postgis
def createAOOOld(conn, id)
   conn.exec("select st_astext(b.geom) from geo.grid b inner join geo.ocorrencias a on st_intersects(b.geom, a.geom) where a.id = #{id} group by a.id, b.geom;") do |result|
      result.each do |row|
         poligono = (row['st_astext'])      
         conn.exec("insert into geo.aoo(id,geom) values (#{id}, (st_geomfromtext(\'#{poligono}\',4326)));")      
      end
   end
end




## Cria e insere dados de RODOVIAS de uma especie na tabela SUBPOPULACAO_RODOVIA
def createSubpopulacaoRodovia(conn)
subpop = []
   conn.exec("select gid from geo.subpopulacoes;") do |result|
      result.each do |row|
         subpop.push(row['gid'])
      end
   end
   for x in (0..subpop.count-1) 
      puts "subpopulacao #{x+1} de #{subpop.count}"
      rod = []
      conn.exec("select gid from geo.rodovias where st_intersects(geom_buffer, (select geom from geo.subpopulacoes where gid = #{subpop[x]}))") do |result|
         result.each do |row|
            rod.push(row['gid'])
         end
      end
      for y in (0..rod.count-1)
         puts "   - rodovia #{y+1} de #{rod.count}"
         conn.exec("insert into geo.subpopulacao_rodovia(gid_subpop, gid_rod) values (#{subpop[x]}, #{rod[y]})") 
      end 
   end
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







