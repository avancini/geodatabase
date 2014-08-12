## ALL FUNCTIONS SCRIPT
#conn.exec("")



## CREATE TAXON TABLES (SPECIES AND OCCURRENCES)
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


## CREATE ANALISYS TABLES
def createAnalisysTables(conn)
	
	## Table: geo.aoo
	conn.exec("DROP TABLE geo.aoo;")
	conn.exec("CREATE TABLE geo.aoo
			(gid serial NOT NULL,
			  id integer,
			  geom geometry(Polygon,4326),
			  CONSTRAINT aoo_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.aoo OWNER TO cncflora;")



	## Table: geo.eoo
	conn.exec("DROP TABLE geo.eoo;")
	conn.exec("CREATE TABLE geo.eoo(
			  gid serial NOT NULL,
			  id integer NOT NULL,
			  geom geometry(Polygon,4326),
			  CONSTRAINT eoo_pkey PRIMARY KEY (id),
			  CONSTRAINT eoo_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.eoo OWNER TO cncflora;")



	## Table: geo.subpopulacoes
	conn.exec("DROP TABLE geo.subpopulacoes;")
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
	conn.exec("DROP TABLE geo.subpopulacao_remanescente;")
	conn.exec("CREATE TABLE geo.subpopulacao_remanescente(
			  gid integer,
			  gid_subpop integer,
			  gid_remanescente
			  id integer,
			  CONSTRAINT subpopulacao_remanescente_gid_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_remanescente_gid_remanescente_fkey FOREIGN KEY (gid_remanescente) REFERENCES geo.remanescentes (gid),
			  CONSTRAINT subpopulacao_remanescente_subpop_gid_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid),
			  CONSTRAINT subpopulacao_remanescente_id_fkey FOREIGN KEY (id) REFERENCES geo.especies (id));")
	conn.exec("ALTER TABLE geo.subpopulacao_remanescente OWNER TO cncflora;")



	## Table: geo.subpopulacao_rodovia
	conn.exec("DROP TABLE geo.subpopulacao_rodovia;")
	conn.exec("CREATE TABLE geo.subpopulacao_rodovia(
			  gid serial NOT NULL,
			  gid_subpop integer,
			  gid_rod integer,
			  CONSTRAINT subpopulacao_rodovia_gid_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_rodovia_gid_rod_fkey FOREIGN KEY (gid_rod) REFERENCES geo.rodovias (gid),
			  CONSTRAINT subpopulacao_rodovia_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid));")
	conn.exec("ALTER TABLE geo.subpopulacao_rodovia OWNER TO cncflora;")



	## Table: geo.subpopulacao_mineracao
	conn.exec("DROP TABLE geo.subpopulacao_mineracao;")
	conn.exec("CREATE TABLE geo.subpopulacao_mineracao(
			  gid serial NOT NULL,
			  gid_subpop integer,
			  gid_mineracao integer,
			  CONSTRAINT subpopulacao_mineracao_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_mineracao_gid_mineracao_fkey FOREIGN KEY (gid_mineracao) REFERENCES geo.mineracao (gid),
			  CONSTRAINT subpopulacao_mineracao_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid));")
	conn.exec("ALTER TABLE geo.subpopulacao_mineracao OWNER TO cncflora;")



	## Table: geo.subpopulacao_uc
	conn.exec("DROP TABLE geo.subpopulacao_uc;")
	conn.exec("CREATE TABLE geo.subpopulacao_uc(
			  gid serial NOT NULL,
			  gid_subpop integer,
			  gid_uc integer,
			  CONSTRAINT subpopulacao_uc_gid_pkey PRIMARY KEY (gid),
			  CONSTRAINT subpopulacao_uc_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid),
			  CONSTRAINT subpopulacao_uc_gid_uc_fkey FOREIGN KEY (gid_uc) REFERENCES geo.ucs (gid));")
	conn.exec("ALTER TABLE geo.subpopulacao_uc OWNER TO cncflora;")



        ## Table: geo.subpopulacao_terra_indigena
        conn.exec("DROP TABLE geo.subpopulacao_terra_indigena;")
        conn.exec("CREATE TABLE geo.subpopulacao_terra_indigena(
                          gid serial NOT NULL,
                          gid_subpop integer,
                          gid_terra_indigena integer,
                          CONSTRAINT subpopulacao_terra_indigena_gid_pkey PRIMARY KEY (gid),
                          CONSTRAINT subpopulacao_terra_indigena_gid_subpop_fkey FOREIGN KEY (gid_subpop) REFERENCES geo.subpopulacoes (gid),
                          CONSTRAINT subpopulacao_terra_indigena_gid_uc_fkey FOREIGN KEY (gid_terra_indigena) REFERENCES geo.ucs (gid));")
        conn.exec("ALTER TABLE geo.subpopulacao_terra_indigena OWNER TO cncflora;")













end








def getSpeciesId(conn)
   especies = []
   conn.exec("select id from geo.especies order by id;") do |result|
      result.each do |row|
         especies.push(row['id'])
      end
   end
return especies
end


## Lista as subpopulacoes
def getSubpopGid(conn)
subpop = []
   conn.exec("select gid from geo.subpopulacoes where area_minerada = 0 order by gid;") do |result|
      result.each do |row|
         subpop.push(row['gid'])
      end
   end
return subpop
end










## Remover espécies fora do recorte Corredor da Mata Atlântic = BA, MG, ES, RJ, SP e PR
def removeEspecies(conn,especies)
=begin
y = especies.count - 1

endemicas = 0
nao_endemicas = 0
remover = []
   for x in (0..y)

      conn.exec("select st_intersects(st_setsrid(st_union(geom), 4326), (select st_setsrid(st_union(geom), 4326) from geo.estados where gid not in (2, 5, 3, 10, 15, 9))) from geo.ocorrencias where id = #{especies[x]};") do |result|
         result.each do |row|
         puts"Id = #{especies[x]} -> #{row['st_intersects']}"
         if (row['st_intersects'] == 'f') then
            endemicas = endemicas + 1
         else 
            nao_endemicas = nao_endemicas + 1
            remover.push(especies[x])
         end
         end
      end
   end
puts "endemicas = #{endemicas}"
puts "nao endemicas = #{nao_endemicas}"
   for x in (0..remover.count - 1)
      conn.exec("delete from geo.ocorrencias where id = #{remover[x]};")
  end
=end
puts "Delete AOO"
     conn.exec("delete from geo.aoo where aoo.id not in (select distinct(id) from geo.ocorrencias);")
puts "Delete AOO - OK"
puts " -- "
puts "Delete EOO"
     conn.exec("delete from geo.eoo where eoo.id not in (select distinct(id) from geo.ocorrencias);")
puts "Delete EOO - OK"
puts " -- "
puts "Delete Rodovia_subpopulacao"
     conn.exec("delete from geo.subpopulacao_rodovia where gid_subpop in (select gid from geo.subpopulacoes where subpopulacoes.id not in (select distinct(id) from geo.ocorrencias));")
puts "Delete Rodovia_subpopulacao - OK"
puts " -- "
puts "Delete Subpopulacoes"
     conn.exec("delete from geo.subpopulacoes where subpopulacoes.id not in (select distinct(id) from geo.ocorrencias);")
puts "Delete Subpopulacoes - OK"
puts " -- "
puts "Delete remanescente_especie"
     conn.exec("delete from geo.remanescente_especie where remanescente_especie.id not in (select distinct(id) from geo.ocorrencias);")
puts "Delete remanescente-especie - OK"
puts " -- "
puts "Delete especies"
     conn.exec("delete from geo.especies where especies.id not in (select distinct(id) from geo.ocorrencias);")
puts "Delete especies - OK"
puts " -- "

puts "delete ok"
end


## Busca os registros pelo ID de uma espécie
def getOccurrenceRegisterById(conn,id)
   ocorrencias = []
   ocorrencias_temp = []
   # Gera uma lista de ocorrencias sem repetir ocorrencias com mesma coordenadas
   conn.exec("select codigocncflora, (st_x(geom) || ' ' || st_y(geom)) as coordenadas from geo.ocorrencias where id =#{id};") do |result|
      result.each do |row|
         coordenadas = {"codigocncflora" => row['codigocncflora'], "coordenadas" => row['coordenadas']}
         ocorrencias_temp.push(coordenadas)  
      end
      ocorrencias_temp = ocorrencias_temp.uniq {|e| e["coordenadas"]} # Elimina coordenadas duplicadas.
      ocorrencias_temp.each do |row| 
         ocorrencias.push(row["codigocncflora"].to_i)
      end
   end
return ocorrencias
end


## Calcula a maior distância entre os pontos
def getDistance(conn,ocorrencias)
   distancia = 0
   a = ocorrencias.count - 1
   if (a >= 2) then
      for x in (0..a)
         b = (x + 1)
         for y in (b..a)
            conn.exec("select st_distance_Sphere(
                                (select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[x]}),
                                (select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[y]})) as meters;") do |result|
               result.each do |row|
                  temp = row['meters'].to_f
                  if (temp > distancia) then
                     distancia = temp
                  end
               end
            end
         end
      end
   else
      distancia = 10000
   end
return distancia
end


## Criar poligono de subpopulacoes no postgis
def insertSubpopulacoes(conn, id, raio)
   conn.exec("select st_astext(ST_Buffer_Meters((select ST_Union(geom) from geo.ocorrencias where id = #{id}), #{raio}));") do |result|
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
             #puts "INSERT INTO geo.subpopulacoes(id, geom) VALUES (#{id}, (select st_geomFromText(' #{subpopulacoes[x]}'),4326));"
          end
      end
   end
end






## Criar poligono de EOO no postgis
def createEOO(conn, id, ocorrencias)
   if (ocorrencias.count < 3) then
      conn.exec("insert into geo.eoo(id, geom) VALUES(#{id}, (select ST_Buffer_Meters((select ST_Union(geom) from geo.ocorrencias where id = #{id}), 10000)));")
   else
      conn.exec("insert into geo.eoo(id, geom) VALUES(#{id}, (select ST_ConvexHull(ST_Collect(ST_SetSrid(geom,4326))) from geo.ocorrencias where id =#{id}));")
   end
end


## Funcao criada para corrigir as geometrias do shape de AOO que estavam armazenados como multipolygon
def corrigeGrid(conn)
   for y in (1..276722)
      conn.exec("select st_astext(the_geom) from geo.grid where gid = #{y};") do |result|
         result.each do |row|
            poligono = (row['st_astext']).gsub("MULTIPOLYGON(((","POLYGON((").gsub(")))","))")
            conn.exec("update geo.grid set geom = (st_geomfromtext(\'#{poligono}\',4326)) where gid = #{y};")
         end
      end
   end
end


## Funcao criada para corrigir as geometrias do shape de Remanescentes que estavam armazenados como multipolygon
def corrigeRemanescentes(conn)
   for y in (1..278630)
      conn.exec("select st_astext(the_geom) from geo.remanescentes where gid = #{y};") do |result|
         result.each do |row|
            poligono = (row['st_astext']).gsub("MULTIPOLYGON(((","POLYGON((").gsub(")))","))")
            conn.exec("update geo.remanescentes set geom = (st_geomfromtext(\'#{poligono}\',4326)) where gid = #{y};")
#            puts "update geo.remanescentes set geom = (st_geomfromtext(\'#{poligono}\',4326)) where gid = #{y};"
            puts "#{y}  -  278599"
         end
      end
   end
end



## Inserir dados de REMANESCENTES de uma especie na tabela REMANESCENTE_ESPECIE
def insertRemanescentes(conn,id)
gid = []
   conn.exec("select distinct(gid) from geo.remanescentes where st_intersects(geom,(select st_union(geom) from geo.subpopulacoes where id = #{id})) and legenda = 'Mata';") do |result|
      result.each do |row|   
         gid.push(row['gid'])
      end
   end
   for x in (0..gid.count-1)
         conn.exec("insert into geo.remanescente_especie values (#{gid[x]}, #{id})")
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
def createAOO(conn, id)
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







