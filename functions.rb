#teste com a espécie id = 4089
yml= YAML.load_file("config.yml")
conn = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])

## Listar as espécies do sistema
def getSpeciesId(conn)
   especies = []
   conn.exec("select id from geo.especies order by id;") do |result|
      result.each do |row|
         especies.push(row['id'])
      end
   end
return especies
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



## Inserir dados de REMANESCENTES de uma especie na tabela REMANESCENTES_ESPECIE
def insertRemanescentes(conn,id)

   conn.exec("insert into geo.remanescentes_especie(id,geom) values (#{id}, (select st_union(geom) from geo.remanescentes where st_intersects(geom, (select st_union(geom) from geo.subpopulacoes where id = #{id})) and legenda = 'Mata'));")
 
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


