## Arquivo de Cálculo de Subpopulações
#encoding: utf-8

require 'rubygems'
require 'pg'
require 'yaml'

# teste com a espécie id = 4089
conn = PG::Connection.new( yml['ip'], 5432, nil, nil, 'mbti', 'cncflora', 'cncflora')

def getOccurrenceRegisterById(conn,id)
   #Busca registros de uma determinada espécie
   ocorrencias = []
   conn.exec("select codigocncflora from geo.ocorrencias where id =#{id} ;") do |result|
      result.each do |row|
         ocorrencias.push(row['codigocncflora'])
      end
   end
   ocorrencias
end
ocorrencias = getOccurrenceRegisterById(conn,4089)

=begin
# Loop para percorrer todas as espécies
distancia = 0
a = ocorrencias.count - 1
#puts a
for x in (0..a) 
b = (x + 1) 
  for y in (b..a)
    conn.exec("select st_distance_Sphere(
				(select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[x]}),
				(select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[y]}))/1000 as km;") do |result|
      result.each do |row|
      	temp = row['km'].to_f
	if (temp > distancia) then
	  distancia = temp
        end

      end
    end
  end  
end

=end

def getDistance(conn,ocorrencias)
   # Loop para percorrer todas as espécies
   distancia = 0
   a = ocorrencias.count - 1
   for x in (0..a) 
      b = (x + 1) 
      for y in (b..a)
         conn.exec("select st_distance_Sphere(
				(select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[x]}),
				(select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[y]}))/1000 as km;") do |result|
            result.each do |row|
      	       temp = row['km'].to_f
               if (temp > distancia) then
	          distancia = temp
               end
            end
         end
      end  
   end
   distancia
end

puts getDistance(conn, ocorrencias)

=begin
# Busca os registros da espécie
conn.exec("select codigocncflora from geo.ocorrencias where id = 4089;") do |result|
  result.each do |row|
  ocorrencias.push(row['codigocncflora'])
  end
end

distancia = 0

# Loop para percorrer todas as espécies
a = ocorrencias.count - 1
#puts a
for x in (0..a) 
b = (x + 1) 
  for y in (b..a)
    conn.exec("select st_distance_Sphere(
				(select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[x]}),
				(select geom from geo.ocorrencias where codigocncflora = #{ocorrencias[y]}))/1000 as km;") do |result|
      result.each do |row|
      	temp = row['km'].to_f
	if (temp > distancia) then
	  distancia = temp
        end

      end
    end
  end  
end
=end
#puts distancia



