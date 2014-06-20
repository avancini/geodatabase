
require 'rubygems'
require 'sinatra'
require 'sinatra/reloader' 
require 'json'
require 'pg'
require 'yaml'
require_relative 'functions'

## Conexão
yml= YAML.load_file("config.yml")
conn = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])
conn2 = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])
conn3 = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])
conn4 = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])
conn5 = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])

get '/' do
   redirect '/index.html'
end

## Busca PONTOS de uma especie
get '/pontos' do

   num=params[:numero]
   pontos = []

   conn.exec("select ST_AsGeoJSON(geom) as pontos from geo.ocorrencias where id = #{num};") do |result|
      result.each do |row|
      pontos.push(JSON.parse(row['pontos']))
      end
   end

#   puts "PONTOS GeoJSON ====>  #{pontos}"
   content_type :json
   pontos.to_json
end



=begin
## Buscar EOO de uma espécie
get '/eoo' do

   num=params[:numero]
   eoo_temp = [] 
   eoo = []
 
   conn2.exec("select st_astext(geom) as poligono from geo.eoo where id = #{num};") do |result2|
      result2.each do |row2|
         txt2= row2['poligono'].gsub("POLYGON((","").gsub("))","")
         #puts "txt = #{txt}"
         vertices=txt2.split(",")
	 #puts "vertices = #{vertices}"
         poli=[]
	 for i in (0..vertices.count-1)
   	    vertice = vertices[i].split(" ")
            #puts "vertice =#{vertice}"
            vertice[0] = vertice[0].to_f
            vertice[1] = vertice[1].to_f
            #puts "vertice =#{vertice}"
            poli.push(vertice)
	 end
         eoo_temp.push(poli)
      end
   end
 
   # loop que cria a estrutura do geojson com o poligono de eoo
   for x in (0..eoo_temp.count-1) 
      eoo[x]={
         :type=>"Feature",
         :geometry=> {
             :type=>"Polygon",
             :coordinates=>[eoo_temp[x]]
          }
      }
   end

   #puts "EOO ====>  #{eoo}"
   content_type :json
   eoo.to_json
end
=end
## Busca EOO de uma especie
get '/eoo' do

   num=params[:numero]
   eoo = []

   conn2.exec(" select st_asgeojson(geom) as poligono from geo.eoo where id = #{num};") do |result2|
      result2.each do |row2|
      eoo.push(JSON.parse(row2['poligono']))
      end
   end

   puts "EOO GeoJSON ====>  #{eoo}"
   content_type :json
   eoo.to_json
end


## Busca SUBPOPULACOES de uma especie
get '/subpopulacoes' do

   num=params[:numero]
   subpopulacoes = []

   conn3.exec("select ST_AsGeoJSON(geom) as poligono from geo.subpopulacoes where id = #{num};") do |result3|
      result3.each do |row3|
      subpopulacoes.push(JSON.parse(row3['poligono']))
      end
   end

#   puts "SUBPOPULACOES GeoJSON ====>  #{subpopulacoes}"
   content_type :json
   subpopulacoes.to_json
end


## Busca AOO de uma especie
get '/aoo' do

   num=params[:numero]
   aoo = []

   conn4.exec("select ST_AsGeoJSON(geom) as poligono from geo.aoo where id = #{num};") do |result4|
      result4.each do |row4|
      aoo.push(JSON.parse(row4['poligono']))
      end
   end

#   puts "AOO GeoJSON ====>  #{aoo}"
   content_type :json
   aoo.to_json
end


## Busca REMANESCENTES de uma especie
get '/remanescentes' do

   num=params[:numero]
   remanescentes = []

   conn5.exec("select ST_AsGeoJSON(geom) as poligono from geo.remanescentes where st_intersects(geom, (select st_union(geom) from geo.subpopulacoes where id = #{num})) and legenda = 'Mata';") do |result5|
      result5.each do |row5|
      remanescentes.push(JSON.parse(row5['poligono']))
      end
   end

#   puts "REMANESCENTES GeoJSON ====>  #{remanescentes}"
   content_type :json
   remanescentes.to_json
end


