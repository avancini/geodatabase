
require 'rubygems'
require 'sinatra'
require 'sinatra/reloader' 
require 'json'
require 'pg'
require 'yaml'
#require_relative 'functions'

## Conexão
yml= YAML.load_file("config.yml")
conn = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])
conn2 = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])
conn3 = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])
conn4 = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])
conn5 = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])
conn6 = PG::Connection.new(yml["ip"], yml["port"], nil, nil, yml["database"], yml["user"], yml["password"])

get '/' do
   redirect '/index.html'
end



## Busca PONTOS de uma especie
get '/pontos' do

   num=params[:numero]
   pontos = []

   conn.exec("select ST_AsGeoJSON(st_transform(geom, 4326)) as pontos from geo.ocorrencias where id = #{num};") do |result|
      result.each do |row|
      pontos.push(JSON.parse(row['pontos']))
      end
   end

   #puts "PONTOS GeoJSON ====>  #{pontos}"
   content_type :json
   pontos.to_json
end



## Busca EOO de uma especie
get '/eoo' do

   num=params[:numero]
   eoo = []

   conn2.exec(" select st_asgeojson(st_transform(geom, 4326)) as poligono from geo.eoo where id = #{num};") do |result2|
      result2.each do |row2|
      eoo.push(JSON.parse(row2['poligono']))
      end
   end

   #puts "EOO GeoJSON ====>  #{eoo}"
   content_type :json
   eoo.to_json
end



## Busca SUBPOPULACOES de uma especie
get '/subpopulacoes' do

   num=params[:numero]
   subpopulacoes = []

   conn3.exec("select ST_AsGeoJSON(st_transform(geom, 4326)) as poligono from geo.subpopulacoes where id = #{num};") do |result3|
      result3.each do |row3|
      subpopulacoes.push(JSON.parse(row3['poligono']))
      end
   end

   #puts "SUBPOPULACOES GeoJSON ====>  #{subpopulacoes}"
   content_type :json
   subpopulacoes.to_json
end


## Busca UCs de uma especie
get '/ucs' do

   num=params[:numero]
   subpopulacoes = []

   conn3.exec("select ST_AsGeoJSON(st_transform(geom, 4326)) as poligono from geo.ucs where gid in (select gid_uc from geo.subpopulacao_uc where gid_subpop in (select gid from geo.subpopulacoes where id =  #{num}));") do |result3|
      result3.each do |row3|
      subpopulacoes.push(JSON.parse(row3['poligono']))
      end
   end

   #puts "SUBPOPULACOES GeoJSON ====>  #{subpopulacoes}"
   content_type :json
   subpopulacoes.to_json
end




=begin
## Busca AOO de uma especie
get '/aoo' do

   num=params[:numero]
   aoo = []

   conn4.exec("select ST_AsGeoJSON(geom) as poligono from geo.aoo where id = #{num};") do |result4|
      result4.each do |row4|
      aoo.push(JSON.parse(row4['poligono']))
      end
   end

   #puts "AOO GeoJSON ====>  #{aoo}"
   content_type :json
   aoo.to_json
end



## Busca REMANESCENTES de uma especie
get '/remanescentes' do

   num=params[:numero]
   remanescentes = []

#   conn5.exec("select ST_AsGeoJSON(geom) as poligono from geo.remanescentes where st_intersects(geom, (select st_union(geom) from geo.subpopulacoes where id = #{num})) and legenda = 'Mata';") do |result5|
conn5.exec("select ST_AsGeoJSON(geom) as poligono from geo.remanescentes where geo.remanescentes.gid in (select distinct(gid) from geo.remanescente_especie where id =  #{num});") do |result5| 
     result5.each do |row5|
      remanescentes.push(JSON.parse(row5['poligono']))
      end
   end

   #puts "REMANESCENTES GeoJSON ====>  #{remanescentes}"
   content_type :json
   remanescentes.to_json
end



## Busca RODOVIAS de uma especie
get '/rodovias' do

   num=params[:numero]
   rodovias = []

conn6.exec("select ST_AsGeoJSON(geom) as linha from geo.rodovias where st_intersects(geom,(select st_union(geom) from geo.subpopulacoes where id = #{num}));") do |result6|
     result6.each do |row6|
      rodovias.push(JSON.parse(row6['linha']))
      end
   end

   content_type :json
   rodovias.to_json
end


=end
