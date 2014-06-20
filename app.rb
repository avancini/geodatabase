
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




## Buscar pontos para uma espécie
get '/pontos' do

   num=params[:numero]
   pontos_temp = []
   pontos = []
 
   conn.exec("select st_x(geom) as long, st_y(geom) as lat from geo.ocorrencias where id = #{num};") do |result|
      result.each do |row|
         pontos_temp.push({"long" => row['long'], "lat" => row['lat']})  
      end
   end

   y = pontos_temp.count - 1
   
   # Loop que cria a estrutura do geojson com todos os pontos
   for x in (0..y) 
      pontos[x]={
         :type=>"Feature",
         :geometry=> {
             :type=>"Point",
             :coordinates=>[pontos_temp[x]['long'],pontos_temp[x]['lat']]
          }
      }
   end 

   #puts "pontos ====>  #{pontos}"
   content_type :json
   pontos.to_json
end





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





## Buscar subpopulacoes de uma espécie
get '/subpopulacoes' do

   num=params[:numero]
   subpopulacoes_temp = []
   subpopulacoes = []

   conn3.exec("select st_astext(geom) as poligono from geo.subpopulacoes where id = #{num};") do |result3|
      result3.each do |row3|
         txt3= row3['poligono'].gsub("POLYGON((","").gsub("))","")
         #puts "txt = #{txt}"
         vertices_sub=txt3.split(",")
         #puts "vertices = #{vertices}"
         poli_sub=[]
         for i in (0..vertices_sub.count-1)
            vertice_sub = vertices_sub[i].split(" ")
            #puts "vertice =#{vertice}"
            vertice_sub[0] = vertice_sub[0].to_f
            vertice_sub[1] = vertice_sub[1].to_f
            #puts "vertice =#{vertice}"
            poli_sub.push(vertice_sub)
         end
         subpopulacoes_temp.push(poli_sub)
      end
   end

   # loop que cria a estrutura do geojson com o poligono de subpopulacoes
   for x in (0..subpopulacoes_temp.count-1)
      subpopulacoes[x]={
         :type=>"Feature",
         :geometry=> {
             :type=>"Polygon",
             :coordinates=>[subpopulacoes_temp[x]]
          }
      }
   end

   #puts "Subpopulacoes ====>  #{subpopulacoes}"
   content_type :json
   subpopulacoes.to_json
end


## Busca AOO de uma especie
get '/aoo' do

   num=params[:numero]
   aoo_temp = []
   aoo = []

   conn4.exec("select st_astext(geom) as poligono from geo.aoo where id = #{num};") do |result4|
      result4.each do |row4|
         txt4= row4['poligono'].gsub("POLYGON((","").gsub("))","")
         #puts "txt = #{txt}"
         vertices_aoo=txt4.split(",")
         #puts "vertices = #{vertices}"
         poli_aoo=[]
         for i in (0..vertices_aoo.count-1)
            vertice_aoo = vertices_aoo[i].split(" ")
            #puts "vertice =#{vertice}"
            vertice_aoo[0] = vertice_aoo[0].to_f
            vertice_aoo[1] = vertice_aoo[1].to_f
            #puts "vertice =#{vertice}"
            poli_aoo.push(vertice_aoo)
         end
         aoo_temp.push(poli_aoo)
      end
   end

   # loop que cria a estrutura do geojson com o poligono de AOO
   for x in (0..aoo_temp.count-1)
      aoo[x]={
         :type=>"Feature",
         :geometry=> {
             :type=>"Polygon",
             :coordinates=>[aoo_temp[x]]
          }
      }
   end

   puts "AOO ====>  #{aoo}"
   content_type :json
   aoo.to_json
end



## Busca remanescentes de uma especie
get '/remanescentes' do

   num=params[:numero]
   remanescentes_temp = []
   remanescentes = []

   conn5.exec("select st_astext(geom) as poligono from geo.remanescentes where st_intersects(geom, (select st_union(geom) from geo.subpopulacoes where id = #{num})) and legenda = 'Mata';") do |result5|
      result5.each do |row5|
         txt5= row5['poligono'].gsub("POLYGON((","").gsub("))","")
         puts "txt5 =====>>>>>>>> #{txt5}"
         puts "<<<<<<<<========= txt5"
         vertices_remanescentes=txt5.split(",")
         #puts "vertices = #{vertices}"
         poli_remanescentes=[]
         for i in (0..vertices_remanescentes.count-1)
            vertice_remanescentes = vertices_remanescentes[i].split(" ")
            #puts "vertice =#{vertice}"
            vertice_remanescentes[0] = vertice_remanescentes[0].to_f
            vertice_remanescentes[1] = vertice_remanescentes[1].to_f
            poli_remanescentes.push(vertice_remanescentes)
         end
         remanescentes_temp.push(poli_remanescentes)
      end
   end

   # loop que cria a estrutura do geojson com o poligono de remanescentes 
   for x in (0..remanescentes_temp.count-1)
#puts remanescentes_temp[x]
      remanescentes[x]={
         :type=>"Feature",
         :geometry=> {
             :type=>"Polygon",
             :coordinates=>[remanescentes_temp[x]]
          }
      }
   end

   #puts "remanescentes ====>  #{remanescentes}"
   content_type :json
   remanescentes.to_json
end





