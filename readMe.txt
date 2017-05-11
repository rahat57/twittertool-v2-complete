jar  : twittertool-v2-searching.jar,twittertool-v2-indexing.jar
port : indexing 8182,searching 8181
pid  : searching 25188,indexing 13498
command : nohup java -jar twittertool-v2-indexing.jar --server.port=8182 > log.log & 
	: nohup java -jar twittertool-v2-searching.jar --server.port=8181 > log.log & 