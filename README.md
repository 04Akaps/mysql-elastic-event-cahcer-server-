# mysql-elastic-event-cahcer-server-
mysql에서의 변동에 따른 이벤트를 캐치하여  elasticSearch에 넣는 서버


```azure
docker run -d -p 9200:9200 -p 9300:9300 \
-e "discovery.type=single-node" \
-e "ELASTIC_USERNAME=<사용할 이름>" \
-e "ELASTIC_PASSWORD=<사용할 패스워드>" \
--name elasticsearch-docker \
docker.elastic.co/elasticsearch/elasticsearch:7.14.0
```
