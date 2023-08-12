# mysql-elastic-event-cahcer-server-
mysql에서의 변동에 따른 이벤트를 캐치하여  elasticSearch에 넣는 서버

<h1>사용한 DB</h1>

- MySql 
```azure
본질적인 데이터 보관 및 발생하는 이벤트를 탐지하기 위해서 사용
```

- Redis
```azure
MySql과 지속적이고 직접적인 Connection을 유지하고 싶지 않아서,
Redis와 기본적으로 통신을 지속하고, 이후 서브 루틴을 통해 10분 간격으로 DB를 업데이트
    - 데이터를 업데이트 하는 용도가 아닌 현재 Position을 업데이트 하기 위함
```

- ElasticSearch
```
후에 검색 엔진을 사용하기 위해서 사용
DB에서 발생하는 이벤트를 감지하여 ElasticSearch에 넣어주고
다른 곳에서 해당 데이터를 통해서 검색을 할 수 있으면
좋지 않을까라는 생각으로 접근
```

<h1>사용한 패키지</h1>

- github.com/inconshreveable/log15
```azure
로그 처리를 위한 패키지
```

- github.com/naoina/toml
```azure
일반적인 env처럼 사용하기 위해서 사용
    주로 toml형태를 사용하기 떄문에 해당 패키지 사용
```




<h1>ElasticSearch Docker</h1>

```azure
docker run -d -p 9200:9200 -p 9300:9300 \
-e "discovery.type=single-node" \
-e "ELASTIC_USERNAME=<사용할 이름>" \
-e "ELASTIC_PASSWORD=<사용할 패스워드>" \
--name elasticsearch-docker \
docker.elastic.co/elasticsearch/elasticsearch:7.14.0
```

