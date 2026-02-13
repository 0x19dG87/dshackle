FROM eclipse-temurin:24

RUN apt-get update -y && apt-get install -y libcurl4-openssl-dev libcjson-dev
