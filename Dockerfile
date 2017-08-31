FROM saiqi/16mb-platform:latest

RUN pip install sqlparse

RUN mkdir /service 

ADD application /service/application
ADD ./cluster.yml /service

WORKDIR /service
