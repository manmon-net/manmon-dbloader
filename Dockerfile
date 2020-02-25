FROM secure.manmon.net/centosmin7:latest

RUN yum -y install java-1.8.0-openjdk-headless && rm -rf /var/cache/yum

ADD manmon-dbloader-0.0.1-SNAPSHOT-jar-with-dependencies.jar /manmon-dbloader-0.0.1-SNAPSHOT-jar-with-dependencies.jar
ADD log4j.properties /log4j.properties

CMD /usr/bin/java -jar manmon-dbloader-0.0.1-SNAPSHOT-jar-with-dependencies.jar
