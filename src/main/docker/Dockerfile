FROM openjdk:8-jdk-alpine
VOLUME /tmp
RUN mkdir /opt/hivehelper
COPY ./conf /opt/hivehelper/conf
COPY ./apache-hive-3.1.2-bin /opt/hivehelper/apache-hive-3.1.2-bin
ENV HIVE_CONF_DIR /opt/hivehelper/conf
ENV HIVE_HOME /opt/hivehelper/apache-hive-3.1.2-bin
ADD hivehelper-0.0.1-SNAPSHOT.jar /opt/hivehelper/app.jar
CMD ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/opt/hivehelper/app.jar","--spring.config.location=/opt/hivehelper/conf/application.properties"]
