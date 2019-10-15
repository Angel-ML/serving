########################################################################################################################
#                                                       JAVA DEV                                                        #
########################################################################################################################
FROM maven:3.6.1-jdk-8 as JAVA_DEV

########################################################################################################################
#                                                       JAVA BUILDER                                                       #
########################################################################################################################
FROM JAVA_DEV as JAVA_BUILDER

WORKDIR /app

COPY ./pom.xml /app/
COPY ./common/pom.xml /app/common/
COPY ./server/pom.xml /app/server/
COPY ./client/pom.xml /app/client/
COPY ./dist/pom.xml /app/dist/

RUN mvn -e -B dependency:resolve dependency:resolve-plugins

COPY . /app/

RUN mvn -e -B -Dmaven.test.skip=true package

########################################################################################################################
#                                                       Runtime                                                        #
########################################################################################################################
FROM openjdk:8-jre as Runtime

ENV SERVING_HOME=/usr/local/angel-serving

COPY --from=JAVA_BUILDER /app/dist/target/serving-*-bin $SERVING_HOME

WORKDIR $SERVING_HOME

ENV PATH="$SERVING_HOME/bin:${PATH}" \
    JAVA_OPTS=-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap \
    PORT=8500 \
    REST_API_PORT=8501 \
    MODEL_BASE_PATH=/models \
    MODEL_NAME=angel-model \
    MODEL_PLATFORM=angel \
    ENABLE_METRIC_SUMMARY=true

EXPOSE 8500 8501

VOLUME /models

CMD [ "/bin/sh", "./bin/run_in_docker.sh" ]
