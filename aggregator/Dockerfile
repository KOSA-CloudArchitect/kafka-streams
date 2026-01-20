FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/review-aggregator-0.2.0-jar-with-dependencies.jar /app/app.jar
ENV JAVA_OPTS="-Xms256m -Xmx768m"
CMD ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
