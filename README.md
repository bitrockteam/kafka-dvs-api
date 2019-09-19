# K·Flightstream server API

[![Build Status](https://iproject-jenkins.reactive-labs.io/buildStatus/icon?job=kafka-flightstream-api%2Fmaster)](https://iproject-jenkins.reactive-labs.io/view/Kafka%20Flightstream/job/kafka-flightstream-api/job/master/)

Scala application with ReST and web-socket APIs serving aggregated Flight events. Part of the [K·FlightStream project](https://github.com/search?q=topic%3Akafka-flightstream+org%3Abitrockteam&type=Repositories).

## Configuration

The application references the following environment variables:

- `HOST`: server host
- `PORT`: server port
- `KAFKA.BOOTSTRAP.SERVERS`: valid `bootstrap.servers` value (see [Confluent docs](https://docs.confluent.io/current/clients/consumer.html#configuration))
- `SCHEMAREGISTRY.URL`: valid `schema.registry.url` value (see [Confluent docs](https://docs.confluent.io/current/schema-registry/docs/schema_registry_tutorial.html#java-consumers))

## Dependencies

### Resolvers

Some dependencies are downloaded from a private Nexus repository. Make sure to provide a `~/.sbt/.credentials.flightstream` file containing valid credentials:

```properties
realm=Sonatype Nexus Repository Manager
host=nexus.reactive-labs.io
user=<your-username>
password=<your-password>
```

### Kafka topics

The application references the following Kafka topics:

- `flight_received`
- `top_arrival_airport`
- `top_departure_airport`
- `top_speed`
- `top_airline`

## API documentation

Web-socket APIs are documented using [AsyncAPI](https://www.asyncapi.com) standard in [this descriptor](api-models/src/main/resources/asyncapi.yaml).

ReST APIs are documented using [OpenAPI v3](https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.2.md) standard in [this descriptor](api-models/src/main/resources/api.yaml).

## How to test

Execute unit tests running the following command:

```sh
sbt test
```

Execute integration tests running the following command:

```sh
sbt it:test
```

## How to build

Build and publish Docker image running the following command:

```sh
sbt docker:publish
```

## Architectural diagram

Architectural diagram is available [here](docs/diagram.puml). It can be rendered using [PlantText](https://www.planttext.com).

