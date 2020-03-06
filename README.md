# DVS server API

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/1f3d8810f9a4499abb489517c5a6875c)](https://app.codacy.com/gh/bitrockteam/kafka-dvs-api?utm_source=github.com&utm_medium=referral&utm_content=bitrockteam/kafka-dvs-api&utm_campaign=Badge_Grade_Dashboard)
[![Build Status](https://iproject-jenkins.reactive-labs.io/buildStatus/icon?job=kafka-dvs-api%2Fmaster)](https://iproject-jenkins.reactive-labs.io/job/kafka-dvs-api/job/master/)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

Scala application with ReST and web-socket APIs serving aggregated flight events. Part of the [DVS project](https://github.com/search?q=topic%3Advs+org%3Abitrockteam&type=Repositories).

## Configuration

The application references the following environment variables:

  - `HOST`: server host
  - `PORT`: server port
  - `KAFKA.BOOTSTRAP.SERVERS`: valid `bootstrap.servers` value (see [Confluent docs](https://docs.confluent.io/current/clients/consumer.html#configuration))
  - `SCHEMAREGISTRY.URL`: valid `schema.registry.url` value (see [Confluent docs](https://docs.confluent.io/current/schema-registry/docs/schema_registry_tutorial.html#java-consumers))
  - `SERVER.WEBSOCKET.MAX.NUMBER.FLIGHTS`: max number of elements returned by API
  - `SERVER.WEBSOCKET.THROTTLE.DURATION`: time interval between each update

## API documentation

Web-socket APIs are documented using [AsyncAPI](https://www.asyncapi.com) standard in [this descriptor](src/main/resources/asyncapi.yaml).

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

## Contribution

If you'd like to contribute to the project, make sure to review our [recommendations](CONTRIBUTING.md).
