# 2IMD15 Data Engineering

## Installation
- Download the sourcecode
- Download the data
- Import the maven project from `pom.xml`
- Copy `config.properties.example` to `config.properties` and fill it in

## Execution
- Run `Main`

## Execute on a local cluster
This example assumes you know where to find the spark-submit binary on your system and want to run on #workers = #logical cores on your machine.
- `package` the project using maven
- submit the execution job using `spark-submit --class Main --master "local[*]" target\Project-1.0-SNAPSHOT.jar`
    - The `*` can be replaced by an integer representing the amount of workers to run on

## Execute on a real cluster
This example assumes you know where to find the spark-submit binary on your system.
- `package` the project using maven
- submit the execution job using `spark-submit --class Main --master "spark://IP:PORT" target\Project-1.0-SNAPSHOT.jar`