# neo4j-imdb
Imports the IMDB dataset to a neo4j database.

## How to use

To be able to use this project, the following steps have to be done in the given order. 

### Requirements

The project was tested with the following software requirements:

 - [Maven](https://maven.apache.org/) 3.6.3 
 - [OpenJDK](https://openjdk.java.net/) 11.0.5_10
 - [Neo4j](https://neo4j.com/) community 4.1.1
 
Please ensure that the required software is available on your system.

### IMDB-data

According to the license model of the imdb dataset, the used source files are not included in this repository. To be 
able to use the provided importer the mentioned files (`name.basics.tsv`, `title.principals.tsv`, `title.basics.tsv`) 
have to be included in the resources folder of this project. Then you can specify the number of relations to be imported
by changing the `max`-parameter of `tsv2CSV.processRelations(max)` in the Importer class. Like this you can import
only a subset of the whole dataset.

The imdb data is available under the following link [https://datasets.imdbws.com/](https://datasets.imdbws.com/), and the documentation to the interfaces can be found here [https://www.imdb.com/interfaces/](https://www.imdb.com/interfaces/).

### Neo4j database 

In order to import the data a neo4j database has to be running on your system. The configuration of the connection 
must be adapted in the `resources/neo4j-config.xml` file.

For importing the data into the database a csv bulk import statement is used. To perform this statement the csv files are exported in the database's import directory. For this, the neo4j installation directory has to be defined in the `neo4j-config.xml` as a constructor argument (`neo4jDatabasePath`) of the `TSV2CSV` class.

### How to run

To start the import process, run the main function in the `Neo4jImdbMain` class.

## Contact

If you have any questions please contact us: [contact@aist.science](mailto:contact@aist.science).

## Scientific Work

If you are using this repository inside a research publication, we would ask you to cite us:

[![DOI](https://zenodo.org/badge/287279035.svg)](https://zenodo.org/badge/latestdoi/287279035)
