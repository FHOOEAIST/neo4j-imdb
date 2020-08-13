package at.fh.hagenberg.aist.neo4j.imdb;

/**
 * <p>The Neo4j Importer</p>
 */
public class Importer implements Runnable {
    private final Repository repository;

    private final TSV2CSV tsv2CSV;

    public Importer(Repository repository, TSV2CSV tsv2CSV) {
        this.repository = repository;
        this.tsv2CSV = tsv2CSV;
    }

    @Override
    public void run() {
        try {
            tsv2CSV.processPerson();
            for(int i = 0; i <= 1026; i++) {
                String statement = "LOAD CSV WITH HEADERS FROM 'file:///myData"+i+".csv' AS row\n " +
                        "CREATE (e:Person {id: row.id, name: row.name, dateOfBirth: toInteger(row.dob), dateOfDeath: toInteger(row.dod), isActor: toBoolean(row.isActor), isDirector: toBoolean(row.isDirector), isManager: toBoolean(row.isManager), isWriter: toBoolean(row.isWriter)})";
                repository.query(statement);
                System.out.println("done: " + i);
            }

            tsv2CSV.processMovies();
            for(int i = 0; i <= 704; i++) {
                String statement = "LOAD CSV WITH HEADERS FROM 'file:///myData"+i+".csv' AS row\n " +
                        "CREATE (e:Movie {id: row.id, name: row.name, type: row.type, isAdult: toBoolean(row.isAdult), startYear: toInteger(row.startYear), endYear: toInteger(row.endYear), runtimeMinutes: toInteger(row.runtimeMinutes), genres: split(row.genres, \";\")})";
                repository.query(statement);
                System.out.println("done: " + i);
            }

            repository.query("create index person_id_index FOR (p:Person) ON (p.id)");
            repository.query("create index movie_id_index FOR (m:Movie) ON (m.id)");

            tsv2CSV.processRelations();
            for(int i = 1; i <= 4050; i++) {
                String statement = "LOAD CSV WITH HEADERS FROM 'file:///myData"+i+".csv' AS row\n " +
                        "MATCH (m:Movie {id: row.titleid})\n "+
                        "MATCH (p:Person {id: row.personid})\n "+
                        "CREATE (p)-[r:part_of {category: row.category, job: row.job, characters: row.characters}]->(m); ";
                repository.query(statement);
                System.out.println("done: " + i);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
