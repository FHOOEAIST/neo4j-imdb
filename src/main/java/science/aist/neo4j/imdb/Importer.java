package science.aist.neo4j.imdb;

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
            // Import Persons to database
            int cnt = tsv2CSV.processPerson();
            for(int i = 0; i <= cnt; i++) {
                String statement = "LOAD CSV WITH HEADERS FROM 'file:///myData"+i+".csv' AS row\n " +
                        "CREATE (e:Person {id: row.id, name: row.name, dateOfBirth: toInteger(row.dob), dateOfDeath: toInteger(row.dod), primaryProfession: split(row.pp, \";\"), knownForTitles: split(row.kft, \";\")})";
                repository.query(statement);
                System.out.println("done: " + i);
            }

            // Import movies to database
            cnt = tsv2CSV.processMovies();
            for(int i = 0; i <= cnt; i++) {
                String statement = "LOAD CSV WITH HEADERS FROM 'file:///myData"+i+".csv' AS row\n " +
                        "CREATE (e:Movie {id: row.id, originalTitle: row.name, primaryTitle: row.primTitle, type: row.type, isAdult: toBoolean(row.isAdult), startYear: toInteger(row.startYear), endYear: toInteger(row.endYear), runtimeMinutes: toInteger(row.runtimeMinutes), genres: split(row.genres, \";\")})";
                repository.query(statement);
                System.out.println("done: " + i);
            }

            // creates indices for person and movie ids to improve the query performance for the creation of the relations
            repository.query("create index person_id_index FOR (p:Person) ON (p.id)");
            repository.query("create index movie_id_index FOR (m:Movie) ON (m.id)");

            // Sometimes it seems that neo4j is creating the index when we do not query it once.
            repository.query("match(m: Movie {id: \"tt0000721\"}) return m");

            // Import relations
            cnt = tsv2CSV.processRelations();
            for(int i = 1; i <= cnt; i++) {
                String statement = "LOAD CSV WITH HEADERS FROM 'file:///myData"+i+".csv' AS row\n " +
                        "MATCH (m:Movie {id: row.titleid})\n "+
                        "MATCH (p:Person {id: row.personid})\n "+
                        "CREATE (p)-[r:part_of {category: row.category, job: row.job, characters: split(row.characters, \";\")}]->(m); ";
                repository.query(statement);
                System.out.println("done: " + i);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
