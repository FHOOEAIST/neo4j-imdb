package science.aist.neo4j.imdb;

/**
 * <p>Base Repository Interface</p>
 */
public interface Repository {

    /**
     * Executes a given cypher query on the database
     *
     * @param query the query to be executed
     */
    void query(String query);
}
