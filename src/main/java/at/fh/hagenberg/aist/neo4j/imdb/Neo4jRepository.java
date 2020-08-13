package at.fh.hagenberg.aist.neo4j.imdb;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Transaction;

/**
 * <p>Simple Repository to execute cypher queries</p>
 */
public class Neo4jRepository implements Repository {
    private final Driver driver;

    public Neo4jRepository(Driver driver) {
        this.driver = driver;
    }

    @Override
    public void query(String query) {
        try(Transaction transaction = driver.session().beginTransaction()) {
            transaction.run(query);
        }
    }
}
