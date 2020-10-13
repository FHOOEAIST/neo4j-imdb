package science.aist.neo4j.imdb;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * <p>The Main class for the neo4j imdb data execution</p>
 */
public class Neo4jImdbMain {
    public static void main(String[] args) {
        try(ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext("neo4j-config.xml")) {
            appContext.getBean("importer", Runnable.class).run();
        }
    }
}
