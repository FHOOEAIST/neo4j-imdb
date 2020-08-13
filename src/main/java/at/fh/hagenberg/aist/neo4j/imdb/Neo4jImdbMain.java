package at.fh.hagenberg.aist.neo4j.imdb;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * <p>The Main class for the neo4j imdb data execution</p>
 */
public class Neo4jImdbMain {
    public static void main(String[] args) {
        ApplicationContext appContext = new ClassPathXmlApplicationContext("neo4j-config.xml");
        Runnable importer = appContext.getBean("importer", Runnable.class);
        importer.run();
    }
}
