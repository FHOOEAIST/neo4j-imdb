package science.aist.neo4j.imdb;

import org.springframework.core.io.ClassPathResource;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>Converts the tsv files from imdb to csv files that can be processed by neo4j csv importer</p>
 * <p>It creates batches with 10.000 entries each file</p>
 * <p>The tsv files can be found <a href="https://datasets.imdbws.com/">here</a> and need to be added to the resource
 * folder since the data must not be republished</p>
 *
 * @see <a href="https://www.imdb.com/interfaces/">IMDB Interfaces</a>
 */
public class TSV2CSV {

    public static final String IMPORT = "import";
    public static final String MY_DATA = "myData";
    private final String neo4jDatabasePath;
    private Set<String> movieId;
    private Set<String> personId;

    public TSV2CSV(String neo4jDatabasePath) {
        this.neo4jDatabasePath = neo4jDatabasePath;
    }

    /**
     * Converts the name.basic.tsv into a csv file for bulk imports
     *
     * @return the number of created .csv files
     * @throws IOException input stream
     */
    public int processPerson() throws IOException {
        if (personId == null || personId.isEmpty())
            throw new IllegalStateException("Must call processRelations before");
        String thisLine;
        ClassPathResource classPathResource = new ClassPathResource("name.basics.tsv");
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
            while (true) {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(neo4jDatabasePath + IMPORT + File.separator + MY_DATA + i + "person.csv")))) {
                    i++;
                    br.readLine(); // skip first line
                    // id, name, date of birth, date of death, primaryProfession (array), knowForTitles (array)
                    bw.write("id,name,dob,dod,pp,kft");
                    bw.newLine();
                    int j = 0;
                    while ((thisLine = br.readLine()) != null) {
                        String[] split = thisLine.split("\t");
                        if (!personId.contains(split[0])) continue;
                        bw.write(split[0]);
                        bw.write(",");
                        bw.write(split[1].replace("\"", ""));
                        bw.write(",");
                        bw.write(split[2]);
                        bw.write(",");
                        bw.write(split[3]);
                        bw.write(",");
                        bw.write(split[4].replace("\"", "").replace(",", ";"));
                        bw.write(",");
                        bw.write(split[5].replace("\"", "").replace(",", ";"));
                        bw.newLine();
                        if (++j >= 10000) {
                            break;
                        }
                    }
                    if (thisLine == null) {
                        break;
                    }
                }
            }
        }
        return i - 1;
    }

    /**
     * Converts the title.principals.tsv into a csv file for bulk imports
     *
     * @param max the maximum number of relations to be imported
     * @return the number of created .csv files
     * @throws IOException input stream
     */
    public int processRelations(int max) throws IOException {
        movieId = new HashSet<>();
        personId = new HashSet<>();
        int count = 0;
        String thisLine;
        ClassPathResource classPathResource = new ClassPathResource("title.principals.tsv");
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
            while (true) {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(neo4jDatabasePath + IMPORT + File.separator + MY_DATA + i + "rel.csv")))) {
                    i++;
                    br.readLine(); // skip first line
                    bw.write("titleid,personid,category,job,characters");
                    bw.newLine();
                    int j = 0;
                    while ((thisLine = br.readLine()) != null) {
                        String[] split = thisLine.split("\t");
                        movieId.add(split[0]);
                        personId.add(split[2]);
                        bw.write(split[0]);
                        bw.write(",");
                        bw.write(split[2]);
                        bw.write(",");
                        bw.write(split[3]);
                        bw.write(",");
                        bw.write(split[4].replace("\"", ""));
                        bw.write(",");
                        bw.write(split[5].replace("\"", "").replace("[", "").replace("]", "").replace(",", ";"));
                        bw.newLine();
                        count++;
                        if (count >= max || ++j >= 10000) {
                            break;
                        }
                    }
                    if (count >= max || thisLine == null) {
                        break;
                    }
                }
            }
        }
        return i - 1;
    }

    /**
     * Converts the title.basic.tsv into a csv file for bulk imports
     *
     * @return the number of created .csv files
     * @throws IOException input stream
     */
    public int processMovies() throws IOException {
        if (movieId == null || movieId.isEmpty()) throw new IllegalStateException("Must call processRelations before");
        String thisLine;
        ClassPathResource classPathResource = new ClassPathResource("title.basics.tsv");
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
            while (true) {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(neo4jDatabasePath + IMPORT + File.separator + MY_DATA + i + "movie.csv")))) {
                    i++;
                    br.readLine(); // skip first line
                    bw.write("id,name,primTitle,type,isAdult,startYear,endYear,runtimeMinutes,genres");
                    bw.newLine();
                    int j = 0;
                    while ((thisLine = br.readLine()) != null) {
                        String[] split = thisLine.split("\t");
                        if (!movieId.contains(split[0])) continue;
                        bw.write(split[0]);
                        bw.write(",");
                        bw.write(split[3].replace("\"", ""));
                        bw.write(",");
                        bw.write(split[2].replace("\"", ""));
                        bw.write(",");
                        bw.write(split[1]);
                        bw.write(",");
                        bw.write(String.valueOf(!split[4].equals("0")));
                        bw.write(",");
                        bw.write(split[5]);
                        bw.write(",");
                        bw.write(split[6]);
                        bw.write(",");
                        bw.write(split[7]);
                        bw.write(",");
                        bw.write(split[8].replace(",", ";"));
                        bw.newLine();
                        if (++j >= 10000) {
                            break;
                        }
                    }
                    if (thisLine == null) {
                        break;
                    }
                }
            }
        }
        return i - 1;
    }
}
