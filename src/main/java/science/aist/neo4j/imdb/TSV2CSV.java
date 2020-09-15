package science.aist.neo4j.imdb;

import org.springframework.core.io.ClassPathResource;

import java.io.*;

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
        String thisLine;
        ClassPathResource classPathResource = new ClassPathResource("name.basics.tsv");
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
            while (true) {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(neo4jDatabasePath + IMPORT + File.separator + MY_DATA + i + ".csv")))) {
                    i++;
                    br.readLine(); // skip first line
                    // id, name, date of birth, date of death, primaryProfession (array), knowForTitles (array)
                    bw.write("id,name,dob,dod,pp,kft");
                    bw.newLine();
                    int j = 0;
                    while ((thisLine = br.readLine()) != null) {
                        String[] split = thisLine.split("\t");
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
     * @return the number of created .csv files
     * @throws IOException input stream
     */
    public int processRelations() throws IOException {
        String thisLine;
        ClassPathResource classPathResource = new ClassPathResource("title.principals.tsv");
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
            while (true) {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(neo4jDatabasePath + IMPORT + File.separator + MY_DATA + i + ".csv")))) {
                    i++;
                    br.readLine(); // skip first line
                    bw.write("titleid,personid,category,job,characters");
                    bw.newLine();
                    int j = 0;
                    while ((thisLine = br.readLine()) != null) {
                        String[] split = thisLine.split("\t");
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
     * Converts the title.basic.tsv into a csv file for bulk imports
     *
     * @return the number of created .csv files
     * @throws IOException input stream
     */
    public int processMovies() throws IOException {
        String thisLine;
        ClassPathResource classPathResource = new ClassPathResource("title.basics.tsv");
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
            while (true) {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(neo4jDatabasePath + IMPORT + File.separator + MY_DATA + i + ".csv")))) {
                    i++;
                    br.readLine(); // skip first line
                    bw.write("id,name,primTitle,type,isAdult,startYear,endYear,runtimeMinutes,genres");
                    bw.newLine();
                    int j = 0;
                    while ((thisLine = br.readLine()) != null) {
                        String[] split = thisLine.split("\t");
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
