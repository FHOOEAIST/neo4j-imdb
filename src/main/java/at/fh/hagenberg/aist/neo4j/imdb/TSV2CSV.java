package at.fh.hagenberg.aist.neo4j.imdb;

import org.springframework.core.io.ClassPathResource;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * <p>Converts the tsv files from imdb to csv files that can be processed by neo4j csv importer</p>
 * <p>It creates batches with 10.000 entries each file</p>
 * <p>The tsv files can be found <a href="https://datasets.imdbws.com/">here</a> and need to be added to the resource
 * folder since the data must not be republished</p>
 * @see <a href="https://www.imdb.com/interfaces/">IMDB Interfaces</a>
 */
public class TSV2CSV {

    private final String neo4jDatabasePath;

    public TSV2CSV(String neo4jDatabasePath) {
        this.neo4jDatabasePath = neo4jDatabasePath;
    }

    public void processPerson() throws Exception {
        String thisLine;
        ClassPathResource classPathResource = new ClassPathResource("name.basics.tsv");
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
            while (true) {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(neo4jDatabasePath+"import"+File.separator+"myData" + i + ".csv")))) {
                    i++;
                    br.readLine(); // skip first line
                    bw.write("id,name,dob,dod,isActor,isDirector,isManager,isWriter");
                    bw.newLine();
                    int j = 0;
                    while ((thisLine = br.readLine()) != null) {
                        String[] split = thisLine.split("\t");
                        List<String> primaryProfession = Arrays.asList(split[4].split(","));
                        bw.write(split[0]);
                        bw.write(",");
                        bw.write(split[1].replace("\"", ""));
                        bw.write(",");
                        bw.write(split[2]);
                        bw.write(",");
                        bw.write(split[3]);
                        bw.write(",");
                        bw.write(String.valueOf(primaryProfession.contains("actor") || primaryProfession.contains("actress")));
                        bw.write(",");
                        bw.write(String.valueOf(primaryProfession.contains("director")));
                        bw.write(",");
                        bw.write(String.valueOf(primaryProfession.contains("manager")));
                        bw.write(",");
                        bw.write(String.valueOf(primaryProfession.contains("writer")));
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
    }

    public void processRelations() throws Exception {
        String thisLine;
        ClassPathResource classPathResource = new ClassPathResource("title.principals.tsv");
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
            while (true) {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(neo4jDatabasePath+"import"+File.separator+"myData" + i + ".csv")))) {
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
    }

    public void processMovies() throws Exception {
        String thisLine;
        ClassPathResource classPathResource = new ClassPathResource("title.basics.tsv");
        int i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
            while (true) {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(neo4jDatabasePath+"import"+File.separator+"myData" + i + ".csv")))) {
                    i++;
                    br.readLine(); // skip first line
                    bw.write("id,name,type,isAdult,startYear,endYear,runtimeMinutes,genres");
                    bw.newLine();
                    int j = 0;
                    while ((thisLine = br.readLine()) != null) {
                        String[] split = thisLine.split("\t");
                        bw.write(split[0]);
                        bw.write(",");
                        bw.write(split[3].replace("\"", ""));
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
    }
}
