package edu.upc.bip.Tests;

import edu.upc.bip.utils.HBaseUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by osboxes on 03/01/17.
 */
public class CSVtoHBASE {

    private static List<Path> files = new ArrayList<>();
    public static void main(String[] args) throws Exception {


        try {
            Path dir = Paths.get("/home/osboxes/data_beiging_4hours");
            listFiles(dir);
            BufferedReader br = null;
            HBaseUtils.deleteTable("csvtohbase");
            HBaseUtils.creatTable("csvtohbase",new String[]{"data"});
            int lineNmb = 0;
            //files.size()
            for (int i = 0; i < files.size(); i++) {
                Path pltFile = files.get(i);
                br = new BufferedReader(new FileReader(pltFile.toFile()));
                int internalLineNmb = 0;

                String line = br.readLine();
                while (line != null) {
                    if (!line.contains("lat") && !line.contains("id")) {
                        String[] words = line.split(";");
                        String rowKey = words[4]+words[7];
                        String payload= words[0]+";"+words[1]+";"+words[4]+";"+words[7];
                        HBaseUtils.addRecord("csvtohbase",rowKey,"data","",payload);
                        System.out.println(lineNmb++);
                    }
                    line = br.readLine();
                }

            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
        }
    }

    static void listFiles(Path path) throws IOException {

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    listFiles(entry);
                }
                if(entry.getFileName().toString().contains(".csv")) {
                    files.add(entry);
                }
            }
        }
    }
}
