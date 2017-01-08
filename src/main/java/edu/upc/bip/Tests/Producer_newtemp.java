package edu.upc.bip.Tests;

import edu.upc.bip.model.Coordinate;
import edu.upc.bip.model.FPGrowthLocalModel;
import edu.upc.bip.utils.HBaseUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class Producer_newtemp {

    private static List<Path> files = new ArrayList<>();
    private static ObjectMapper objectMapper = new ObjectMapper();
    public static void main(String[] args) throws IOException {
        // set up the producer


        try {
            //HBaseUtils.deleteTable("dataminingupc");
            //HBaseUtils.creatTable("dataminingupc",new String[]{"data"});
            Path dir = Paths.get("/home/osboxes/data_orig/new/200000.csv");
            BufferedReader br = null;

            int lineNmb = 1;
            //files.size()

            Path pltFile = dir;
            br = new BufferedReader(new FileReader(pltFile.toFile()));
            int internalLineNmb = 0;

            String line = br.readLine();
            List<Coordinate> coordinates = new ArrayList<>();
            List<FPGrowthLocalModel> fpGrowths = new ArrayList<>();
            while (line != null) {
                String[] words = line.split(";");
                if (lineNmb > 1 && lineNmb < 37) {
                    coordinates.add(new Coordinate(words[0], words[1]));
                    if (lineNmb == 36) {
                        fpGrowths.add(new FPGrowthLocalModel(coordinates, 148734L));
                        coordinates = new ArrayList<>();
                    }
                } else if (lineNmb >= 37 && lineNmb < 77) {
                    coordinates.add(new Coordinate(words[0], words[1]));
                    if (lineNmb == 76) {
                        fpGrowths.add(new FPGrowthLocalModel(coordinates, 16304L));
                        coordinates = new ArrayList<>();
                    }
                } else if (lineNmb >= 77 && lineNmb < 121) {
                    coordinates.add(new Coordinate(words[0], words[1]));
                    if (lineNmb == 120) {
                        fpGrowths.add(new FPGrowthLocalModel(coordinates, 12127L));
                        coordinates = new ArrayList<>();
                    }
                } else if (lineNmb >= 121) {
                    coordinates.add(new Coordinate(words[0], words[1]));
                }
                lineNmb++;
                line = br.readLine();
            }
            fpGrowths.add(new FPGrowthLocalModel(coordinates, 10834L));
            coordinates = new ArrayList<>();

            lineNmb = 1;
            dir = Paths.get("/home/osboxes/data_orig/new/1.csv");
            br = new BufferedReader(new FileReader(dir.toFile()));
            line = br.readLine();
            while (line != null) {
                String[] words = line.split(";");
                if (lineNmb > 1 && lineNmb < 27) {
                    coordinates.add(new Coordinate(words[0], words[1]));
                    if (lineNmb == 26) {
                        fpGrowths.add(new FPGrowthLocalModel(coordinates, 12420L));
                        coordinates = new ArrayList<>();
                    }
                } else if (lineNmb >= 27 && lineNmb < 55) {
                    coordinates.add(new Coordinate(words[0], words[1]));
                    if (lineNmb == 54) {
                        fpGrowths.add(new FPGrowthLocalModel(coordinates, 11323L));
                        coordinates = new ArrayList<>();
                    }
                } else if (lineNmb >= 55 && lineNmb < 86) {
                    coordinates.add(new Coordinate(words[0], words[1]));
                    if (lineNmb == 85) {
                        fpGrowths.add(new FPGrowthLocalModel(coordinates, 14123L));
                        coordinates = new ArrayList<>();
                    }
                } else if (lineNmb >= 86) {
                    coordinates.add(new Coordinate(words[0], words[1]));
                }
                lineNmb++;
                line = br.readLine();
            }
            fpGrowths.add(new FPGrowthLocalModel(coordinates, 13523L));

            HBaseUtils.addRecord("dataminingupc", "s2017-01-01T07:55:00e2017-01-03T16:05:10", "data", "", objectMapper.writeValueAsString(fpGrowths));

        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
        }

    }

}
