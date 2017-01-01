package edu.upc.bip.model;

import edu.upc.bip.utils.HBaseUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Created by osboxes on 21/12/16.
 */
public class FPGrowthLocalModel implements Serializable {

    private List<Coordinate> items;
    private Long frequency;

    public FPGrowthLocalModel(List<Coordinate> items, Long frequency) {
        this.items = items;
        this.frequency = frequency;
    }

    public List<Coordinate> getItems() {
        return items;
    }

    public void setItems(List<Coordinate> items) {
        this.items = items;
    }

    public Long getFrequency() {
        return frequency;
    }

    public void setFrequency(Long frequency) {
        this.frequency = frequency;
    }


    public void writeJsonToHbase(String start,String end, ObjectMapper objectMapper) throws IOException {

        try {
            HBaseUtils.addRecord("datamining", "s"+start+"e"+end, "data", "", objectMapper.writeValueAsString(this));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
