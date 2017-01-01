package edu.upc.bip.model;

import java.io.Serializable;

/**
 * Created by osboxes on 18/12/16.
 */
public class heatmapConverter implements Serializable {
    public String id;

    public String data;

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
