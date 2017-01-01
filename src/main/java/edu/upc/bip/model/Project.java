package edu.upc.bip.model;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Created by osboxes on 30/12/16.
 */
public class Project implements Serializable{

    private String userName;
    private int userID;
    private int projectID;
    private List<Coordinate> poi;

    public String toJson()
    {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void toObject(String jsonString) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            Project project = objectMapper.readValue(jsonString, Project.class);
            this.setPoi(project.getPoi());
            this.setProjectID(project.getProjectID());
            this.setUserID(project.getUserID());
            this.setUserName(project.getUserName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Project() {
    }

    /**
     * New Project
     * @param userName
     * @param userID
     * @param projectID
     * @param poi
     */
    public Project(String userName, int userID, int projectID, List<Coordinate> poi) {
        this.userName = userName;
        this.userID = userID;
        this.projectID = projectID;
        this.poi = poi;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public int getProjectID() {
        return projectID;
    }

    public void setProjectID(int projectID) {
        this.projectID = projectID;
    }

    public List<Coordinate> getPoi() {
        return poi;
    }

    public void setPoi(List<Coordinate> poi) {
        this.poi = poi;
    }
}
