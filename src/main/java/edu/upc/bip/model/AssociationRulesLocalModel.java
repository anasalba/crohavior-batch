package edu.upc.bip.model;

import java.io.Serializable;
import java.util.List;

/**
 * Created by osboxes on 21/12/16.
 */
public class AssociationRulesLocalModel implements Serializable {

    private List<Coordinate> antecedent;
    private List<Coordinate> consequent;
    private Double confidence;

    public AssociationRulesLocalModel(List<Coordinate> antecedent, List<Coordinate> consequent, Double confidence) {
        this.antecedent = antecedent;
        this.consequent = consequent;
        this.confidence = confidence;
    }

    public List<Coordinate> getAntecedent() {
        return antecedent;
    }

    public void setAntecedent(List<Coordinate> antecedent) {
        this.antecedent = antecedent;
    }

    public List<Coordinate> getConsequent() {
        return consequent;
    }

    public void setConsequent(List<Coordinate> consequent) {
        this.consequent = consequent;
    }

    public Double getConfidence() {
        return confidence;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }
}
