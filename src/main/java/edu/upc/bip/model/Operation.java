package edu.upc.bip.model;

import edu.upc.bip.utils.HBaseUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by osboxes on 31/12/16.
 */
public class Operation implements Serializable {

    private String toStart;
    private String toEnd;
    private Double support;
    private Double confidence;

    public Operation() {
    }

    public Operation(String toStart, String toEnd, Double support, Double confidence) {
        this.toStart = toStart;
        this.toEnd = toEnd;
        this.support = support;
        this.confidence = confidence;
    }

    public String getToStart() {
        return toStart;
    }

    public void setToStart(String toStart) {
        this.toStart = toStart;
    }

    public String getToEnd() {
        return toEnd;
    }

    public void setToEnd(String toEnd) {
        this.toEnd = toEnd;
    }

    public Double getSupport() {
        return support;
    }

    public void setSupport(Double support) {
        this.support = support;
    }

    public Double getConfidence() {
        return confidence;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }

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
            Operation operation = objectMapper.readValue(jsonString, Operation.class);
            this.setToStart(operation.getToStart());
            this.setToEnd(operation.getToEnd());
            this.setSupport(operation.getSupport());
            this.setConfidence(operation.getConfidence());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addToOperationsTable(Date start, Date end, Double support, Double confidence) throws Exception {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String startString = formatter.format(start);
        String endString = formatter.format(end);
        Operation o = new Operation(startString,endString,support,confidence);
        String rowID = "s"+startString+"e"+endString;
        if(HBaseUtils.getOneRecordValue("operations",rowID) == "")
        {
            if (HBaseUtils.getOneRecordValue("datamining","r"+startString+"e"+endString) == "") {
                HBaseUtils.addRecord("operations", rowID, "data", "", o.toJson());
            }
        }
    }

    public static void addToOperationsTable(String start, String end, Double support, Double confidence) throws Exception {
        //Ensure this format "yyyy-MM-dd'T'HH:mm:ss"
        Operation o = new Operation(start,end,support,confidence);
        String rowID = "s"+start+"e"+end;
        if(HBaseUtils.getOneRecordValue("operations",rowID) == "")
        {
            if (HBaseUtils.getOneRecordValue("datamining","r"+start+"e"+end) == "") {
                HBaseUtils.addRecord("operations", rowID, "data", "", o.toJson());
            }
        }
    }

    public static List<Operation> getAllOperations() throws IOException {
        List<String> strings = HBaseUtils.getRecordRangeValues("operations","","");
        List<Operation> operations = new ArrayList();
        for (String string: strings)
        {
            Operation operation = new Operation();
            operation.toObject(string);
            operations.add(operation);
        }
        return operations;
    }

    public static void deleteOperation(String rowID) throws IOException {
        HBaseUtils.delRecord("operations",rowID);
    }
}
