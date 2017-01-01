package edu.upc.bip.utils;

/**
 * Created by osboxes on 16/11/16.
 */
import edu.upc.bip.model.Transaction;

import java.io.Serializable;
import java.util.Comparator;

public class TimestampComparator implements Comparator<Transaction>, Serializable {

    @Override
    public int compare(Transaction o1, Transaction o2) {
        if(o1 == null && o2 == null) {
            return 0;
        } else if(o1 == null || o1.getTimestamp() == null) {
            return 1;
        } else if(o2 == null || o2.getTimestamp() == null) {
            return -1;
        } else {
            return o1.getTimestamp().compareTo(o2.getTimestamp());
        }
    }

}
