package edu.upc.bip.Tests;

import edu.upc.bip.utils.HBaseUtils;

/**
 * Created by osboxes on 09/12/16.
 */
public class TestForFlume {
    public static void main(String[] args){
        String[] colFamily = new String[1];
        colFamily[0] = "clients";

        try {
            HBaseUtils.creatTable("transactions", colFamily);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
