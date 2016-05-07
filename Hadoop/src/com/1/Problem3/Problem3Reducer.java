package com.HW1.Problem3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Problem3Reducer extends Reducer<Text, Text, Text, Text> {

    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;
    Map<Text, Text> finalOutput = new LinkedHashMap<Text, Text>();

    @Override
    public void setup(Context context) {
        joinType = context.getConfiguration().get("join.type");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        listA.clear();
        listB.clear();

        for (Text t : values) {
            if (t.charAt(0) == '0') {
                listA.add(new Text(t.toString().substring(1).trim()));
            } else if (t.charAt(0) == '1') {
                listB.add(new Text(t.toString().substring(1).trim()));
            }
        }
        executeJoinLogic(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text t : finalOutput.keySet()) {
            context.write(t, new Text("::" + finalOutput.get(t)));
        }

    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {
        if (joinType.equalsIgnoreCase("inner")) {
            if (!listA.isEmpty() && !listB.isEmpty()) {
                for (Text A : listA) {
                    for (Text B : listB) {
                        finalOutput.put(A, B);
                    }
                }
            }
        }
    }
}
