package com.HW1.Problem3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Problem3ReviewReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private TreeMap<Double, ArrayList<String>> map = new TreeMap<Double, ArrayList<String>>();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        Double rating = new Double(0.0);
        for (DoubleWritable d : values) {
            Double r = new Double(d.get());
            rating = rating + r;
            count++;
        }
        rating = rating / count;

        if (map.containsKey(rating)) {
            ArrayList<String> idLIst = map.get(rating);
            idLIst.add(key.toString());
            map.put(rating, idLIst);
        } else {
            ArrayList<String> iDList = new ArrayList<String>();
            iDList.add(key.toString());
            map.put(rating, iDList);

            if (map.size() > 10) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Double s : map.descendingKeySet()) {
            ArrayList<String> ids = map.get(s);
            for (String id : ids) {
                if (count == 10) {
                    break;
                }
                context.write(new Text(id + "^"), new DoubleWritable(s));
                count++;
            }
        }
    }

}
