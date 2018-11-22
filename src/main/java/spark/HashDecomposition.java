package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class HashDecomposition {
    static final int k = 3;
    Map<Integer, Integer> vertexTriplesNum;
    String dir;

    public HashDecomposition(String path) {
        vertexTriplesNum = new HashMap<>();
        dir = path;
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("data file arg!");
            return;
        }
        String dir;
        if (System.getProperty("os.name").contains("Windows")) {
            dir = "C:\\Users\\peng\\IdeaProjects\\spark-jni\\";
        } else if (System.getProperty("user.home").contains("ganpeng")) {
            dir = System.getProperty("user.home") + "/spark/";
        } else {
            dir = System.getProperty("user.home") + "/IdeaProjects/spark-jni/";
        }
        HashDecomposition hd = new HashDecomposition(dir);
        long start = System.currentTimeMillis();
        hd.readTriples(args[0]);
        hd.triplesDivide(args[0]);
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start) / (double) 1000 + "(s)");
    }

    public void readTriples(String data) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(dir + data));
            String triple = reader.readLine();
            String[] spo;
            while (triple != null) {
                spo = triple.split(" ");
                vertexTriplesNum.merge(spo[0].hashCode(), 1, (x, y) -> x + y);
                triple = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void triplesDivide(String data) {
        List<Map.Entry<Integer, Integer>> list = new ArrayList<>(vertexTriplesNum.entrySet());
        Collections.sort(list, (x, y) -> x.getValue().compareTo(y.getValue()));
        List<Integer> hashCode = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : list) {
            hashCode.add(entry.getKey());
        }
        list = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(dir + data));
            FileWriter[] fw = new FileWriter[k];
            for (int i = 0; i < k; i++) {
                fw[i] = new FileWriter(dir + "partition" + i + ".n3");
            }
            String triple = reader.readLine();
            String[] spo;
            while (triple != null) {
                spo = triple.split(" ");
                int index = hashCode.indexOf(spo[0].hashCode()) % k;
                fw[index].write(triple + '\n');
                triple = reader.readLine();
            }
            reader.close();
            for (FileWriter writer : fw) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
