package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CountLines {
    public static void main(String[] args) {
        if (args.length == 0) {
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
        long[] tripleNum = new long[args.length];
        try {
            for (int i = 0; i < args.length; i++) {
                BufferedReader reader = new BufferedReader(new FileReader(dir + args[i]));
                while (reader.readLine() != null) {
                    tripleNum[i] += 1;
                }
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < args.length; i++) {
            System.out.println(args[i] + " triples num: " + tripleNum[i] + ".");
        }
    }
}
