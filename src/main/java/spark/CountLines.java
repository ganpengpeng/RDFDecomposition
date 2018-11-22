package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CountLines {
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
        long tripleNum = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(dir + args[0]));
            while (reader.readLine() != null) {
                tripleNum += 1;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(args[0] + " triples num: " + tripleNum + ".");
    }
}
