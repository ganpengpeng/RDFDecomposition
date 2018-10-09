package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Graph {
    //    Set<Integer> vertices;
    HashMap<Integer, String> vertexName;
    HashMap<String, Integer> vertexId;
    HashMap<Integer, HashMap<Integer, String>> edge;
    //    ArrayList<ArrayList<Integer>> edge2;
    String dataPath;
    Integer count;

    private Graph(String path) {
        vertexId = new HashMap<>();
        vertexName = new HashMap<>();
        edge = new HashMap<>();
        count = -1;
        dataPath = path;
    }

    public static void main(String[] args) {
        Graph graph = new Graph("/home/peng/IdeaProjects/spark-jni/test.n3");
        graph.loadGraph();
        graph.printGraph();
    }

    public boolean loadGraph() {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(dataPath));
            String triple = reader.readLine();
            String[] spo;
            while (triple != null) {
//                for (String s : triple.split(" ")) {
//                    System.out.println(s);
//                }
                spo = triple.split(" ");
                Integer subjectId;
                Integer objectId;
                subjectId = vertexId.get(spo[0]);
                if (subjectId == null) {
                    incCount();
                    subjectId = count;
                    vertexId.put(spo[0], subjectId);
                    vertexName.put(subjectId, spo[0]);
                    edge.put(subjectId, new HashMap<>());
                }
                objectId = vertexId.get(spo[2]);
                if (objectId == null) {
                    incCount();
                    objectId = count;
                    vertexId.put(spo[2], objectId);
                    vertexName.put(objectId, spo[2]);
                    edge.get(subjectId).put(objectId, spo[1]);
                }
                triple = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    public boolean loadGraph(String path) {
        dataPath = path;
        return true;
    }

    private void incCount() {
        count += 1;
    }

    public void printGraph() {
        if (count == 0)
            return;
        for (Integer integer : edge.keySet()) {
            for (Integer integer1 : edge.get(integer).keySet()) {
                System.out.println(vertexName.get(integer) + ' ' + vertexName.get(integer1) + ' ' + edge.get(integer).get(integer1));
            }
        }
    }
}