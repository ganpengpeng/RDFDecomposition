package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Graph {
    //    Set<Integer> vertices;
    HashMap<Integer, String> vertexName;
    HashMap<String, Integer> vertexId;
    ArrayList<ArrayList<String>> edge;
    //    ArrayList<ArrayList<Integer>> edge2;
    String dataPath;
    Integer count;

    private Graph(String path) {
        vertexId = new HashMap<>();
        vertexName = new HashMap<>();
        edge = new ArrayList<>();
        count = -1;
        dataPath = path;
    }

    public static void main(String[] args) {
        Graph graph = new Graph("/home/peng/IdeaProjects/sparkjni/test.n3");
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
                    vertexId.put(spo[0], count);
                    vertexName.put(count, spo[0]);
                    edge.add(subjectId, new ArrayList<>());
                }
                objectId = vertexId.get(spo[2]);
                if (objectId == null) {
                    incCount();
                    objectId = count;
                    vertexId.put(spo[2], count);
                    vertexName.put(count, spo[2]);
                    edge.get(subjectId).add(objectId, spo[1]);
                }
                edge.add(subjectId, new ArrayList<>());
                edge.get(subjectId).add(objectId, spo[1]);
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
        for (Integer integer : vertexName.keySet()) {
            System.out.println(integer + ": " + vertexName.get(integer));
        }
    }
}