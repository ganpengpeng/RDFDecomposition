package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Graph {
    //    Set<Integer> vertices;
    HashMap<Integer, String> vertexName;
    HashMap<String, Integer> vertexId;
    HashMap<Integer, Integer> inDegree;
    HashMap<Integer, Integer> outDegree;
    HashMap<Integer, HashMap<Integer, String>> edge;
    //    ArrayList<ArrayList<Integer>> edge2;
    String dataPath;
    Integer count;

    private Graph(String path) {
        vertexId = new HashMap<>();
        vertexName = new HashMap<>();
        inDegree = new HashMap<>();
        outDegree = new HashMap<>();
        edge = new HashMap<>();
        count = -1;
        dataPath = path;
    }

    public static void main(String[] args) {
        Graph graph = new Graph("/home/peng/IdeaProjects/spark-jni/LUBM1U.n3");
        graph.loadGraph();
        //graph.printGraph();
    }

    public boolean loadGraph() {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(dataPath));
            String triple = reader.readLine();
            String[] spo;
            while (triple != null) {
                spo = triple.split(" ");
                Integer subjectId;
                Integer objectId;
                subjectId = vertexId.get(spo[0]);
                if (subjectId == null) {
                    incCount();
                    subjectId = count;
                    vertexId.put(spo[0], subjectId);
                    vertexName.put(subjectId, spo[0]);
                    inDegree.put(subjectId, 0);
                    outDegree.put(subjectId, 0);
                    edge.put(subjectId, new HashMap<>());
                }
                objectId = vertexId.get(spo[2]);
                if (objectId == null) {
                    incCount();
                    objectId = count;
                    vertexId.put(spo[2], objectId);
                    vertexName.put(objectId, spo[2]);
                    inDegree.put(objectId, 0);
                    outDegree.put(objectId, 0);
                    if (edge.containsKey(objectId) == false) {
                        edge.put(objectId, new HashMap<>());
                    }
                }
                edge.get(subjectId).put(objectId, spo[1]);
                Integer originValue = outDegree.get(subjectId);
                outDegree.put(subjectId, originValue + 1);
                originValue = inDegree.get(objectId);
                inDegree.put(objectId, originValue + 1);
                triple = reader.readLine();
            }
//            for (Map.Entry<Integer, Integer> entry : outDegree.entrySet()) {
//                System.out.println(entry.getKey()+"---"+entry.getValue());
//            }
//            System.out.println("--------------------");
//            for (Map.Entry<Integer, Integer> entry : inDegree.entrySet()) {
//                System.out.println(entry.getKey()+"---"+entry.getValue());
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    public boolean loadGraph(String path) {
        dataPath = path;
        return loadGraph();
    }

    private void incCount() {
        count += 1;
    }

    public void graphDFS() {

    }

    public void printGraph() {
        if (count == 0)
            return;
        int i = 0;
        for (Integer integer : edge.keySet()) {
            for (Integer integer1 : edge.get(integer).keySet()) {
                System.out.println(vertexName.get(integer) + ' ' + edge.get(integer).get(integer1)
                    + ' ' + vertexName.get(integer1));
                i += 1;
            }
        }
        System.out.println("i = " + i);
    }
}