package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Graph {
    //    Set<Integer> vertices;
    HashMap<Integer, String> vertexName;
    HashMap<String, Integer> vertexId;
    HashMap<Integer, Integer> inDegree;
    HashMap<Integer, Integer> outDegree;
    HashMap<Integer, HashMap<Integer, String>> edge;
    Integer count;
    String dataPath;
    ArrayList<ArrayList<Integer>> endToEndPathSet;

    public Graph(String path) {
        vertexId = new HashMap<>();
        vertexName = new HashMap<>();
        inDegree = new HashMap<>();
        outDegree = new HashMap<>();
        edge = new HashMap<>();
        count = -1;
        dataPath = path;
        endToEndPathSet = new ArrayList<>();
    }

    public static void main(String[] args) {
        Graph graph = new Graph("/home/peng/IdeaProjects/spark-jni/graph.n3");
        graph.loadGraph();
        //graph.printGraph();
        graph.generateEP();
        graph.printEP();
    }

    public boolean loadGraph() {
        /*
         *  **************************************************************
         *  WARNING: I found that there are duplicate triples in test
         *     n3 file, and this function can solve this case, but
         *     indegree and outdegree will increase even it is a duplicate
         *     triple. So, indegree and outdegree will only use to
         *     judge a vertex if it is a start vertex or end vertex.
         *  **************************************************************
         */
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
            return false;
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

    public void generateEP() {
        ArrayList<Integer> path = new ArrayList<>();
        boolean[] visited = new boolean[vertexName.size()];
        for (Map.Entry<Integer, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                path.clear();
                DFS(path, visited, entry.getKey());
            }
        }
        for (int i = 0; i < visited.length; ++i) {
            if (visited[i] == false) {
                path.clear();
                DFS(path, visited, i);
            }
        }
    }

    public void DFS(ArrayList<Integer> path, boolean[] visited, Integer id) {
        path.add(id);
        visited[id] = true;
        if (outDegree.get(id) == 0) {
            endToEndPathSet.add(new ArrayList<>(path));
//            printPath(path);
//            System.out.println("---start---");
//            printEP();
//            System.out.println("---end---");
            return;
        }
        HashMap<Integer, String> nextSet = edge.get(id);
        for (Integer integer : nextSet.keySet()) {
            if (path.contains(integer)) {
                path.add(integer);
                endToEndPathSet.add(new ArrayList<>(path));
//                printPath(path);
//                System.out.println("---start---");
//                printEP();
//                System.out.println("---end---");
                path.remove(path.size() - 1);
            } else {
                DFS(path, visited, integer);
                path.remove(path.size() - 1);
            }
        }
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

    public void printEP() {
        for (ArrayList<Integer> path : endToEndPathSet) {
            printPath(path);
        }
    }

    public void printPath(ArrayList<Integer> path) {
        for (Integer integer : path) {
            System.out.print(vertexName.get(integer) + " ");
//            System.out.print(integer + " ");
        }
        System.out.println();
    }
}