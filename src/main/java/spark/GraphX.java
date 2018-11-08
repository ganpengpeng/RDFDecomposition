package spark;

import scala.Int;

import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class GraphX {
    //    Set<Integer> vertices;
    Map<Integer, String> vertexName;
    Map<String, Integer> vertexId;
    Map<Integer, Integer> inDegree;
    Map<Integer, Integer> outDegree;
    Map<Integer, Map<Integer, String>> edge;
    //HashMap<Integer, HashMap<Integer, String>> reverseEdge;
    Integer count;
    String dataPath;
    Map<Integer, Integer> vertexPathNum;
    Set<Integer> startVertexSet;
    List<List<Integer>> result;

    public GraphX(String path) {
        vertexId = new HashMap<>();
        vertexName = new HashMap<>();
        inDegree = new HashMap<>();
        outDegree = new HashMap<>();
        edge = new HashMap<>();
        //reverseEdge = new HashMap<>();
        count = -1;
        dataPath = path;
        vertexPathNum = new HashMap<>();
        startVertexSet = new HashSet<>();
        result = new ArrayList<>();
    }

    public static void main(@NotNull String[] args) {
        GraphX graphX;
        if (args.length != 1){
            System.out.println("data file arg!");
            return;
        }
        if (System.getProperty("os.name").contains("Windows")) {
            graphX = new GraphX("C:\\Users\\peng\\IdeaProjects\\spark-jni\\" + args[0]);
        } else if (System.getProperty("user.home").contains("ganpeng")) {
            graphX = new GraphX(System.getProperty("user.home") +
                    "/spark/" + args[0]);
        } else {
            graphX = new GraphX(System.getProperty("user.home") +
                    "/IdeaProjects/spark-jni/" + args[0]);
        }
        long start = System.currentTimeMillis();
        graphX.loadGraph();
        graphX.generateEP();
        //graphX.printPathNum();
        long end = System.currentTimeMillis();
        System.out.println("GraphX: " + (end - start) / (double) 1000 + "(s)");
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
                    /*
                     *  case:
                     *      a vertex first occur as object, then occur as subject.
                     *      if not put the object to edge set, exception will be thrown
                     *      when execute to this sentence.
                     *      (edge.get(subjectId).put(objectId, spo[1]);//spark/GraphX.java:94)
                     */
                    if (edge.containsKey(objectId) == false) {
                        edge.put(objectId, new HashMap<>());
                    }
                }
                if (edge.get(subjectId).get(objectId) == null) {
                    edge.get(subjectId).put(objectId, spo[1]);
                    Integer originValue = outDegree.get(subjectId);
                    outDegree.put(subjectId, originValue + 1);
                    originValue = inDegree.get(objectId);
                    inDegree.put(objectId, originValue + 1);
                }
                triple = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void generateEP() {
        ArrayList<Integer> path = new ArrayList<>();
        boolean[] visited = new boolean[vertexName.size()];
        for (Map.Entry<Integer, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                result.add(new ArrayList<>());
                result.get(result.size()-1).add(entry.getValue());
                path.clear();
                DFS(path, visited, entry.getKey());
            }
        }
        for (int i = 0; i < visited.length; ++i) {
            if (visited[i] == false) {
                result.add(new ArrayList<>());
                result.get(result.size()-1).add(i);
                path.clear();
                DFS(path, visited, i);
            }
        }
    }

    public void DFS(ArrayList<Integer> path, boolean[] visited, Integer id) {
        path.add(id);
        visited[id] = true;
        if (outDegree.get(id) == 0) {
            incPathNum(path);
            return;
        }
        Map<Integer, String> nextSet = edge.get(id);
        for (Integer integer : nextSet.keySet()) {
            if (path.contains(integer)) {
                incPathNum(path);
            } else {
                DFS(path, visited, integer);
                path.remove(path.size() - 1);
            }
        }
    }

    public void incPathNum(ArrayList<Integer> path) {
        for (Integer integer : path) {
            Integer i = vertexPathNum.get(integer);
            if (i == null) {
                vertexPathNum.put(integer, 1);
            } else {
                vertexPathNum.put(integer, i + 1);
            }
        }
    }

    public void mergeVertex(){

    }
    public boolean loadGraph(String path) {
        dataPath = path;
        return loadGraph();
    }

    private void incCount() {
        count += 1;
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

    public void printPathNum() {
        for (Map.Entry<Integer, Integer> entry : vertexPathNum.entrySet()) {
            System.out.println(vertexName.get(entry.getKey()) + ": " + entry.getValue());
        }
    }
}