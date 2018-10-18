package spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ApproximateAlgorithm {
    ArrayList<ArrayList<ArrayList<Integer>>> result;
    HashMap<Integer, ArrayList<ArrayList<Integer>>> vertexPath;
    HashMap<Integer, Double> vertexWeight;
    //    Set<Integer> vertexSet;
    Graph graph;
    int maxPathGroup;

    public ApproximateAlgorithm(String path) {
        graph = new Graph(path);
        result = new ArrayList<>();
        vertexPath = new HashMap<>();
        vertexWeight = new HashMap<>();
//        vertexSet = new HashSet<>();
    }

    public static void main(String[] args) {
        ApproximateAlgorithm aa;
        System.out.println(System.getProperty("user.home"));
        if (System.getProperty("os.name").contains("Windows")) {
            aa = new ApproximateAlgorithm("C:\\Users\\peng\\IdeaProjects\\spark-jni\\graph.n3");
        } else {
            aa = new ApproximateAlgorithm(System.getProperty("user.home") +
                "/IdeaProjects/spark-jni/graph.n3");
        }
        aa.initialize();
        aa.printResult();
        aa.printVertexPath();
        aa.printVertexWeight();
    }

    public void approximateAlgorithm() {
        //merge start vertices
        for (Map.Entry<Integer, Integer> entry : graph.inDegree.entrySet()) {
            if (entry.getKey() == 0) {
                //mergeVertex();
                // TODO
            }
        }
    }

    public void mergeVertex(Integer vid) {
        // TODO
        ArrayList<ArrayList<Integer>> pathSet = vertexPath.get(vid);
        for (ArrayList<Integer> path1 : pathSet) {
            for (ArrayList<ArrayList<Integer>> group : result) {
                for (ArrayList<Integer> path2 : group) {
                    if (System.identityHashCode(path1) == System.identityHashCode(path2)) {

                    }
                }
            }
        }
    }

    public void initialize() {
        graph.loadGraph();
        graph.generateEP();
        maxPathGroup = 0;
        //init Res and E<v>
        for (ArrayList<Integer> arrayList : graph.endToEndPathSet) {
            result.add(new ArrayList<>());
            //try to not use new here
            result.get(result.size() - 1).add(arrayList);
            for (int i = 0; i < arrayList.size() - 1; i++) {
                try {
                    vertexPath.get(arrayList.get(i)).add(arrayList);
                } catch (NullPointerException e) {
                    vertexPath.put(arrayList.get(i), new ArrayList<>());
                    vertexPath.get(arrayList.get(i)).add(arrayList);
                }
            }
        }
        //calculate weight of vertices
        for (Map.Entry<Integer, ArrayList<ArrayList<Integer>>> entry : vertexPath.entrySet()) {
            Double sum = new Double(0);
            for (ArrayList<Integer> integers : entry.getValue()) {
                sum += integers.size();
            }
            vertexWeight.put(entry.getKey(), sum);
        }
    }

    public void printResult() {
        for (ArrayList<ArrayList<Integer>> pathGroup : result) {
            System.out.println("---path group start---");
            for (ArrayList<Integer> path : pathGroup) {
//                System.out.println(System.identityHashCode(path));
                for (Integer integer : path) {
//                    System.out.print(integer + " ");
                    System.out.print(graph.vertexName.get(integer) + " ");
                }
                System.out.println();
            }
            System.out.println("---path group end---");
        }
    }

    public void printVertexPath() {
        for (Map.Entry<Integer, ArrayList<ArrayList<Integer>>> entry : vertexPath.entrySet()) {
            System.out.println("---vertex value: " + graph.vertexName.get(entry.getKey()) + " start---");
            for (ArrayList<Integer> path : entry.getValue()) {
//                System.out.println(System.identityHashCode(path));
                for (Integer integer : path) {
//                    System.out.print(integer + " ");
                    System.out.print(graph.vertexName.get(integer) + " ");
                }
                System.out.println();
            }
            System.out.println("---vertex value: " + graph.vertexName.get(entry.getKey()) + " end---");
        }
    }

    public void printVertexWeight() {
        for (Map.Entry<Integer, Double> entry : vertexWeight.entrySet()) {
            System.out.println("---vertex value: " + graph.vertexName.get(entry.getKey()) +
                " vertex weight: " + entry.getValue() + " ---");
        }
    }
}
