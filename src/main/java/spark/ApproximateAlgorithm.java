package spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ApproximateAlgorithm {
    ArrayList<ArrayList<ArrayList<Integer>>> result;
    HashMap<Integer, ArrayList<ArrayList<Integer>>> vertexPath;
    //    Set<Integer> vertexSet;
    Graph graph;

    public ApproximateAlgorithm(String path) {
        graph = new Graph(path);
        result = new ArrayList<>();
        vertexPath = new HashMap<>();
//        vertexSet = new HashSet<>();
    }

    public static void main(String[] args) {
        ApproximateAlgorithm aa = new ApproximateAlgorithm("/home/peng/IdeaProjects/spark-jni/graph.n3");
        aa.initialize();
    }

    public static void approximateAlgorithm() {

    }

    public void initialize() {
        graph.loadGraph();
        graph.generateEP();

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
    }

    public void printResult() {
        for (ArrayList<ArrayList<Integer>> pathGroup : result) {
            System.out.println("---path group start---");
            for (ArrayList<Integer> path : pathGroup) {
                for (Integer integer : path) {
                    System.out.print(integer + " ");
                }
                System.out.println();
            }
            System.out.println("---path group end---");
        }
    }

    public void printVertexPath() {
        for (Map.Entry<Integer, ArrayList<ArrayList<Integer>>> entry : vertexPath.entrySet()) {
            System.out.println("---vertex id" + entry.getKey() + "start---");
            for (ArrayList<Integer> path : entry.getValue()) {
                for (Integer integer : path) {
                    System.out.print(integer + " ");
                }
                System.out.println();
            }
            System.out.println("---vertex id" + entry.getKey() + "end---");
        }
    }
}
