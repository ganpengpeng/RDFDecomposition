package spark;

import java.util.*;

public class ApproximateAlgorithm {
    static final int k = 2;
    ArrayList<ArrayList<ArrayList<Integer>>> result;
    HashMap<Integer, ArrayList<ArrayList<Integer>>> vertexPath;
    HashMap<Integer, Double> vertexWeight;
    Set<Integer> vertexSet;
    Graph graph;

    public ApproximateAlgorithm(String path) {
        graph = new Graph(path);
        result = new ArrayList<>();
        vertexPath = new HashMap<>();
        vertexWeight = new HashMap<>();
        vertexSet = new HashSet<>();
    }

    public static void main(String[] args) {
        ApproximateAlgorithm aa;
        System.out.println(System.getProperty("user.home"));
        if (System.getProperty("os.name").contains("Windows")) {
            aa = new ApproximateAlgorithm("C:\\Users\\peng\\IdeaProjects\\spark-jni\\graph.n3");
        } else if (System.getProperty("user.home").contains("peng")) {
            aa = new ApproximateAlgorithm(System.getProperty("user.home") +
                "/IdeaProjects/spark-jni/LUBM1U.n3");
        } else {
            aa = new ApproximateAlgorithm(System.getProperty("user.home") +
                "/spark/graph.n3");
        }
        aa.initialize();
        aa.printResult();
        //aa.printVertexPath();
        //aa.printVertexWeight();
        aa.approximateAlgorithm();
        System.out.println("------ I am the dividing line ------");
        aa.printResult();
    }

    public void approximateAlgorithm() {
        /*
         * firstly, we can merge start vertices which have zero in degree.
         * then, merge vertex according their weight.
         */

        // merge start vertices
        for (Map.Entry<Integer, Integer> entry : graph.inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                if (mergeVertex(entry.getKey())) {
                    vertexSet.add(entry.getKey());
                } else {
                    System.out.println("vertex id: " + entry.getKey() + " name: " +
                        graph.vertexName.get(entry.getKey()) + " size overflow, not merged!");
                }
            }
        }

        // sort vertices according their weight by ascending
        ArrayList<Map.Entry<Integer, Double>> list = new ArrayList<Map.Entry<Integer, Double>>(
            vertexWeight.entrySet());
        // ascending sort
        Collections.sort(list, (o1, o2) -> o1.getValue().compareTo(o2.getValue()));
        // merge by weight
        for (Map.Entry<Integer, Double> entry : list) {
            // if there is only one path passing a vertex v, then add v to merged vertices set.
            if (vertexPath.get(entry.getKey()).size() == 1) {
                vertexSet.add(entry.getKey());
            } else if (mergeVertex(entry.getKey())) {
                vertexSet.add(entry.getKey());
            } else {
                System.out.println("vertex id: " + entry.getKey() + ", name: " +
                    graph.vertexName.get(entry.getKey()) + ", size overflow, not merged!");
            }
        }
    }

    public boolean mergeVertex(Integer vid) {
        /*
         * edge case:
         *  1. there is only one end to end path passing a vertex, case solved in
         *      spark/ApproximateAlgorithm.java:65.
         *  2. path group size will big than ceil(n/k) after merging a vertex.
         *      in this case, do not execute merge operation.
         */

        // find groups that contain paths which is passing vertex vid
        ArrayList<ArrayList<ArrayList<Integer>>> groupForMerge = new ArrayList<>();
        ArrayList<ArrayList<Integer>> pathSet = vertexPath.get(vid);
        for (ArrayList<Integer> path1 : pathSet) {
            for (ArrayList<ArrayList<Integer>> group : result) {
                if (!groupForMerge.contains(group) && group.contains(path1)) {
                    groupForMerge.add(group);
                }
            }
        }

        // vertex already been merged, then just return true.
        if (groupForMerge.size() == 1)
            return true;
        int mergedGroupSize = 0;
        for (ArrayList<ArrayList<Integer>> group : groupForMerge) {
            mergedGroupSize += group.size();
        }
        // if the sum of group size big than ceil(n/k), print message and return false.
        if (mergedGroupSize > Math.ceil(graph.endToEndPathSet.size() / (double) k)) {

            return false;
        }
        Iterator<ArrayList<ArrayList<Integer>>> iterator = groupForMerge.iterator();
        ArrayList<ArrayList<Integer>> firstGroup = iterator.next();
        while (iterator.hasNext()) {
            ArrayList<ArrayList<Integer>> nextGroup = iterator.next();
            firstGroup.addAll(nextGroup);
            result.remove(nextGroup);
        }
        return true;
    }

    public void initialize() {
        graph.loadGraph();
        graph.generateEP();
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
