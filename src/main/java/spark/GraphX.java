package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class GraphX extends Thread {
    //    Set<Integer> vertices;
    static final int k = 3;
    int threadNum = 0;
    Map<Integer, String> vertexName;
    Map<Integer, String> edgeName;
    Map<Integer, Integer> inDegree;
    Map<Integer, Integer> outDegree;
    Map<Integer, Map<Integer, Integer>> edge;
    //HashMap<Integer, HashMap<Integer, String>> reverseEdge;
    String dataPath;
    //the number of paths passing a vertex
    Map<Integer, Integer> vertexPathNum;
    //start vertex set of each vertex
    Map<Integer, Set<Integer>> startVertexSet;
    List<List<Integer>> result;
    int startVertexNum;
    String dir;
    CountDownLatch latch;

    public GraphX(String path) {
        vertexName = new HashMap<>();
        edgeName = new HashMap<>();
        inDegree = new HashMap<>();
        outDegree = new HashMap<>();
        edge = new HashMap<>();
        //reverseEdge = new HashMap<>();
        dataPath = path;
        vertexPathNum = new HashMap<>();
        startVertexSet = new HashMap<>();
        result = new ArrayList<>();
        startVertexNum = 0;
        dir = null;
        latch = new CountDownLatch(k);
    }

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
        GraphX graphX = new GraphX(dir + args[0]);
        graphX.setDataOutputDir(dir);
        long start = System.currentTimeMillis();
        graphX.loadGraph();
        long midMid = System.currentTimeMillis();
        graphX.generateEP();
        long mid = System.currentTimeMillis();
        graphX.mergeVertex();
        long end = System.currentTimeMillis();
        graphX.start();
        long endEnd = System.currentTimeMillis();
        //graphX.printResult();
        graphX.printOverView();
        System.out.println("load graph: " + (midMid - start) / (double) 1000 + "(s)");
        System.out.println("generateEP: " + (mid - midMid) / (double) 1000 + "(s)");
        System.out.println("vertex merge: " + (end - mid) / (double) 1000 + "(s)");
        System.out.println("data: " + (endEnd - end) / (double) 1000 + "(s)");
        System.out.println("total time: " + (endEnd - start) / (double) 1000 + "(s)");
    }

    public void setDataOutputDir(String aDir) {
        dir = aDir;
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
                Integer sHash = spo[0].hashCode();
                Integer pHash = spo[1].hashCode();
                Integer oHash = spo[2].hashCode();
                if (!vertexName.containsKey(sHash)) {
                    vertexName.put(sHash, spo[0]);
                }
                if (!edgeName.containsKey(pHash)) {
                    edgeName.put(pHash, spo[1]);
                }
                if (!vertexName.containsKey(oHash)) {
                    vertexName.put(oHash, spo[2]);
                }
                //TODO issue
                edge.computeIfAbsent(sHash, k -> new HashMap<>()).computeIfAbsent(oHash, k -> pHash);
                outDegree.merge(sHash, 1, (x, y) -> x + y);
                inDegree.merge(oHash, 1, (x, y) -> x + y);
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
        ArrayList<Integer> vertices = new ArrayList<>(vertexName.keySet());
        boolean[] visited = new boolean[vertexName.size()];
        for (Map.Entry<Integer, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                result.add(new ArrayList<>());
                result.get(result.size() - 1).add(entry.getKey());
                path.clear();
                DFS(path, visited, vertices.indexOf(entry.getKey()), vertices);
            }
        }
        for (int i = 0; i < visited.length; ++i) {
            if (!visited[i]) {
                result.add(new ArrayList<>());
                result.get(result.size() - 1).add(vertices.get(i));
                path.clear();
                DFS(path, visited, i, vertices);
            }
        }
        //when this func return, result size is the number of start vertices.
        startVertexNum = result.size();
    }

    public void DFS(ArrayList<Integer> path, boolean[] visited, Integer index, ArrayList<Integer> vertices) {
        Integer hash = vertices.get(index);
        path.add(hash);
        visited[index] = true;
        if (!outDegree.containsKey(hash)) {
            incPathNum(path);
            return;
        }
        Map<Integer, Integer> nextSet = edge.get(hash);
        for (Integer integer : nextSet.keySet()) {
            if (path.contains(integer)) {
                incPathNum(path);
            } else {
                DFS(path, visited, vertices.indexOf(integer), vertices);
                path.remove(path.size() - 1);
            }
        }
    }

    public void incPathNum(ArrayList<Integer> path) {
        for (int j = 1; j < path.size(); j++) {
            vertexPathNum.merge(path.get(j), 1, (x, y) -> x + y);
            startVertexSet.computeIfAbsent(path.get(j), k -> new HashSet<>()).add(path.get(0));
        }
    }

    public void mergeVertex() {
        List<List<Integer>> groups = new ArrayList<>();
        ArrayList<Map.Entry<Integer, Integer>> list = new ArrayList<>(vertexPathNum.entrySet());
        // ascending sort
        Collections.sort(list, (o1, o2) -> o1.getValue().compareTo(o2.getValue()));
        System.out.println("Result size before merging: " + result.size());
        for (Map.Entry<Integer, Integer> entry : list) {
            Set<Integer> verticesForMerge = startVertexSet.get(entry.getKey());
            groups.clear();
            int groupSize = 0;
            outer:
            for (List<Integer> integers : result) {
                for (Integer id : verticesForMerge) {
                    if (integers.contains(id)) {
                        groups.add(integers);
                        groupSize += integers.size();
                        if (groupSize > Math.ceil(startVertexNum / (double) k))
                            break outer;
                        break;
                    }
                }
            }
            if (groups.size() == 1) {
                continue;
            }
            if (groupSize <= Math.ceil(startVertexNum / (double) k)) {
                Iterator<List<Integer>> iterator = groups.iterator();
                List<Integer> firstGroup = iterator.next();
                while (iterator.hasNext()) {
                    List<Integer> nextGroup = iterator.next();
                    firstGroup.addAll(nextGroup);
                    result.remove(nextGroup);
                }
            }
            /*
             *  case:
             *      sometimes, if result.size=k+1, then groups.size=3.
             *      in this case, result.size will be k-1 after merging vertex.
             *      so, how to deal with this case?
             *      in fact, this case will not occur.
             */
            if (result.size() <= k) {
                System.out.println("return! result size : " + result.size() + ", k: " + k);
                return;
            }
        }
        while (result.size() > k) {
            Collections.sort(result, ((o1, o2) -> Integer.compare(o1.size(), o2.size())));
            result.get(0).addAll(result.get(1));
            result.remove(1);
        }
    }

    public void start() {
        Thread[] t = new Thread[k];
        for (int i = 0; i < k; i++) {
            t[i] = new Thread(this, "thread" + i);
            t[i].start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            System.out.println(Thread.currentThread().getName() + " running!");
            FileWriter fw = new FileWriter(dir + Thread.currentThread().getName() + ".n3");
            generateEP(result.get(threadNum), fw);
            fw.close();
            latch.countDown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void generateEP(List<Integer> startVertexGroup, FileWriter fw) throws IOException {
        threadNum += 1;
        System.out.println("group " + (threadNum - 1) + " size: " + startVertexGroup.size());
        ArrayList<Integer> path = new ArrayList<>();
        Map<Integer, Set<Integer>> flag = new HashMap<>();
        ArrayList<Integer> vertices = new ArrayList<>(vertexName.keySet());
        boolean[] visited = new boolean[vertexName.size()];
        for (Integer integer : startVertexGroup) {
            path.clear();
            DFS(fw, flag, path, visited, vertices.indexOf(integer), vertices);
        }
    }

    public void DFS(FileWriter fw, Map<Integer, Set<Integer>> flag, ArrayList<Integer> path,
                    boolean[] visited, Integer index, ArrayList<Integer> vertices) throws IOException {
        path.add(vertices.get(index));
        if (path.size() > 1) {
            Integer sHash, oHash;
            sHash = path.get(path.size() - 2);
            oHash = path.get(path.size() - 1);
            Set<Integer> set = flag.computeIfAbsent(sHash, k -> new HashSet<>());
            if (!set.contains(oHash)) {
                set.add(oHash);
                String spo = vertexName.get(sHash) + " ";
                spo += edgeName.get(edge.get(sHash).get(oHash)) + " ";
                spo += vertexName.get(oHash) + "\n";
                fw.write(spo);
            }
        }
        visited[index] = true;
        if (!outDegree.containsKey(vertices.get(index))) {
            return;
        }
        Map<Integer, Integer> nextSet = edge.get(vertices.get(index));
        for (Integer integer : nextSet.keySet()) {
            if (!path.contains(integer)) {
                DFS(fw, flag, path, visited, vertices.indexOf(integer), vertices);
                path.remove(path.size() - 1);
            }
        }
    }

    public boolean loadGraph(String path) {
        dataPath = path;
        return loadGraph();
    }

    public void printResult() {
        for (List<Integer> vertexGroup : result) {
            System.out.println();
            System.out.println("---vertex group start, size: " + vertexGroup.size() + "------------------------------");
            for (Integer integer : vertexGroup) {
                System.out.println(vertexName.get(integer));
            }
        }
    }

    public void printStartVertexSet() {
        for (Map.Entry<Integer, Set<Integer>> entry : startVertexSet.entrySet()) {
            System.out.println("---vertex set start---" + vertexName.get(entry.getKey()));
            for (Integer integer : entry.getValue()) {
                System.out.println(vertexName.get(integer));
            }
        }
    }

    public void printOverView() {
        System.out.println("vertex group number: " + result.size());
        System.out.println("start vertex number: " + startVertexNum);
        System.out.println("not start vertex number: " + startVertexSet.size());
        System.out.println("total vertex number: " + vertexName.size());
    }
}