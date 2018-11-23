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
    Map<String, Integer> vertexId;
    Map<Integer, Integer> inDegree;
    Map<Integer, Integer> outDegree;
    Map<Integer, Map<Integer, String>> edge;

    //HashMap<Integer, HashMap<Integer, String>> reverseEdge;
    Integer count;
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
        vertexId = new HashMap<>();
        vertexName = new HashMap<>();
        inDegree = new HashMap<>();
        outDegree = new HashMap<>();
        edge = new HashMap<>();
        //reverseEdge = new HashMap<>();
        count = -1;
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
        graphX.generateEP();
        long mid = System.currentTimeMillis();
        graphX.mergeVertex();
        long end = System.currentTimeMillis();
        graphX.start();
        long endEnd = System.currentTimeMillis();
        //graphX.printResult();
        graphX.printOverView();
        System.out.println("vertex weight: " + (mid - start) / (double) 1000 + "(s)");
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
            long triplesNum = 0;
            long edgeNum = 0;
            String triple = reader.readLine();
            String[] spo;
            while (triple != null) {
                triplesNum += 1;
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
                    if (!edge.containsKey(objectId)) {
                        edge.put(objectId, new HashMap<>());
                    }
                }
                // TODO triples number
                if (edge.get(subjectId).get(objectId) == null) {
                    edgeNum += 1;
                    edge.get(subjectId).put(objectId, spo[1]);
                    Integer originValue = outDegree.get(subjectId);
                    outDegree.put(subjectId, originValue + 1);
                    originValue = inDegree.get(objectId);
                    inDegree.put(objectId, originValue + 1);
                } else {
                    System.out.println(triple);
                }
                triple = reader.readLine();
            }
            System.out.println("triples num: " + triplesNum + ".");
            System.out.println("edges num: " + edgeNum + ".");
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
                result.get(result.size() - 1).add(entry.getKey());
                path.clear();
                DFS(path, visited, entry.getKey());
            }
        }
        for (int i = 0; i < visited.length; ++i) {
            if (visited[i] == false) {
                result.add(new ArrayList<>());
                result.get(result.size() - 1).add(i);
                path.clear();
                DFS(path, visited, i);
            }
        }
        //when this func return, result size is the number of start vertices.
        startVertexNum = result.size();
    }

    public void DFS(ArrayList<Integer> path, boolean[] visited, Integer id) {
        path.add(id);
        if (path.size() > 1) {
//            if (startVertexSet.get(id) == null) {
//                startVertexSet.put(id, new HashSet<>());
//            }
//            startVertexSet.get(id).add(path.get(0));
            startVertexSet.computeIfAbsent(id, k -> new HashSet<>()).add(path.get(0));
        }
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
        for (int j = 1; j < path.size(); j++) {
//            Integer i = vertexPathNum.get(path.get(j));
//            if (i == null) {
//                vertexPathNum.put(path.get(j), 1);
//            } else {
//                vertexPathNum.put(path.get(j), i + 1);
//            }
            vertexPathNum.merge(path.get(j), 1, (x, y) -> x + y);
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
                System.out.println("return! result size : " + result.size() + " k: " + k);
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
        boolean[] visited = new boolean[vertexName.size()];
        for (Integer integer : startVertexGroup) {
            path.clear();
            DFS(fw, flag, path, visited, integer);
        }
    }

    public void DFS(FileWriter fw, Map<Integer, Set<Integer>> flag, ArrayList<Integer> path, boolean[] visited, Integer id) throws IOException {
        path.add(id);
        if (path.size() > 1) {
            Integer sId, oId;
            sId = path.get(path.size() - 2);
            oId = path.get(path.size() - 1);
            Set<Integer> set = flag.computeIfAbsent(sId, k -> new HashSet<>());
            if (!set.contains(oId)) {
                set.add(oId);
                String spo = vertexName.get(sId) + " ";
                spo += edge.get(sId).get(oId) + " ";
                spo += vertexName.get(oId) + " ";
                fw.write(spo + '\n');
            }
        }
        visited[id] = true;
        if (outDegree.get(id) == 0) {
            return;
        }
        Map<Integer, String> nextSet = edge.get(id);
        for (Integer integer : nextSet.keySet()) {
            if (!path.contains(integer)) {
                DFS(fw, flag, path, visited, integer);
                path.remove(path.size() - 1);
            }
        }
    }

    public boolean loadGraph(String path) {
        dataPath = path;
        return loadGraph();
    }

    private void incCount() {
        count += 1;
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