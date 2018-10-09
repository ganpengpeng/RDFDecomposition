package TripleBitDBM;

public class BuildTripleBit {
    static {
        System.load("/home/ganpeng/spark/libtriplebit.so");
    }

    public static void main(String[] args) {
        build("/home/ganpeng/spark/spark/TripleBit/LUBM1U.n3", "/home/ganpeng/spark/spark/TripleBit/database/");
    }

    private static void build(String dbDir, String queryDir) {
        TripleBitDBM tripleBitDBM = new TripleBitDBM();
        if (tripleBitDBM.buildTripleBit(dbDir, queryDir)) {
            System.out.println("build success!");
        } else {
            System.out
                .println("build fail! Usage: %s <N3 file name> <Database Directory>");
        }
    }

    public void buildData(String dbDir, String queryDir) {
        build(dbDir, queryDir);
    }
}
