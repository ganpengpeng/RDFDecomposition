package TripleBitDBM;

import java.util.ArrayList;

public class TripleBitDBM {
    public native boolean buildTripleBit(String n3file, String dbDir);

    public native boolean loadTripleBit(String dbDir, String queryDir);

    public native ArrayList<String> queryTripleBit(String query);

    public native long getIDByUri(String uri);

    public native String getUriByID(long id);

    public native long getPIDByPredicate(String predicate);

    public native String getPredicateByPID(long id);

    public native void deleteTripleBit();
}
