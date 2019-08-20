package com.chicos.interfaces.associate;

import com.chicos.interfaces.common.VBStore;
import java.util.Iterator;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;

public class AssociateDAO {

    private Connection connection = DriverManager.getConnection("ojai:mapr:");
    private VBStore store;

    public Connection getConnection() {
        return connection;
    }

    public VBStore getStore() {
        return store;
    }

    public AssociateDAO(String tabelPath, boolean print) {
        store = new VBStore (connection.getStore(tabelPath));
        store.setPrint(print);
    }

    public Iterator<Document> getIterator() {
        return store.find().iterator();
    }

    public Document get(String id) {
        return store.findById(id);
    }

}
