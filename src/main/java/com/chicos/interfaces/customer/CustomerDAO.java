package com.chicos.interfaces.customer;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import java.util.Iterator;
import java.util.List;

public class CustomerDAO {

    private Connection connection = DriverManager.getConnection("ojai:mapr:");
    private VBStore store;

    public void setPrint(boolean print) {
        if(store != null)
            store.setPrint(print);
    }

    public CustomerDAO(String tabelPath, boolean print) {
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
