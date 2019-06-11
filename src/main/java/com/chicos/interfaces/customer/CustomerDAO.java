package com.chicos.interfaces.customer;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import java.util.Iterator;
import java.util.List;

public class CustomerDAO {

    private String table = "/user/mapr/tables/chicos/customer.db";
    private static int current = 0;

    private Connection connection = DriverManager.getConnection("ojai:mapr:");
    private DocumentStore store;

    public CustomerDAO() {
        store = connection.getStore(table);
    }

    public Iterator<Document> getIterator() {
        return store.find().iterator();
    }

    public Document get(String id) {
        return store.findById(id);
    }
}
