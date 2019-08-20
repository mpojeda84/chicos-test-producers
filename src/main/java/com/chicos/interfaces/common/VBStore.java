package com.chicos.interfaces.common;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryResult;


//=== Vadim Brodsky 2019-03-15 - 2019-06-14 === 
public class VBStore
{
	private DocumentStore _store;
	private boolean print;

	private static final Logger log = LogManager.getLogger(VBStore.class.getName());

	private static String millies (long n) 
	{ return String.format("%,dms @", (System.nanoTime()-n)/1000); }
	
	public DocumentStore getStore() {
		return _store;
	}

	public void setPrint(boolean print) {
		this.print = print;
	}

	public VBStore(DocumentStore store) {
		this._store = store;
		print = true;
	}

	public Document findById(String _id) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		Document r = null;
		long n = System.nanoTime();
		try { return r =_store.findById(_id); }
		finally { if(print) log.info(millies(n)+m +"("+_id+") => "+r); }
	}

	public void update(String _id, DocumentMutation mutation) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { _store.update(_id, mutation);	}
		finally { if(print) log.info(millies(n)+m +"("+_id+","+mutation+")"); }
	}
	
//	private String pm(DocumentMutation mutation) {
//		com.mapr.db.rowcol.MutationImpl ii = ((com.mapr.db.rowcol.MutationImpl) mutation);
//		ii
//		.forEach(
//				x->{
//					if (x != null) 
//						System.out.println(
//							x.getFieldPath());});
//		return ii.asJsonString();
//	}

	public DocumentStream find() {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { return _store.find(); }
		finally { if(print) log.info(millies(n)+m +"()"); }
	}
	
	public void close() {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { _store.close();	}
		finally { if(print) log.info(millies(n)+m +"()"); }
	}
	
	public void insertOrReplace(Document doc) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { _store.insertOrReplace(doc);	}
		finally { if(print) log.info(millies(n)+m +"("+doc+")"); }
	}
	
	public QueryResult find(Query query) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { return _store.find(query);	}
		finally { if(print) log.info(millies(n)+m +"("+query+")"); }
	}
	
	public boolean checkAndUpdate(String _id, QueryCondition qc, DocumentMutation dm) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		Boolean r = null; // boolean r = false; // 2019-06-14
		long n = System.nanoTime();
		try { return r = _store.checkAndUpdate(_id, qc, dm);	}
		finally { if(print) log.info(millies(n)+m +"("+_id+","+qc+","+dm+") => "+r); }
	}
	
	public void insert(Document doc) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { _store.insert(doc);	}
		finally { if(print) log.info(millies(n)+m +"("+doc+")"); }
	}

	public void delete(String id) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { _store.delete(id);	}
		finally { if(print) log.info(millies(n)+m +"("+id+")"); }
	}

	public void insertOrReplace(String _id, Document doc) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { _store.insertOrReplace(_id, doc);	}
		finally { if(print) log.info(millies(n)+m +"("+_id+","+doc+")"); }
	}
}