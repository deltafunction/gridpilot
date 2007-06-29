package gridpilot.dbplugins.atlas;

import java.net.MalformedURLException;

import org.globus.util.GlobusURL;

public class LookupPFN {
  
  ATLASDatabase db;
  String catalogServer;
  GlobusURL catalogUrl;
  String lfn;
  boolean findAll;
   
  public LookupPFN(ATLASDatabase _db, String _catalogServer,
      String _lfn, boolean _findAll) throws MalformedURLException {
    db = _db;
    lfn = _lfn;
    findAll = _findAll;
    catalogServer = _catalogServer;
    catalogUrl = new GlobusURL(catalogServer);
  }
  
  public String [] lookup() throws Exception {
    return null;
  }

}