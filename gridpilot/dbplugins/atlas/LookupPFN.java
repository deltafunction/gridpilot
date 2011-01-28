package gridpilot.dbplugins.atlas;

import java.net.MalformedURLException;

import org.globus.util.GlobusURL;

public class LookupPFN {
  
  protected ATLASDatabase db;
  protected String catalogServer;
  protected GlobusURL catalogUrl;
  protected String lfn;
  protected String guid;
  protected boolean findAll;
   
  public LookupPFN(ATLASDatabase _db, String _catalogServer,
      String _lfn, String _guid, boolean _findAll) throws MalformedURLException {
    db = _db;
    lfn = _lfn;
    findAll = _findAll;
    catalogServer = _catalogServer;
    catalogUrl = new GlobusURL(catalogServer);
  }
  
  /**
   * Lookup file size, checksum and physical locations.
   * @return a list of the form size, checksum, pfn1, pfn2, ...
   * @throws Exception
   */
  public String [] lookup() throws Exception {
    return null;
  }

}