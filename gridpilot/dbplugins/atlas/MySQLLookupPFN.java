package gridpilot.dbplugins.atlas;

import gridfactory.common.Debug;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

public class MySQLLookupPFN  extends LookupPFN {

  private String [] pfns = null;
  private String [] ret = null;

  public MySQLLookupPFN(ATLASDatabase db, String catalogServer,
      String lfn, String guid, boolean findAll) throws MalformedURLException {
    super(db, catalogServer, lfn, guid, findAll);
  }

  public String [] lookup() throws Exception {
    // Set parameters
    String driver = "org.gjt.mm.mysql.Driver";
    String port = catalogUrl.getPort()==-1 ? "" : ":"+catalogUrl.getPort();
    String user = catalogUrl.getUser()==null ? "" : catalogUrl.getUser();
    String passwd = catalogUrl.getPwd()==null ? "" : catalogUrl.getPwd();
    String path = catalogUrl.getPath()==null ? "" : "/"+catalogUrl.getPath();
    String host = catalogUrl.getHost();
    String alias = host.replaceAll("\\.", "_");
    String database = "jdbc:mysql://"+host+port+path;
    boolean gridAuth = false;
    // The (GridPilot) convention is that if no user name is given (in TOA), we use
    // gridAuth to authenticate
    if(user.equals("")){
      gridAuth = true;
      user = GridPilot.getClassMgr().getSSL().getGridDatabaseUser();
    }
    // Make the connection
    // we use the database url as alias
    GridPilot.getClassMgr().establishJDBCConnection(
        alias, driver, database, user, passwd, gridAuth,
        db.connectTimeout, db.socketTimeout, db.lrcPoolSize);
    if(gridAuth){
      db.activateSsl();
    }
    Connection conn = db.getDBConnection(alias);
    // First query the t_lfn table to get the guid
    if(guid==null){
      lookupGuid(conn);
    }
    if(guid==null){
      db.appendError("ERROR: No GUID found for LFN "+lfn);
      Debug.debug(db.getError(), 1);
      conn.close();
      throw new SQLException(db.getError());
    }
    // Now query the t_pfn table to get the pfn
    String req = "SELECT pfname, fsize, md5sum FROM t_pfn, t_meta WHERE t_pfn.guid = '"+guid+"' AND " +
            "t_pfn.guid = t_meta.guid";
    Debug.debug(">> "+req, 3);
    ResultSet rset = conn.createStatement().executeQuery(req);
    Vector<String> resultVector = new Vector<String>();
    String bytes = null;
    String checksum = null;
    String [] res = null;
    while(rset.next()){
      if(db.getStop() || !db.lookupPFNs()){
        rset.close();
        conn.close();
        return null;
      }
      res = MyUtil.split(rset.getString("pfname"));
      for(int i=0; i<res.length; ++i){
        resultVector.add(res[i]);
      }
      if(bytes==null){
        bytes = rset.getString("fsize");
      }
      if(checksum==null){
        checksum = rset.getString("md5sum");
        if(checksum!=null && !checksum.equals("") && !checksum.matches("\\w+:.*")){
          checksum = "md5:"+checksum;
        }
      }
    }
    if(resultVector.size()==0){
      db.appendError("ERROR: No pfns with found for guid "+guid);
      Debug.debug(db.getError(), 1);
      rset.close();
      conn.close();
      throw new SQLException(db.getError());
    }
    rset.close();
    conn.close();
    Object [] pfnArray = resultVector.toArray();
    pfns = new String [pfnArray.length];
    for(int i=0; i<pfnArray.length; ++i){
      pfns[i] = (String) pfnArray[i];
    }
    if(!findAll && pfns!=null && pfns.length>1){
      pfns = new String [] {pfns[0]};
    }
    ret = new String [pfns.length+2];
    ret[0] = null;
    ret[1] = null;
    for(int i=0; i<pfns.length; ++i){
      ret[i+2] = pfns[i];
    }
    return ret;
  }

  private void lookupGuid(Connection conn) throws Exception {
    String req = "SELECT guid FROM t_lfn WHERE lfname ='"+lfn+"'";
    ResultSet rset = null;
    Vector<String> guidVector = new Vector<String>();
    Debug.debug(">> "+req, 3);
    rset = conn.createStatement().executeQuery(req);
    while(rset.next()){
      if(db.getStop() || !db.lookupPFNs()){
        rset.close();
        conn.close();
        Debug.debug("LFN "+lfn+" not found.", 1);
        return;
      }
      guidVector.add(rset.getString("guid"));
    }
    if(guidVector.size()==0){
      rset.close();
      conn.close();
      pfns = new String [] {};
      ret = new String [pfns.length+2];
      ret[0] = null;
      ret[1] = null;
      for(int i=0; i<pfns.length; ++i){
        ret[i+2] = pfns[i];
      }
      Debug.debug("LFN "+lfn+" not found.", 1);
      return;
    }
    else if(guidVector.size()>1){
      db.appendError("WARNING: More than one ("+guidVector.size()+") guids with found for lfn "+lfn);
      Debug.debug(db.getError(), 1);
    }
    guid = (String) guidVector.get(0);
  }
}
