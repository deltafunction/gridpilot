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

  public MySQLLookupPFN(ATLASDatabase db, String catalogServer,
      String lfn, boolean findAll) throws MalformedURLException {
    super(db, catalogServer, lfn, findAll);
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
    GridPilot.getClassMgr().sqlConnection(
        alias, driver, database, user, passwd, gridAuth,
        db.connectTimeout, db.socketTimeout, db.lrcPoolSize);
    if(gridAuth){
      ATLASDatabase.activateSsl();
    }
    Connection conn = db.getDBConnection(alias);
    // First query the t_lfn table to get the guid
    String req = null;
    if(db.homeSite!=null && db.homeServerMysqlAlias!=null &&
        catalogServer.equalsIgnoreCase(db.homeServerMysqlAlias)){
      req = "SELECT t_lfn.guid, sync FROM t_lfn, t_meta WHERE lfname ='"+lfn+"' AND " +
              "t_lfn.guid=t_meta.guid";
    }
    else{
      req = "SELECT guid FROM t_lfn WHERE lfname ='"+lfn+"'";
    }
    ResultSet rset = null;
    String guid = null;
    Vector guidVector = new Vector();
    Debug.debug(">> "+req, 3);
    rset = conn.createStatement().executeQuery(req);
    while(rset.next()){
      if(db.getStop() || !db.findPFNs){
        rset.close();
        conn.close();
        return null;
      }
      // Don't display records flagged for deletion
      if(db.homeSite!=null && db.homeServerMysqlAlias!=null &&
          catalogServer.equalsIgnoreCase(db.homeServerMysqlAlias) &&
          rset.getString("sync").equals("delete")){
        continue;
      }
      guidVector.add(rset.getString("guid"));
    }
    String [] pfns = null;
    String [] ret = null;
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
      return ret;
    }
    else if(guidVector.size()>1){
      db.error = "WARNING: More than one ("+guidVector.size()+") guids with found for lfn "+lfn;
      Debug.debug(db.error, 1);
    }
    guid = (String) guidVector.get(0);
    // Now query the t_pfn table to get the pfn
    req = "SELECT pfname, fsize, md5sum FROM t_pfn, t_meta WHERE t_pfn.guid = '"+guid+"' AND " +
            "t_pfn.guid = t_meta.guid";
    Debug.debug(">> "+req, 3);
    rset = conn.createStatement().executeQuery(req);
    Vector resultVector = new Vector();
    String bytes = null;
    String checksum = null;
    String [] res = null;
    while(rset.next()){
      if(db.getStop() || !db.findPFNs){
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
      db.error = "ERROR: No pfns with found for guid "+guid;
      Debug.debug(db.error, 1);
      rset.close();
      conn.close();
      throw new SQLException(db.error);
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
}
