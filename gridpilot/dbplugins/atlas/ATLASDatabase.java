package gridpilot.dbplugins.atlas;

import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.rmi.RemoteException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JProgressBar;
import javax.xml.rpc.ServiceException;

import org.glite.lfc.LFCConfig;
import org.glite.lfc.LFCException;
import org.glite.lfc.LFCServer;
import org.glite.lfc.internal.FileDesc;

import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;


import gridfactory.common.ConfigFile;
import gridfactory.common.DBCache;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.LogFile;
import gridfactory.common.ResThread;

import gridpilot.Database;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

public class ATLASDatabase extends DBCache implements Database{
  
  public final static String DQ2_API_VERSION = "0_3_0";
  
  private String dq2ReaderServer;
  private String dq2WriterServer;
  private String dq2Port;
  private String dq2SecurePort;
  private String dq2Path;
  private String dq2ReaderUrl;
  private String dq2WriterUrl;
  private String dbName;
  private String [] preferredSites;
  private String [] skipSites;
  private LogFile logFile;
  private int fileCatalogTimeout = 2000;
  private HashMap<String, Vector<String>> dqLocationsCache = new HashMap<String, Vector<String>>();
  private HashMap<String, DBResult> queryResults = new HashMap<String, DBResult>();
  private boolean useCaching = false;
  private TiersOfAtlas toa = null;
  private HashSet<String> httpSwitches = new HashSet<String>();
  private boolean forceFileDelete = false;
  private DQ2Access dq2ReadAccess = null;
  private DQ2Access dq2WriteAccess = null;
  private LFCConfig lfcConfig = new LFCConfig();
  private String[] fileFields;
 
  private boolean sslActivated = false;
  private boolean proxySslActivated = false;
  private boolean findPFNs = true;
  private boolean stop = false;
  private String error;
  // WORKING THREAD SEMAPHORE
  private boolean working = false;


  // when creating user datasets, this will be prepended with /grid/atlas
  public String lfcUserBasePath = null;
  public String connectTimeout = "10000";
  public String socketTimeout = "30000";
  public String lrcPoolSize = "5";
  public String homeSite;

  //public String homeServerMysqlAlias;

  public ATLASDatabase(String _dbName) throws IOException, GeneralSecurityException{
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();
    dbName = _dbName;
    
    String useCachingStr = GridPilot.getClassMgr().getConfigFile().getValue(dbName, "Cache search results");
    if(useCachingStr==null || useCachingStr.equalsIgnoreCase("")){
      useCaching = false;
    }
    else{
      useCaching = ((useCachingStr.equalsIgnoreCase("yes")||
          useCachingStr.equalsIgnoreCase("true"))?true:false);
    }

    dq2ReaderServer = configFile.getValue(dbName, "DQ2 reader server");
    dq2WriterServer = configFile.getValue(dbName, "DQ2 writer server");
    dq2Port = configFile.getValue(dbName, "DQ2 port");
    dq2SecurePort = configFile.getValue(dbName, "DQ2 secure port");
    dq2Path = configFile.getValue(dbName, "DQ2 path");
    String lfcUserPath = configFile.getValue(dbName, "User path");
    if(lfcUserPath==null /*|| lfcUserPath.equals("")*/){
      lfcUserBasePath = "";
      //lfcUserBasePath = "/users/"+GridPilot.getClassMgr().getSSL().getGridDatabaseUser()+"/";
      //logFile.addInfo("Notice: will register new files under path "+lfcUserBasePath+" in LFC.");
    }
    else{
      lfcUserBasePath = lfcUserPath;
    }
    
    if(dq2SecurePort==null){
      error = "WARNING: DQ2 secure port not ocnfigured. Write access disabled.";    
      logFile.addMessage(error);
    }

    if(dq2ReaderServer==null || dq2Path==null){
      error = "ERROR: DQ2 not configured. Aborting.";    
      logFile.addMessage(error);
      return;
    }

    //dq2Url = http://atlddmpro.cern.ch:8000/dq2/
    dq2ReaderUrl = "http://"+dq2ReaderServer+(dq2Port==null?"":":"+dq2Port)+
       (dq2Path.startsWith("/")?dq2Path:"/"+dq2Path)+(dq2Path.endsWith("/")?"":"/");
    dq2WriterUrl = "http://"+dq2ReaderServer+(dq2Port==null?"":":"+dq2Port)+
    (dq2Path.startsWith("/")?dq2Path:"/"+dq2Path)+(dq2Path.endsWith("/")?"":"/");
    error = "";

    try{
      dq2ReadAccess = new DQ2Access(dq2ReaderServer, Integer.parseInt(dq2SecurePort), dq2Path);
    }
    catch(Exception e){
      error = "ERROR: could connect to DQ2 at "+dq2ReaderUrl+" on port "+dq2SecurePort+
         ". DQ2 cannot be used.";
      logFile.addMessage(error, e);
    }
    
    try{
      dq2WriteAccess = new DQ2Access(dq2WriterServer, Integer.parseInt(dq2SecurePort), dq2Path);
    }
    catch(Exception e){
      error = "ERROR: could connect to DQ2 at "+dq2WriterUrl+" on port "+dq2SecurePort+
         ". Registering datasets in DQ2 will not be possible.";
      logFile.addMessage(error, e);
    }
    
    fileFields = getFieldNames("file");
    
    // Set preferred download sites
    preferredSites = configFile.getValues(dbName, "preferred sites");
    // Set download sites to be ignored
    skipSites = configFile.getValues(dbName, "ignored sites");
    // Set home server and possible mysql alias
    homeSite = configFile.getValue(dbName, "home site");
    /*if(homeSite!=null){
      String [] servers = MyUtil.split(homeSite);
      if(servers.length==2){
        homeSite = servers[0];
        homeServerMysqlAlias = servers[1];
      }
      else if(servers.length==0 || servers.length>2){
        homeSite = null;
      }
    }*/
    // Get and cache the TOA file
    String[] toaLocations = configFile.getValues(dbName, "Tiers of atlas");
    toa = new TiersOfAtlas(toaLocations[0], toaLocations.length>1?toaLocations[1]:null);
    
    // Set the timeout for querying file catalogs
    String timeout = configFile.getValue(dbName, "File catalog timeout");
    if(timeout!=null){
      try{
        fileCatalogTimeout = Integer.parseInt(timeout);
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not parse file catalog timeout", e);
      }
    }
    
    String forceDeleteStr = null;
    try{
      forceDeleteStr = configFile.getValue(dbName, "Force file deletion");
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    forceFileDelete = (forceDeleteStr!=null && forceDeleteStr.equalsIgnoreCase("yes"));
  }
  
  public boolean lookupPFNs(){
    return findPFNs;
  }
  
  protected void activateSsl() throws Exception{
    if(sslActivated){
      return;
    }
    GridPilot.getClassMgr().getSSL().activateSSL();
    //GSSCredential credential = GridPilot.getClassMgr().getSSL().getGridCredential();
    //lfcConfig.globusCredential = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
    //Debug.debug("Created new LFCConfig from ID "+lfcConfig.globusCredential.getIdentity(), 1);
    sslActivated = true;
    if(GridPilot.IS_FIRST_RUN){
      proxySslActivated = false;
    }
  }
  
  protected void activateProxySsl() throws Exception{
    if(proxySslActivated){
      return;
    }
    GridPilot.getClassMgr().getSSL().activateProxySSL(null, true);
    GSSCredential credential = GridPilot.getClassMgr().getSSL().getGridCredential();
    lfcConfig.globusCredential = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
    Debug.debug("Created new LFCConfig from ID "+lfcConfig.globusCredential.getIdentity(), 1);
    //sslActivated = false;
    proxySslActivated = true;
  }

  public void requestStop(){
    setFindPFNs(false);
    stop = true;
  }
  
  public void requestStopLookup(){
    setFindPFNs(false);
    Debug.debug("Stopping lookup", 2);
  }
  
  public void clearRequestStopLookup(){
    setFindPFNs(true);
    Debug.debug("Re-enabling lookup", 2);
  }

  public void clearRequestStop(){
    stop = false;
  }

  public boolean getStop(){
    return stop;
  }

  public boolean isFileCatalog(){
    return true;
  }

  public boolean isJobRepository(){
    return false;
  }
  
  public void disconnect(){
  }

  public void clearCaches(){
    clearCacheEntries("dataset");
    clearCacheEntries("file");
    toa.clear();
    dqLocationsCache.clear();
  }
  
  private void setFindPFNs(boolean doit){
    findPFNs = doit;
  }
  
  public /*synchronized*/ DBResult select(String selectRequest, String idField, boolean findAll){
    DBResult ret = null;
    try{
      if(useCaching && queryResults.containsKey(selectRequest)){
        Debug.debug("Returning cached result for "+selectRequest+"-->"+queryResults.get(selectRequest), 3);
        return queryResults.get(selectRequest);
      }
      
      if(getStop()){
        return null;
      }
      
      if(!waitForWorking()){
        GridPilot.getClassMgr().getLogFile().addMessage("WARNING: timed out waiting for other search to complete," +
          "search not performed --> "+selectRequest);
        return null;
      }
     
      ret = doSelect(selectRequest, idField, findAll);
      
    }
    catch(Exception e){
      logFile.addMessage("WARNING: Problem performing SELECT request "+selectRequest, e);
    }
    finally{
      stopWorking();
    }
    return ret;
  }
  
  // Just a wrapper calling doDoSelect an appropriate number of times if ORs are present
  public DBResult doSelect(String selectRequest, String idField, boolean findAll){
    if(!selectRequest.toLowerCase().contains(" or ")){
      return doDoSelect(selectRequest, idField, findAll);
    }
    // First see if the last non-OR wildcard search covers the current.
    String [] selects = selectRequest.split("(?i)\\s+or\\s+");
    DBResult[] ret = new DBResult[selects.length];
    String select = selects[0];
    ret[0] = doDoSelect(select, idField, findAll);
    for(int i=1; i<selects.length; ++i){
      select = select.replaceFirst("(?i)\\s+where\\s+(.*)$", " WHERE "+selects[i]);
      ret[i] = doDoSelect(select, idField, findAll);
    }
    return DBResult.merge(ret);
  }
    
  public /*synchronized*/ DBResult doDoSelect(String selectRequest, String idField, boolean findAll){
        
    JProgressBar pb = null;
    String req = selectRequest;
    Pattern patt;
    Matcher matcher;
    String [] fields;
    String [][] values;

    // *, row1, row2 -> *
    if(selectRequest.matches("SELECT \\*\\,.*") ||
        selectRequest.matches("SELECT \\* FROM .+")){
      Debug.debug("Correcting non-valid select pattern", 3);
      patt = Pattern.compile("SELECT \\*\\, (.+) FROM", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll("SELECT * FROM");
    }
    // Make sure we have identifier as last column.
    else{
      patt = Pattern.compile(", "+idField+", ", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll(", ");
      patt = Pattern.compile(" "+idField+" FROM", Pattern.CASE_INSENSITIVE);
      if(!patt.matcher(req).find()){
        patt = Pattern.compile(" FROM (\\w+)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceAll(", "+idField+" FROM "+"$1");
      }
    }
    
    Debug.debug("request: "+req, 3);
    String table = req.replaceFirst("SELECT (.+) FROM (\\S+)\\s*.*", "$2");
    String fieldsString = req.replaceFirst("SELECT (.*) FROM (\\S+)\\s*.*", "$1");
    if(fieldsString.indexOf("*")>-1){
      fields = getFieldNames(table);
    }
    else{
      fields = MyUtil.split(fieldsString, ", ");
    }
    Debug.debug("fields: "+MyUtil.arrayToString(fields, ":"), 3);
    
    // TODO: display error for expensive wildcard searches that are not allowed
    // When searching for nothing, for this database we return
    // nothing (in other cases we return a complete wildcard search result).
    if(req.matches("SELECT (.+) FROM (\\S+)")){
      error = "ERROR: this is a too expensive search pattern; "+req;
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
      Debug.debug(error, 1);
      DBResult res = new DBResult(fields, new String[0][fields.length]);
      queryResults.put(selectRequest, res);
      return res;
    }
    
    String get = null;
    String conditions = null;
    
    // Find != rules. < and > rules could be done in the same way...
    conditions = req.replaceFirst("SELECT (.*) FROM (\\w*) WHERE (.*)", "$3");
    HashMap<String, String> excludeMap = new HashMap<String, String>();
    String pattern = null;
    String conditions1 = conditions;
    while(conditions1.matches(".+ (\\S+) != (\\S+)\\s*.*")){
      Debug.debug("Constructing exclude map, "+conditions1, 3);
      pattern = conditions1.replaceFirst(".+ (\\S+) != (\\S+)\\s*.*", "$2");
      pattern = pattern.replaceAll("\\*", "\\.\\*");
      excludeMap.put(conditions1.replaceFirst(".+ (\\S+) != (\\S+)\\s*.*", "$1"),
          pattern);
      conditions1 = conditions1.replaceFirst(".+ (\\S+) != (\\S+)\\s*.*", "");
    }
    
    Debug.debug("Exclude patterns: "+
        MyUtil.arrayToString(excludeMap.keySet().toArray())+"-->"+
        MyUtil.arrayToString(excludeMap.values().toArray()), 2);

    //---------------------------------------------------------------------
    if(table.equalsIgnoreCase("dataset")){
      // "dsn", "vuid", "incomplete", "complete"
      
      String dsn = "";
      String vuid = "";
      String complete = "";
      String incomplete = "";

      // Construct get string. Previously this worked for dsn= and vuid= requests.
      // With v0.3 it works only with dsn= requests.
      get = conditions.replaceAll("(?i)\\bdsn = (\\S+)", "dsn=$1");      
      get = get.replaceAll("(?i)\\bvuid = (\\S+)", "vuid=$1");
      get = get.replaceAll("(?i)\\bcomplete = (\\S+)", "complete=$1");
      get = get.replaceAll("(?i)\\bincomplete = (\\S+)", "incomplete=$1");
      
      /*get = get.replaceAll("(?i)\\bdsn CONTAINS (\\S+)", "dsn=*$1*");
      get = get.replaceAll("(?i)\\bvuid CONTAINS (\\S+)", "vuid=*$1*");
      get = get.replaceAll("(?i)\\bcomplete CONTAINS (\\S+)", "complete=*$1*");
      get = get.replaceAll("(?i)\\bincomplete CONTAINS (\\S+)", "incomplete=*$1*");*/
      // Replaced *bla bla* patterns with bla bla*
      // This inconsistency is due to the fact that ATLAS no longer accepts patterns
      // starting with *
      get = get.replaceAll("(?i)\\bdsn CONTAINS (\\S+)", "dsn=$1*");
      get = get.replaceAll("(?i)\\bvuid CONTAINS (\\S+)", "vuid=$1*");
      get = get.replaceAll("(?i)\\bcomplete CONTAINS (\\S+)", "complete=$1*");
      get = get.replaceAll("(?i)\\bincomplete CONTAINS (\\S+)", "incomplete=$1*");

      
      get = get.replaceAll(" AND ", "&");

      if(get.matches("(?i).*\\bdsn=(\\S+).*")){
        dsn = get.replaceFirst("(?i).*\\bdsn=(\\S+).*", "$1");
      }
      if(get.matches("(?i).*\\bvuid=(\\S+)\\b.*")){
        vuid = get.replaceFirst("(?i).*\\bvuid=(\\S+).*", "$1");
      }
      Debug.debug("Matching complete; "+get, 3);
      if(get.matches("(?i).*\\bcomplete=(\\S+).*")){
        Debug.debug("Matched complete!", 3);
        complete = get.replaceFirst("(?i).*\\bcomplete=(\\S+).*", "$1");
      }
      if(get.matches("(?i).*\\bincomplete=(\\S+).*")){
        incomplete = get.replaceFirst("(?i).*\\bincomplete=(\\S+).*", "$1");
      }
      
      String str = null;
      Debug.debug("dsn, vuid, complete, incomplete: "+
          dsn+", "+vuid+", "+complete+", "+incomplete, 2);
      // dsns can be looked up with simple GET
      //if(complete.equals("") && incomplete.equals("")){
        if(vuid.equals("")){
          get = dq2ReaderUrl+"ws_repository/rpc?operation=queryDatasetByName&version=0&API="+DQ2_API_VERSION+"&"+get;
          Debug.debug(">>> get string was : "+get, 3);        
          URL url = null;
          try{
            url = new URL(get);
          }
          catch(MalformedURLException e){
            error = "Could not construct "+get;
            Debug.debug(error, 2);
            return null;
          }
          // Get the DQ result
          str = readGetUrl(url).trim();
        }
        else if(dsn.equals("")){
          try{
            //Debug.debug(">>> get string was : "+dq2Url+"ws_location/rpc?"+
            //    "operation=queryDatasetLocations&API="+DQ2_API_VERSION+"&dsns=[]&vuids="+"["+vuidsString+"]", 3);
            //ret = readGetUrl(new URL(url));
            //ret = URLDecoder.decode(ret, "utf-8");
            str = dq2ReadAccess.getDatasetsByVUIDs("['"+vuid+"']").trim();
          }
          catch(Exception e){
            Debug.debug("WARNING: search returned an error "+str, 1);
            return new DBResult(fields, new String[0][fields.length]);
          }
        }
        // Check if the result is of the form {...}
        // We expect something like
        // "{'user.FrederikOrellana5894-ATLAS.testdataset': [1]}"
        if(str==null || !str.matches("^\\{.*\\}$") || str.matches("^\\{\\}$")){
          Debug.debug("WARNING: search returned an error:"+str+":", 1);
          return new DBResult(fields, new String[0][fields.length]);
        }
      //}
      // DQ2 cannot handle other searches.
      if(str==null){
        error = "WARNING: cannot perform search "+get;
        Debug.debug(error, 1);
        return new DBResult(fields, new String[0][fields.length]);
      }
      
      // Now parse the DQ string and construct DBRecords
      String [] records = MyUtil.split(str, "\\}, ");
      if(records==null || records.length==0){
        Debug.debug("WARNING: no records found with "+str, 2);
        return new DBResult(fields, new String[0][fields.length]);
      }
      
      Vector<String[]> valuesVector = new Vector<String[]>();
      String [] record = null;
      String vuidsString = null;
      String [] vuids = null;
      //String duid = null;
      for(int i=0; i<records.length; ++i){
        
        Vector<String> recordVector = new Vector<String>();
        boolean exCheck = true;
        records[i] = records[i].replaceFirst("^\\{", "");
        records[i] = records[i].replaceFirst("\\}\\}$", "");
        record = MyUtil.split(records[i], ": \\{'vuids': ");
        
        if(record!=null && record.length>1){
          // If the string is the result of a dsn=... request, the
          // split went ok and this should work:
          String name = record[0].replaceAll("'", "");
          //duid = record[0].replaceFirst("'duid': '(.*)'", "$1");
          vuidsString = record[1].replaceFirst("\\[(.*)\\], .*", "$1");
          vuids = MyUtil.split(vuidsString, ", ");
          Debug.debug("Found "+vuids.length+" vuids: "+vuidsString, 3);
          DQ2Locations [] dqLocations = null;
          try{
            dqLocations = getLocations(vuidsString);
          }
          catch(IOException e){
            error = "WARNING: could not find locations.";
            GridPilot.getClassMgr().getStatusBar().setLabel(error);
            e.printStackTrace();
            logFile.addMessage(error, e);
          }
          // If some selection boxes have been set, use patterns for restricting.
          vuid = vuid.replaceAll("([^\\.])\\*", "$1.*").replaceFirst("^\\*", ".*");
          complete = complete.replaceAll("([^\\.])\\*", "$1.*").replaceFirst("^\\*", ".*");
          incomplete = incomplete.replaceAll("([^\\.])\\*", "$1.*").replaceFirst("^\\*", ".*");
          Debug.debug("Matching: "+vuid+":"+complete+":"+incomplete, 2);
          for(int j=0; j<vuids.length; ++j){
            Debug.debug("vuid: "+vuids[j], 3);
            recordVector = new Vector<String>();
            exCheck = true;
            for(int k=0; k<fields.length; ++k){
              if(fields[k].equalsIgnoreCase("dsn")){
                recordVector.add(name);
              }
              else if(fields[k].equalsIgnoreCase("vuid")){
                vuids[j] = vuids[j].replaceAll("'", "");
                if(vuid==null || vuid.equals("") || vuids[j].matches("(?i)"+vuid)){
                  Debug.debug("Adding vuid: "+vuids[j], 3);
                  recordVector.add(vuids[j]);
                }
                else{
                  exCheck = false;
                }
              }
              else if(fields[k].equalsIgnoreCase("incomplete") &&
                  dqLocations!=null && dqLocations[j]!=null){
                String incompleteString =
                  MyUtil.arrayToString(dqLocations[j].getIncomplete());
                if(incomplete==null || incomplete.equals("") ||
                    incompleteString.matches("(?i)"+incomplete)){
                  Debug.debug("Adding incomplete: "+incompleteString, 3);
                  recordVector.add(incompleteString);
                }
                else{
                  exCheck = false;
                }
              }
              else if(fields[k].equalsIgnoreCase("complete") &&
                  dqLocations!=null && dqLocations[j]!=null){
                String completeString =
                  MyUtil.arrayToString(dqLocations[j].getComplete());
                Debug.debug("Matching complete; "+completeString+"<->"+complete, 3);
                if(complete==null || complete.equals("") ||
                    completeString.matches("(?i)"+complete)){
                  Debug.debug("Adding complete: "+completeString, 3);
                  recordVector.add(completeString);
                }
                else{
                  exCheck = false;
                }
              }
              else{
                recordVector.add("");
              }
              if(excludeMap.containsKey(fields[k]) &&
                  recordVector.get(k).toString().matches(
                      "(?i)"+excludeMap.get(fields[k]).toString())){
                exCheck = false;
              }
            }
            if(exCheck){
              Debug.debug("Adding record "+MyUtil.arrayToString(recordVector.toArray()), 3);
              valuesVector.add(recordVector.toArray(new String[recordVector.size()]));
            }
            else{
              Debug.debug("Excluding record ", 2);
            }
          }
        }
        else{
          // Otherwise it should be the result of a vuid=... request and
          // this should work:
          if(vuid!=null && vuid.length()>0){
            DQ2Locations [] dqLocations = null;
            try{
              dqLocations = getLocations("'"+vuid+"'");
            }
            catch(IOException e){
              error = "WARNING: could not find locations.";
              GridPilot.getClassMgr().getStatusBar().setLabel(error);
              e.printStackTrace();
              logFile.addMessage(error, e);
            }
            //record = Util.split(records[i], "'dsn': ");
            //{'user.FrederikOrellana5894-ATLAS.testdataset': [1]}
            record = MyUtil.split(records[i], "'[^']+': ");
            String name = records[i].replaceFirst(".*'([^']+)': \\[\\d+\\].*", "$1");
            if(name.equals(records[i])){
              name = "";
            }
            // If some selection boxes have been set, use patterns for restricting.
            complete = complete.replaceAll("([^\\.])\\*", "$1.*").replaceFirst("^\\*", ".*");
            incomplete = incomplete.replaceAll("([^\\.])\\*", "$1.*").replaceFirst("^\\*", ".*");
            dsn = dsn.replaceAll("([^\\.])\\*", "$1.*").replaceFirst("^\\*", ".*");
            for(int k=0; k<fields.length; ++k){
              if(fields[k].equalsIgnoreCase("dsn")){
                if(dsn.equals("") || name.matches("(?i)"+dsn)){
                  recordVector.add(name);
                }
                else{
                  exCheck = false;
                }
              }
              else if(fields[k].equalsIgnoreCase("vuid")){
                Debug.debug("Adding vuid: "+vuid, 3);
                recordVector.add(vuid);
              }
              else if(fields[k].equalsIgnoreCase("incomplete") &&
                  dqLocations!=null && dqLocations[0]!=null){
                String incompleteString =
                  MyUtil.arrayToString(dqLocations[0].getIncomplete());
                if(incomplete==null || incomplete.equals("") ||
                    incompleteString.matches("(?i)"+incomplete)){
                  recordVector.add(incompleteString);
                }
                else{
                  exCheck = false;
                }
              }
              else if(fields[k].equalsIgnoreCase("complete") &&
                  dqLocations!=null && dqLocations[0]!=null){
                String completeString =
                  MyUtil.arrayToString(dqLocations[0].getComplete());
                if(complete==null || complete.equals("") ||
                    completeString.matches("(?i)"+complete)){
                  recordVector.add(completeString);
                }
                else{
                  exCheck = false;
                }
              }
              else{
                recordVector.add("");
              }
              if(excludeMap.containsKey(fields[k]) &&
                  recordVector.get(k).toString().matches(
                      "(?i)"+excludeMap.get(fields[k]).toString())){
                exCheck = false;
              }
            }
            if(exCheck){
              Debug.debug("Adding record "+MyUtil.arrayToString(recordVector.toArray()), 3);
              valuesVector.add(recordVector.toArray(new String[recordVector.size()]));
            }
            else{
              Debug.debug("Excluding record ", 2);
            }
          }
          else{
            Debug.debug("ERROR: something went wrong; could " +
                "not parse "+records[i], 2);
          }
        }
      }
      values = new String[valuesVector.size()][fields.length];
      for(int i=0; i<valuesVector.size(); ++i){
        for(int j=0; j<fields.length; ++j){
          values[i][j] = valuesVector.get(i)[j];
        }
        Debug.debug("Adding record "+MyUtil.arrayToString(values[i]), 3);
      }
      DBResult res = new DBResult(fields, values);
      queryResults.put(selectRequest, res);
      return res;
    }
   //---------------------------------------------------------------------
    else if(table.equalsIgnoreCase("file")){
      // "dsn", "lfn", "pfns", "guid", "bytes", "checksum" - to save lookups allow also search on vuid
      String dsn = "";
      String lfn = "";
      String pfns = "";
      String guid = "";      
      String vuid = "";
      String bytes = "";
      String checksum = "";

      // Construct get string
      get = conditions.replaceAll("(?i)\\bvuid = (\\S+)", "vuid=$1");
      get = get.replaceAll("(?i)\\bdsn = (\\S+)", "dsn=$1");
      get = get.replaceAll("(?i)\\blfn = (\\S+)", "lfn=$1");
      get = get.replaceAll("(?i)\\bpfns = (\\S+)", "pfns=$1");
      get = get.replaceAll("(?i)\\bguid = (\\S+)", "guid=$1");
      get = get.replaceAll("(?i)\\bbytes = (\\S+)", "bytes=$1");
      get = get.replaceAll("(?i)\\bchecksum = (\\S+)", "checksum=$1");
      
      get = get.replaceAll("(?i)\\bvuid CONTAINS (\\S+)", "vuid=*$1*");
      get = get.replaceAll("(?i)\\bdsn CONTAINS (\\S+)", "dsn=*$1*");
      get = get.replaceAll("(?i)\\blfn CONTAINS (\\S+)", "lfn=*$1*");
      get = get.replaceAll("(?i)\\bpfns CONTAINS (\\S+)", "pfns=*$1*");
      get = get.replaceAll("(?i)\\bguid CONTAINS (\\S+)", "guid=*$1*");
      get = get.replaceAll("(?i)\\bbytes CONTAINS (\\S+)", "bytes=*$1*");
      get = get.replaceAll("(?i)\\bchecksum CONTAINS (\\S+)", "checksum=*$1*");

      get = get.replaceAll(" AND ", "&");
      
      if(get.matches("(?i).*\\bdsn=(\\S+).*")){
        dsn = get.replaceFirst("(?i).*\\bdsn=(\\S+).*", "$1");
      }
      if(get.matches("(?i).*\\bvuid=(\\S+).*")){
        vuid = get.replaceFirst("(?i).*\\bvuid=(\\S+).*", "$1");
      }
      if(get.matches("(?i).*\\blfn=(\\S+).*")){
        lfn = get.replaceFirst("(?i).*\\blfn=(\\S+).*", "$1");
      }
      if(get.matches("(?i).*\\bpfns=(\\S+).*")){
        pfns = get.replaceFirst("(?i).*\\bpfns=(\\S+).*", "$1");
      }
      if(get.matches("(?i).*\\bguid=(\\S+).*")){
        guid = get.replaceFirst("(?i).*\\bguid=(\\S+).*", "$1");
      }
      if(get.matches("(?i).*\\bbytes=(\\S+).*")){
        bytes = get.replaceFirst("(?i).*\\bbytes=(\\S+).*", "$1");
      }
      if(get.matches("(?i).*\\bchecksum=(\\S+).*")){
        checksum = get.replaceFirst("(?i).*\\bchecksum=(\\S+).*", "$1");
      }
      
      if((vuid==null || vuid.equals("") || vuid.indexOf("*")>-1) &&
          dsn!=null && !dsn.equals("") && dsn.indexOf("*")==-1){
        Debug.debug("dataset name: "+dsn, 3);
        vuid = getDatasetID(dsn);
        Debug.debug("dataset id: "+vuid, 3);
        if(vuid!=null && !vuid.equals("")){
          get += "&vuid="+vuid;
        }
      }
      
      // For files, DQ2 only understands searches on vuid
      if(vuid==null || vuid.equals("")){
        error = "WARNING: could not find vuid. DQ cannot search for file on " +
            "other keys...";
        GridPilot.getClassMgr().getStatusBar().setLabel(error);
        logFile.addMessage(error);
      }
      
      //get = dq2Url+"ws_content/files?"+get;
      //Debug.debug(">>> get string was : "+get, 3);
      
      /*URL url = null;
      try{
        url = new URL(get);
      }
      catch(MalformedURLException e){
        error = "Could not construct "+get;
        Debug.debug(error, 2);
        return null;
      }

      // Get the DQ result
      String str = readGetUrl(url);*/
      String str;
      try{
        str = dq2ReadAccess.getDatasetFiles("['"+vuid+"']");
        // get rid of time stamp.
        str = str.replaceFirst("\\((.*),[^,]+\\)", "$1").trim();
        Debug.debug("Found files : "+str, 3);
      }
      catch(Exception e1){
        error = "Could not get file names for "+vuid;
        Debug.debug(error, 2);
        return null;
      }
      // Discard html
      str = str.replaceFirst("(?i)(?s)<html>.*</html>", "");
      // Check if the result is of the form {...}
      if(!str.matches("^\\{.*\\}$")){
        Debug.debug("ERROR: cannot parse search result "+str, 1);
        return new DBResult(fields, new String[0][fields.length]);
      }
      // Now parse the DQ string and construct DBRecords
      str = str.replaceFirst("^\\{", "");
      str = str.replaceFirst("\\}$", "");
      String [] records = MyUtil.split(str, "\\}, ");
      if(records==null || records.length==0){
        Debug.debug("WARNING: no records found with "+str, 2);
        return new DBResult(fields, new String[0][fields.length]);
      }    
      Vector<String[]> valuesVector = new Vector<String[]>();
      String [] record = null;
      GridPilot.getClassMgr().getStatusBar().stopAnimation();
      if(!GridPilot.getClassMgr().getStatusBar().isCenterComponentSet()){
        pb = MyUtil.setProgressBar(records.length, dbName);
      }
      for(int i=0; i<records.length; ++i){
        if(getStop()){
          break;
        }
        GridPilot.getClassMgr().getStatusBar().setLabel("Record "+(i+1)+" : "+records.length);
        try{
          if(pb!=null){
            pb.setValue(i+1);
          }
        }
        catch(Exception ee){
        }
        Vector<String> recordVector = new Vector<String>();
        boolean exCheck = true;
        record = MyUtil.split(records[i], ": \\{");
        if(record==null || record.length==0){
          Debug.debug("WARNING: could not parse record "+record, 2);
          continue;
        }
        // If the string is the result of a vuid=... request, the
        // split went ok and this should work:
        guid = record[0].replaceAll("'", "").trim();
        lfn = record[1].replaceAll(".*'lfn': '([^']*)'.*", "$1").trim();
        bytes = record[1].replaceAll(".*'filesize': '([^']*)'.*", "$1").trim();
        if(bytes.equals(record[1])){
          bytes = record[1].replaceAll(".*'filesize': (\\w+)[\\W]*.*", "$1").trim();
          if(bytes.equals(record[1])){
            bytes = "";
          }
        }
        checksum = record[1].replaceAll(".*'checksum': '([^']*)'.*", "$1").trim();
        if(checksum.equals(record[1])){
          checksum = "";
        }
        String catalogs = "";
        Debug.debug("Finding PFNs "+lookupPFNs(), 2);
        Debug.debug("Using guid "+guid+" extracted from "+MyUtil.arrayToString(record), 2);
        if(lookupPFNs()){
          PFNResult pfnRes = null;
          try{
            pfnRes = findPFNs(vuid, dsn, guid, lfn, findAll?Database.LOOKUP_PFNS_ALL:Database.LOOKUP_PFNS_ONE);
            catalogs = MyUtil.arrayToString(pfnRes.getCatalogs().toArray());
            bytes = (bytes.equals("")&&pfnRes.getBytes()!=null&&
               !pfnRes.getBytes().equals("")?pfnRes.getBytes():bytes);
            checksum = (checksum.equals("")&&pfnRes.getChecksum()!=null&&
               !pfnRes.getChecksum().equals("")?pfnRes.getChecksum():checksum);
          }
          catch(Exception e){
            e.printStackTrace();
          }
          pfns = MyUtil.arrayToString(pfnRes.getPfns().toArray(new String[pfnRes.getPfns().size()]));
        }
        else{
          pfns = "";
        }
 
        recordVector = new Vector<String>();
        exCheck = true;
        for(int k=0; k<fields.length; ++k){
          if(fields[k].equalsIgnoreCase("lfn")){
            //recordVector.add(lfn);
            recordVector.add(/*(lfn.startsWith("/")?"":"/")+*//*makeAtlasPath(lfn)*/lfn);
          }
          else if(fields[k].equalsIgnoreCase("dsn")){
            recordVector.add(dsn);
          }
          else if(fields[k].equalsIgnoreCase("pfns")){
            recordVector.add(pfns);
          }
          else if(fields[k].equalsIgnoreCase("guid")){
            recordVector.add(guid);
          }
          else if(fields[k].equalsIgnoreCase("vuid")){
            recordVector.add(vuid);
          }
          else if(fields[k].equalsIgnoreCase("catalogs")){
            recordVector.add(catalogs);
          }
          else if(fields[k].equalsIgnoreCase("bytes")){
            recordVector.add(bytes);
          }
          else if(fields[k].equalsIgnoreCase("checksum")){
            recordVector.add(checksum);
          }
          else{
            recordVector.add("");
          }
          if(excludeMap.containsKey(fields[k]) &&
              recordVector.get(k).toString().matches(
                  excludeMap.get(fields[k]).toString())){
            exCheck = false;
          }
        }
        if(exCheck){
          Debug.debug("Adding record "+MyUtil.arrayToString(recordVector.toArray(new String[recordVector.size()])), 3);
          valuesVector.add(recordVector.toArray(new String[recordVector.size()]));
        }
        else{
          Debug.debug("Excluding record ", 2);
        }       

      }
      //setFindPFNs(true);
      if(pb!=null){
        GridPilot.getClassMgr().getStatusBar().removeProgressBar(pb);
        GridPilot.getClassMgr().getStatusBar().clearCenterComponent();
      }
      values = new String[valuesVector.size()][fields.length];
      for(int i=0; i<valuesVector.size(); ++i){
        for(int j=0; j<fields.length; ++j){
          values[i][j] = valuesVector.get(i)[j];
        }
        Debug.debug("Adding record "+MyUtil.arrayToString(values[i]), 3);
      }
      DBResult res = new DBResult(fields, values, idField);
      queryResults.put(selectRequest, res);
      return res;
    }
    else{
      Debug.debug("WARNING: table "+table+" not supported", 1);
      return null;
    }
  }
    
  /**
   * Returns the locations of a set of vuids.
   */
  private DQ2Locations [] getLocations(String vuidsString) throws IOException {
    
    if(getStop()){
      return null;
    }
    
    /*curl --user-agent "dqcurl" --silent --get --insecure --data
    "dsns=%5B%5D" --data "vuids=%5B%27cdced2bd-5217-423a-9690-8b2bb5b48fa8%27%5D"
    http://atlddmpro.cern.ch:8000/dq2/ws_location/dataset
    dsns=[]&vuids=['cdced2bd-5217-423a-9690-8b2bb5b48fa8']*/
    //String url = dq2Url+"ws_location/rpc?"+
    //   "operation=queryDatasetLocations&API="+DQ2_API_VERSION+"&dsns=[]&vuids="+URLEncoder.encode("["+vuidsString+"]", "utf-8");
    
    /* curl -i --insecure -H "User-Agent: dqcurl" -H "TUID: fjob-20090505-12382701" 
      --url "https://atlddmcat.cern.ch:443/dq2/ws_location/rpc?operation=queryDatasetLocations&version=0&API=30" 
      --data "vuids=%5B%27fb7f2a3a-07eb-467e-bb22-66944df82d44%27%5D" --cert /tmp/x509up_u9649 
      --key /tmp/x509up_u9649 */
    
    String ret = null;
    try{
      //Debug.debug(">>> get string was : "+dq2Url+"ws_location/rpc?"+
      //    "operation=queryDatasetLocations&API="+DQ2_API_VERSION+"&dsns=[]&vuids="+"["+vuidsString+"]", 3);
      //ret = readGetUrl(new URL(url));
      //ret = URLDecoder.decode(ret, "utf-8");
      ret = dq2ReadAccess.getDatasetLocations("["+vuidsString+"]");
    }
    catch(Exception e){
      e.printStackTrace();
      error = "WARNING: problem with getLocations. "+e.getMessage();
      throw new IOException(error);
    }
    // Check if the result is of the form {...}
    if(ret==null || ret.matches("^\\{'.*':'.*Exception'.*")){
      error = "WARNING: search returned an error.";
      throw new IOException(error);
    }
    /* Parse the result, e.g.
      {'csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103': 
      {0: ['ASGCDISK', 'BNLPANDA', 'CERNCAF', 'LYONTAPE', 'PICDISK', 'UTA'], 
      1: []}, 'csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103_bnl': 
      {0: ['IFAE', 'IFIC', 'PICDISK', 'UAM'], 1: ['BNLPANDA']}}*/
    String str = ret.replaceFirst("^\\{", "");
    str = str.replaceFirst("\\}$", "");
    String [] records = MyUtil.split(str, "}, ");
    // we return an array of the same length as the vuid array.
    // If DQ2 returned a shorter result, we pad with the last entry.
    int len = MyUtil.split(vuidsString, ", ").length;
    DQ2Locations [] dqLocations = new DQ2Locations[len];
    Debug.debug("Found "+records.length+" records. "+MyUtil.arrayToString(records, " : "), 2);
    for(int i=0; i<len; ++i){
      
      if(getStop()){
        break;
      }

      if(i>records.length-1){
        dqLocations[i] = dqLocations[records.length-1];
        continue;
      }
      String [] recordEntries = null;
      recordEntries = MyUtil.split(records[i], ": \\{0: ");
      Debug.debug(i+" - Found "+records.length+" entries. "+MyUtil.arrayToString(recordEntries, " : "), 2);
      String [] locations = null;
      if(recordEntries!=null && recordEntries.length==1 &&
          recordEntries[0].equals("")){
        dqLocations[i] = new DQ2Locations(null, new String [] {},
            new String [] {});
        continue;
      }
      if(recordEntries==null || recordEntries.length<2){
        error = "WARNING: problem parsing record "+records[i];
        throw new IOException(error);
      }
      locations = MyUtil.split(recordEntries[1], ", 1: ");
      locations[1] = locations[1].replaceFirst("\\}$", "");
      locations[0] = locations[0].replaceFirst("^\\['", "");
      locations[0] = locations[0].replaceFirst("'\\]$", "");
      locations[1] = locations[1].replaceFirst("^\\['", "");
      locations[1] = locations[1].replaceFirst("'\\]$", "");
      Debug.debug("incomplete: "+locations[0], 3);
      Debug.debug("complete: "+locations[1], 3);
      String [] incompleteArr = null;
      if(locations[0].equals("[]")){
        incompleteArr = new String [] {};;
      }
      else{
        incompleteArr = MyUtil.split(locations[0], "', '");
      }
      String [] completeArr = null;
      if(locations[1].equals("[]")){
        completeArr = new String [] {};;
      }
      else{
        completeArr = MyUtil.split(locations[1], "', '");
      }
      // We don't use DQ2Locations.getDatasetName() anywhere,
      // so we don't have to waste time setting the dataset name
      // and just set it to null
      dqLocations[i] = new DQ2Locations(null, incompleteArr,
          completeArr);
      Debug.debug("final incomplete: "+MyUtil.arrayToString(dqLocations[i].getIncomplete()), 3);
      Debug.debug("final complete: "+MyUtil.arrayToString(dqLocations[i].getComplete()), 3);
    }
    return dqLocations;
  }
  
  private String readGetUrl(URL url){
    InputStream is = null;
    DataInputStream dis = null;
    StringBuffer str = new StringBuffer("");
    try{
      is = url.openStream();
      dis = new DataInputStream(new BufferedInputStream(is));
      for(;;){
        int data = dis.read();
        // Check for EOF
        if(data==-1){
          break;
        }
        else{
          str.append((char) data);
        }
      }
      Debug.debug("Get-result: "+str, 3);
    }
    catch(IOException e){
      error = "Could not open "+url;
      e.printStackTrace();
      Debug.debug(error, 2);
      return null;
    }
    finally{
      try{
        dis.close();
      }
      catch(Exception ee){
      }
      try{
        is.close();
      }
      catch(Exception ee){
      }
    }
    return decodeReply(str.toString());
  }

  private String decodeReply(String str){
    String dec = "";
    try{
      dec = URLDecoder.decode(str, "utf-8");
      // remove blank lines
      dec = dec.replaceAll("\\n^\\s+$", "");
      dec = dec.replaceAll("\\r\\n^\\s+$", "");
      if(dec.lastIndexOf("\n")==dec.length()-1){
        dec = dec.substring(0, dec.length()-1);
      }
      Debug.debug("Decoded result: :"+dec+":", 3);
    }
    catch(UnsupportedEncodingException e){
      e.printStackTrace();
    }
    return dec;
  }
  
  public static void main(String [] params){
    //String lfn = "trig1_misal1_mc11.007406.singlepart_singlepi7.recon.log.v12003103_tid003805._00003.job.log.tgz.6";
    String lfn = "trig1_misal1_mc12.006384.PythiaH120gamgam.recon.AOD.v13003002_tid016421";
    String [] lfnMetaData = MyUtil.split(lfn, "\\.");
    String baseStr = lfn.replaceFirst("^(.*)\\._[^\\.]+\\..*$", "$1");
    String [] baseMetaData = MyUtil.split(baseStr, "\\.");
    System.out.println("baseStr: "+baseStr);
    System.out.println("--> length: "+baseMetaData.length);
    String atlasLpn;
    if(baseMetaData.length==6){
      atlasLpn = /*datafiles*/"dq2/"+lfnMetaData[0]+"/"+lfnMetaData[4];
      //atlasLPN += "/"+lfnMetaData[3];
      atlasLpn += "/"+baseStr;
      atlasLpn += "/"+lfn;
    }
    else{
      atlasLpn = lfn;
    }
    System.out.println("atlasLpn: "+atlasLpn);
  }
  
  /**
   * Returns an array of: size, checksum, SURL1, SURL2, ... for the given file name (lfn).
   * The catalog server string must be of the form
   * lfc://lfc-fzk.gridka.de:/grid/atlas/,
   * mysql://dsdb-reader:dsdb-reader1@db1.usatlas.bnl.gov:3306/localreplicas or
   * lrc://dms02.usatlas.bnl.gov:8000/dq2/lrc/PoolFileCatalog.
   * The lfn must be of the form
   * csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103._00001.pool.root;
   * then, in the case of LFC, the following lfn is looked for
   * /grid/atlas/datafiles/
   * csc11/recon/
   * csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103/
   * csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103._00001.pool.root.
   * In the case of lrc:
   * http://dms02.usatlas.bnl.gov:8000/dq2/
   * E.g. to look for trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432._00001.job.log.tgz.1 :
     http://dms02.usatlas.bnl.gov:8000/dq2/lrc/PoolFileCatalog?lfns=trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432._00001.job.log.tgz.1
     (0, '<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n<!-- Edited By POOL -->\n<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n<POOLFILECATALOG>\n\n  <META name="fsize" type="string"/>\n\n  <META name="md5sum" type="string"/>\n\n  <META name="lastmodified" type="string"/>\n\n  <META name="archival" type="string"/>\n\n  <File ID="af97820a-8cee-4b3b-808a-713e6bd21e8e">\n    <physical>\n      <pfn filetype="" name="srm://dcsrm.usatlas.bnl.gov/pnfs/usatlas.bnl.gov/others01/2007/06/trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432_sub0/trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432._00001.job.log.tgz.1"/>\n    </physical>\n    <logical>\n      <lfn name="trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432._00001.job.log.tgz.1"/>\n    </logical>\n    <metadata att_name="archival" att_value="V"/>\n    <metadata att_name="fsize" att_value="2748858"/>\n    <metadata att_name="lastmodified" att_value="1171075105"/>\n    <metadata att_name="md5sum" att_value="8a74295a00637eecdd8443cd36df49a7"/>\n  </File>\n\n</POOLFILECATALOG>\n') 
   */
  
  private String [] lookupPFNs(final String _catalogServer, final String dsn, final String guid,
      final String lfn, final boolean findAll){
    ResThread t = new ResThread(){
      String [] res = null;
      public String [] getString2Res(){
        return res;
      }
      public void run(){
        if(getStop() || !lookupPFNs()){
          return;
        }
        try{
          res = doLookupPFNs(_catalogServer, dsn, guid, lfn, findAll);
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    };
    t.start();
    // If we have to get grid password and decrypt private key, better
    // wait a long time
    int pfnTimeout = fileCatalogTimeout;
    if(!sslActivated){
      pfnTimeout = 0;
    }
    if(!MyUtil.myWaitForThread(t, dbName, pfnTimeout, "lookup pfns", new Boolean(false))){
      error = "WARNING: timed out for "+lfn+" on "+_catalogServer;
      logFile.addMessage(error);
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
    }
    return t.getString2Res();
  }
  
  /**
   * 
   * @param _catalogServer
   * @param dsn
   * @param lfn - can be null
   * @param guid - can be null
   * @param findAll
   * @return an array where the first two entries are bytes, checksums, then
   * follows the PFNs. Bytes and checksum may each be null.
   * @throws RemoteException
   * @throws ServiceException
   * @throws MalformedURLException
   * @throws SQLException
   */
  private String [] doLookupPFNs(String _catalogServer, String dsn, String guid, String lfn, boolean findAll)
     throws Exception {
    // get rid of the :/, which GlobusURL doesn't like
    String catalogServer = _catalogServer.replaceFirst("(\\w):/(\\w)", "$1/$2");
    GlobusURL catalogUrl = new GlobusURL(catalogServer);
    if(catalogUrl.getProtocol().equals("lfc")){
      activateProxySsl();
      return (new LFCLookupPFN(this, lfcConfig, catalogServer, dsn, lfn, guid, findAll, true)).lookup();
    }
    else if(catalogUrl.getProtocol().equals("mysql")){
      activateSsl();
      return (new MySQLLookupPFN(this, catalogServer, lfn, guid, findAll)).lookup();
    }
    else if(catalogUrl.getProtocol().equals("http")){
      activateSsl();
      return (new LRCLookupPFN(this, catalogServer, lfn, guid, findAll)).lookup();
    }
    else{
      error = "ERROR: protocol not supported: "+catalogUrl.getProtocol();
      Debug.debug(error, 1);
      throw new MalformedURLException(error);
    }
  }

  /**
   * Deletes an array of SURLs for the given file name (lfn).
   * The catalog server string must be of the form
   * mysql://dsdb-reader:dsdb-reader1@db1.usatlas.bnl.gov:3306/localreplicas.
   * LFC is not supported.
   */
  private void deleteLFNs(String _catalogServer, String [] lfns)
     throws Exception {
   if(_catalogServer.toLowerCase().startsWith("mysql:")){
     activateSsl();
     deleteLFNsInMySQL(_catalogServer, lfns);
   }
   else if(_catalogServer.toLowerCase().startsWith("lfc:")){
     activateProxySsl();
     deleteLFNsInLFC(_catalogServer, lfns);
   }
   else{
     error = "ERROR: protocol not supported: "+_catalogServer;
     Debug.debug(error, 1);
     throw new MalformedURLException(error);
   }
  }

private void deleteLFNsInLFC(String catalogServer, String[] lfns) throws URISyntaxException, LFCException {
    if(getStop()){
      return;
    }
    LFCServer lfcServer = new LFCServer(lfcConfig, new URI(catalogServer));
    lfcServer.connect();
    String guid;
    for(int i=0; i<lfns.length; ++i){
      try{
        guid = lfcServer.fetchFileDesc(lfns[i]).getGuid();
        lfcServer.deleteFile(guid, lfns[i]);
      }
      catch(Exception e){
        error = "WARNING: problem deleting lfn "+lfns[i]+" on "+catalogServer;
        e.printStackTrace();
        logFile.addMessage(error, e);
      }

      if(getStop()){
        lfcServer.disconnect();
        lfcServer.dispose();
        return;
      }
    }
    lfcServer.disconnect();
    lfcServer.dispose();
  }

private void deleteLFNsInMySQL(String _catalogServer, String [] lfns)
     throws Exception {
    
    if(getStop()){
      return;
    }
    
    // get rid of the :/, which GlobusURL doesn't like
    String catalogServer = _catalogServer.replaceFirst("(\\w):/(\\w)", "$1/$2");
    GlobusURL catalogUrl = new GlobusURL(catalogServer);
    if(catalogUrl.getProtocol().equals("mysql")){
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
        activateSsl();
        user = GridPilot.getClassMgr().getSSL().getGridDatabaseUser();
      }
      // Make the connection
      GridPilot.getClassMgr().establishJDBCConnection(
          alias, driver, database, user, passwd, gridAuth,
          connectTimeout, socketTimeout, lrcPoolSize);
      Connection conn = getDBConnection(alias);
      String lfn = null;
      int rowsAffected = 0;
      for(int i=0; i<lfns.length; ++i){
        
        if(getStop()){
          break;
        }

        try{
          lfn = lfns[i];
          // First query the t_lfn table to get the guid
          String req = "SELECT guid FROM t_lfn WHERE lfname ='"+lfn+"'";
          ResultSet rset = null;
          String guid = null;
          Vector<String> resultVector = new Vector<String>();
          Debug.debug(">> "+req, 2);
          rset = conn.createStatement().executeQuery(req);
          while(rset.next()){
            resultVector.add(rset.getString("guid"));
          }
          if(resultVector.size()==0){
            error = "ERROR: no guid with found for lfn "+lfn;
            throw new SQLException(error);
          }
          else if(resultVector.size()>1){
            error = "WARNING: More than one ("+resultVector.size()+") guids with found for lfn "+lfn;
            logFile.addMessage(error);
          }
          guid = resultVector.get(0);
          // Now delete this guid from the t_lfn, t_pfn and t_meta tables
          req = "DELETE FROM t_lfn WHERE guid = '"+guid+"'";
          Debug.debug(">> "+req, 2);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not delete guid "+guid+" from t_lfn on "+catalogServer;
            logFile.addMessage(error);
          }
          req = "DELETE FROM t_pfn WHERE guid = '"+guid+"'";
          Debug.debug(">> "+req, 2);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not delete guid "+guid+" from t_pfn on "+catalogServer;
            logFile.addMessage(error);
          }
          req = "DELETE FROM t_meta WHERE guid = '"+guid+"'";
          Debug.debug(">> "+req, 2);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not delete guid "+guid+" from t_meta on "+catalogServer;
            logFile.addMessage(error);
          }
        }
        catch(Exception e){
          error = "WARNING: problem deleting lfn "+lfns[i]+" on "+catalogServer;
          e.printStackTrace();
          logFile.addMessage(error, e);
        }
      }
      conn.close();
    }
    else{
      error = "ERROR: protocol not supported: "+catalogUrl.getProtocol();
      Debug.debug(error, 1);
      throw new MalformedURLException(error);
    }
  }
  
  /**
   * Registers an array of SURLs for the given file name (lfn).
   * In the case of MySQL, the catalog server string must be of the form
   * mysql://dsdb-reader:dsdb-reader1@db1.usatlas.bnl.gov:3306/localreplicas.
   * @throws URISyntaxException 
   * @throws LFCException 
   */

  private void registerLFNs(String _catalogServer, String path, String [] guids,
      String [] lfns, String [] pfns, String [] sizes,
      String [] checksums) throws Exception {
    if(_catalogServer.toLowerCase().startsWith("mysql:")){
      activateSsl();
      registerLFNsInMySQL(_catalogServer, guids, lfns, pfns, sizes,
          checksums);
    }
    else if(_catalogServer.toLowerCase().startsWith("lfc:")){
      activateProxySsl();
      registerLFNsInLFC(_catalogServer, path, guids, lfns, pfns, sizes,
          checksums);
    }
    else{
      error = "ERROR: protocol not supported: "+_catalogServer;
      Debug.debug(error, 1);
      throw new MalformedURLException(error);
    }
  }

  private void registerLFNsInLFC(String _catalogServer, String path, String [] guids,
      String [] lfns, String [] pfns, String [] sizes,
      String [] checksums)
     throws RemoteException, ServiceException, MalformedURLException, SQLException, URISyntaxException, LFCException {
    if(getStop()){
      return;
    }
    LFCServer lfcServer = new LFCServer(lfcConfig, new URI(_catalogServer));
    lfcServer.connect();
    // Make sure the directory exists
    boolean dirExists = false;
    try{
      dirExists = lfcServer.listDirectory(path)!=null;
    }
    catch(Exception e){
      e.printStackTrace();
    }
    if(!dirExists){
      Debug.debug("Creating directory in LFC: "+path, 2);
      lfcServer.mkdir(path);
    }
    int colonIndex;
    String chksumType;
    String chksum;
    long size;
    String fullName;
    for(int i=0; i<lfns.length; ++i){
      try{
        size = -1L;
        try{
          size = Long.parseLong(sizes[i]);
        }
        catch(Exception ee){
          ee.printStackTrace();
        }
        chksumType = null;
        chksum = null;
        if(checksums[i]!=null && !checksums[i].trim().equals("")){
          colonIndex = checksums[i].indexOf(":");
          if(colonIndex>0){
            chksumType = checksums[i].substring(0, colonIndex);
            chksum = checksums[i].substring(colonIndex+1);
          }
          else{
            chksumType = "md5";
            chksum = checksums[i];
          }
        }
        fullName = path+lfns[i];
        //lfcServer.register(new URI(pfns[i]), fullName, size);
        Debug.debug("Registering file in LFC: "+guids[i]+":"+fullName, 2);
        lfcServer.registerEntry(fullName, guids[i]);
        Debug.debug("Registering file in LFC: "+pfns[i]+":"+fullName+":"+size, 2);
        FileDesc fileDesc = lfcServer.fetchFileDesc(guids[i], true);
        lfcServer.addReplica(fileDesc, new URI(pfns[i]));
        try{
          // This will not work when registering files on a remote server (chksum is not available)
          Debug.debug("Setting size of file in LFC: "+guids[i]+":"+size+":"+chksumType+":"+chksum, 2);
          lfcServer.setFileSize(guids[i], size, chksumType, chksum);
        }
        catch(Exception ee){
          ee.printStackTrace();
        }
      }
      catch(Exception e){
        error = "WARNING: problem inserting lfn or lfn/pfn "+lfns[i]+"/"+pfns[i]+" on "+_catalogServer;
        logFile.addMessage(error, e);
        GridPilot.getClassMgr().getStatusBar().setLabel(error);
      }
      if(getStop()){
        lfcServer.disconnect();
        lfcServer.dispose();
        return;
      }
    }
    lfcServer.disconnect();
    lfcServer.dispose();
  }

  private void registerLFNsInMySQL(String _catalogServer, String [] guids,
      String [] lfns, String [] pfns, String [] sizes,
      String [] checksums)
     throws RemoteException, ServiceException, MalformedURLException, SQLException {
    
    if(getStop()){
      return;
    }
    
    // get rid of the :/, which GlobusURL doesn't like
    String catalogServer = _catalogServer.replaceFirst("(\\w):/(\\w)", "$1/$2");
    GlobusURL catalogUrl = new GlobusURL(catalogServer);
    Connection conn = null;
    try{
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
        activateSsl();
        user = GridPilot.getClassMgr().getSSL().getGridDatabaseUser();
      }
      // Make the connection
      GridPilot.getClassMgr().establishJDBCConnection(
          alias, driver, database, user, passwd, gridAuth,
          connectTimeout, socketTimeout, lrcPoolSize);
      conn = getDBConnection(alias);
      int rowsAffected = 0;
      String req = null;
      // Do the insertions in t_lfn and t_pfn
      for(int i=0; i<lfns.length; ++i){
        
        if(getStop()){
          break;
        }
        
        try{
          req = "INSERT INTO t_lfn (lfname, guid) VALUES " +
             "('"+lfns[i]+"', '"+guids[i]+"')";
          Debug.debug(">> "+req, 3);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not insert lfn "+lfns[i]+" on "+catalogServer;
            logFile.addMessage(error);
          }
          req = "INSERT INTO t_pfn (pfname, guid) VALUES " +
          "('"+pfns[i]+"', '"+guids[i]+"')";
          Debug.debug(">> "+req, 3);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not insert pfn "+pfns[i]+" on "+catalogServer;
            logFile.addMessage(error);
          }
        }
        catch(Exception e){
          error = "ERROR: could not insert lfn or lfn/pfn "+lfns[i]+"/"+pfns[i]+" on "+catalogServer;
          logFile.addMessage(error, e);
          GridPilot.getClassMgr().getStatusBar().setLabel(error);
        }
        // First, just try and create the metadata entry - in case it's not there already
        try{ 
          req = "INSERT INTO t_meta (guid) VALUES " + "('"+guids[i]+"')";
          Debug.debug(">> "+req, 3);
          rowsAffected = conn.createStatement().executeUpdate(req);
          Debug.debug("rowsAffected "+rowsAffected, 3);
        }
        catch(Exception e){
          e.printStackTrace();
        }
        try{
          req = "UPDATE t_meta SET ";
          boolean comma = false;
          // if a size or a checksum is specified, use it
          if(sizes!=null && sizes[i]!=null && !sizes[i].equals("")){
            req += "fsize = '"+sizes[i]+"'";
            comma = true;
          }
          if(checksums!=null && checksums[i]!=null && checksums[i].startsWith("md5:")){
            if(comma){
              req += ", ";
            }
            req += "md5sum = '"+checksums[i].substring(4)+"'";
            comma = true;
          }
          if(comma){
            req += " WHERE guid ='"+guids[i]+"'";
            rowsAffected = conn.createStatement().executeUpdate(req);
          }
        }
        catch(Exception e){
          error = "WARNING: could flag guid "+guids[i]+" for write in t_meta on "+catalogServer;
          logFile.addMessage(error);
        }
      }
      conn.close();
    }
    catch(Exception e){
      try{
        conn.close();
      }
      catch(Exception ee){
      }
    }
  }
  
  /**
   * Checks if a site is in the list of ignored sites.
   * @param site
   * @return true if the site should be skipped.
   */
  private boolean checkSkip(String site){
    if(skipSites!=null && skipSites.length>0){
      for(int j=0; j<skipSites.length; ++j){
        if(site.matches("(?i)"+skipSites[j])){
          return true;
        }
      }
    }
    return false;
  }
  

  /**
   * Find the locations of a given dataset.
   * @param vuid dataset ID
   * @return Vector of Strings (site names)
   */
  private Vector<String> getOrderedLocations(String vuid){
    
    // First check cache
    if(dqLocationsCache.containsKey(vuid)){
      return dqLocationsCache.get(vuid);
    }
    
    DQ2Locations dqLocations = null;
    try{
      dqLocations = getLocations("'"+vuid+"'")[0];
    }
    catch(IOException e){
      error = "WARNING: could not find locations.";
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
      e.printStackTrace();
      logFile.addMessage(error, e);
    }
    
    // construct vector of all locations
    Vector<String> locations = new Vector<String>();
    // make sure homeServer is first in the list, excluding ignored sites
    for(int i=0; i<dqLocations.getComplete().length; ++i){
      if(dqLocations.getComplete()[i].equalsIgnoreCase(homeSite) &&
          !checkSkip(homeSite)){
        locations.add(homeSite);
        break;
      }
    }
    for(int i=0; i<dqLocations.getIncomplete().length; ++i){
      if(dqLocations.getIncomplete()[i].equalsIgnoreCase(homeSite) &&
          !checkSkip(homeSite)){
        locations.add(homeSite);
        break;
      }
    }
    // add the preferred sites if there, excluding ignored sites
    boolean added = false;
    for(int ii=0; ii<preferredSites.length; ++ii){
      added = false;
      for(int i=0; i<dqLocations.getComplete().length; ++i){
        if(!dqLocations.getComplete()[i].equalsIgnoreCase(homeSite) &&
            dqLocations.getComplete()[i].matches("(?i)"+preferredSites[ii]) &&
            !checkSkip(preferredSites[ii])){
          locations.add(preferredSites[ii]);
          added = true;
          break;
        }
      }
      if(added){
        continue;
      }
      for(int i=0; i<dqLocations.getIncomplete().length; ++i){
        if(!dqLocations.getIncomplete()[i].equalsIgnoreCase(homeSite) &&
            dqLocations.getIncomplete()[i].equalsIgnoreCase(preferredSites[ii]) &&
            !checkSkip(preferredSites[ii])){
          locations.add(preferredSites[ii]);
          break;
        }
      }
    }
    // add the rest, excluding ignored sites
    HashSet<String> preferredSet = new HashSet<String>();
    Collections.addAll(preferredSet, preferredSites);
    for(int i=0; i<dqLocations.getComplete().length; ++i){
      if(!dqLocations.getComplete()[i].equalsIgnoreCase(homeSite) &&
          !preferredSet.contains(dqLocations.getComplete()[i]) &&
          !checkSkip(dqLocations.getComplete()[i])){
        locations.add(dqLocations.getComplete()[i]);
      }
    }
    for(int i=0; i<dqLocations.getIncomplete().length; ++i){
      if(!dqLocations.getIncomplete()[i].equalsIgnoreCase(homeSite) &&
          !preferredSet.contains(dqLocations.getIncomplete()[i]) &&
          !checkSkip(dqLocations.getIncomplete()[i])){
        locations.add(dqLocations.getIncomplete()[i]);
      }
    }
    Debug.debug("Found locations "+MyUtil.arrayToString(locations.toArray()), 3);
    dqLocationsCache.put(vuid, locations);
    return locations;
  }
  
  private class PFNResult{
    PFNResult(){
      setPfns(new Vector<String>());
      setCatalogs(new Vector<String>());
      setBytes("");
      setChecksum("");
    }
    private Vector<String> pfns;
    private Vector<String> catalogs;
    private String bytes;
    private String checksum;
    protected void setPfns(Vector<String> _pfns){
      pfns = _pfns;
    }
    protected void setCatalogs(Vector<String> _catalogs){
      catalogs = _catalogs;
    }
    protected void setBytes(String _bytes){
      bytes = _bytes;
    }
    protected void setChecksum(String _checksum){
      checksum = _checksum;
    }
    protected Vector<String> getPfns(){
      return pfns;
    }
    protected Vector<String> getCatalogs(){
      return catalogs;
    }
    protected String getBytes(){
      return bytes;
    }
    protected String getChecksum(){
      return checksum;
    }
  }
  
  private PFNResult findPFNs(String vuid, String dsn, String guid, String lfn, int findAll){
    if(dsn.endsWith("/")){
      return findPFNsOfContainer(vuid, dsn, guid, lfn, findAll);
    }
    else{
      return doFindPFNs(vuid, dsn, guid, lfn, findAll);
    }
  }
  
  private PFNResult findPFNsOfContainer(String vuid, String dsn, String guid, String lfn, int findAll){
    PFNResult res = new PFNResult();
    try{
      String [] childrenDsns = getChildrenDatasetNames(dsn);
      String childVuid;
      PFNResult partRes;
      Vector<String> existingPfns;
      String pfn;
      for(int i=0; i<childrenDsns.length; ++i){
        if(getStop() || !lookupPFNs()){
          return res;
        }
        childVuid = getDatasetID(childrenDsns[i]);
        partRes = doFindPFNs(childVuid, childrenDsns[i], guid, lfn, findAll);
        res.getCatalogs().addAll(partRes.getCatalogs());
        if(res.getBytes()==null || res.getBytes().trim().equals("")){
          res.setBytes(partRes.getBytes());
        }
        if(res.getChecksum()==null || res.getChecksum().trim().equals("")){
          res.setChecksum(partRes.getChecksum());
        }
        existingPfns = res.getPfns();
        for(Iterator<String>it=partRes.getPfns().iterator(); it.hasNext();){
          pfn = it.next();
          if(!existingPfns.contains(pfn)){
            existingPfns.add(pfn);
          }
        }
        res.setPfns(existingPfns);
      }
    }
    catch(InterruptedException e){
      e.printStackTrace();
      return res;
    }
    return res;
  }
  
  private PFNResult doFindPFNs(String vuid, String dsn, String _guid, String lfn, int findAll){
    // Query all with a timeout of 5 seconds.    
    // First try the home server if configured.
    // Next try the locations with complete datasets, then the incomplete.
    // For each LRC catalog try both the mysql and the http interface.
    String lfName = lfn;
    String guid = _guid;
    Vector<String> locations = getOrderedLocations(vuid);
    String [] locationsArray = locations.toArray(new String[locations.size()]);
    PFNResult res = new PFNResult();
    if(getStop() || !lookupPFNs()){
      return res;
    }
    try{
      Debug.debug("Checking locations "+MyUtil.arrayToString(locationsArray), 3);
      for(int i=0; i<locationsArray.length; ++i){
        Debug.debug("Checking location "+locationsArray[i], 3);
        if(locationsArray[i]==null || locationsArray[i].matches("\\s*")){
          continue;
        }
        if(getStop() || !lookupPFNs()){
          return res;
        }
        String [] pfns = null;
        try{
          Debug.debug("Querying TOA for "+i+":"+locationsArray[i], 2);
          String catalogServer = null;
          String fallbackServer = null;
          if(homeSite!=null && 
              locationsArray[i].equalsIgnoreCase(homeSite)){
            catalogServer = toa.getFileCatalogServer(locationsArray[i], false); 
          }
          else{
            if(!httpSwitches.contains(locationsArray[i])){
              catalogServer = toa.getFileCatalogServer(locationsArray[i], false);
              fallbackServer = toa.getFileCatalogServer(locationsArray[i], true);
            }
            else{
              catalogServer = toa.getFileCatalogServer(locationsArray[i], true);
              fallbackServer = toa.getFileCatalogServer(locationsArray[i], false);
            }
          }
          if(catalogServer==null || catalogServer.trim().equals("")){
            logFile.addMessage("WARNING: could not find catalog server for "+
                locationsArray[i]);
            continue;
          }
          if(findAll==Database.LOOKUP_PFNS_ONLY_CATALOG_URLS){
            res.getCatalogs().add(catalogServer);
            continue;
          }
          Debug.debug("Querying "+i+"-->"+catalogServer+" for "+lfName, 2);
          GridPilot.getClassMgr().getStatusBar().setLabel("Querying "+catalogServer);
          try{
            try{
              pfns = lookupPFNs(catalogServer, dsn, guid, lfName, findAll==Database.LOOKUP_PFNS_ALL);
            }
            catch(Exception e){
              e.printStackTrace();
            }
            if(fallbackServer!=null && (pfns==null || pfns.length==0)){
              Debug.debug("No PFNs found, trying fallback "+fallbackServer, 2);
              pfns = lookupPFNs(fallbackServer, dsn, guid, lfName, findAll==Database.LOOKUP_PFNS_ALL);
              catalogServer = fallbackServer;
              if(pfns!=null && pfns.length>2){
                Debug.debug("Switching to http for "+locationsArray[i], 2);
                httpSwitches.add(locationsArray[i]);
              }
            }
            if(pfns!=null && pfns.length>2){
              res.getCatalogs().add(catalogServer);
              res.setBytes(pfns[0]);
              res.setChecksum(pfns[1]);
              for(int n=2; n<pfns.length; ++n){
                res.getPfns().add(pfns[n]);
              }
            }
          }
          catch(Throwable t){
            t.printStackTrace();
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName, t);
          }
        }
        catch(Exception e){
          e.printStackTrace();
        }
        Debug.debug("Found PFNs "+MyUtil.arrayToString(pfns, "-->")+". All PFNs now "+res.getPfns(), 2);
        // break out after first location, if "Find all" is not checked
        if(findAll!=Database.LOOKUP_PFNS_ALL && !res.getPfns().isEmpty() && res.getPfns().get(0)!=null){
          break;
        }
        // if we did not find anything on this location, put it last in the
        // HashMap of locations
        if(pfns==null || pfns.length<3 || pfns[2]==null || pfns[2].equals("")){
          Vector<String> tl = dqLocationsCache.get(vuid);
          int j=0;
          for(j=0; j<tl.size(); ++j){
            if(tl.get(j).equals(locationsArray[i])){
              break;
            }
          }
          int len = tl.size();
          Debug.debug("Deprecating location: "+locationsArray[i]+" -->"+
              j+":"+len+":"+(-len+j+1)+"-->"+findAll, 2);
          Collections.rotate(dqLocationsCache.get(vuid).subList(j, len),
              len-j-1);
          Debug.debug("New location cache for "+vuid+
              ": "+MyUtil.arrayToString((dqLocationsCache.get(vuid)).toArray()), 2);
        }
      }
      if(findAll==Database.LOOKUP_PFNS_ONLY_CATALOG_URLS){
        return res;
      }
      // Eliminate duplicates
      Vector<String> pfnShortVector = new Vector<String>();
      String tmpPfn = null;
      for(Iterator<String> it=res.getPfns().iterator(); it.hasNext();){
        tmpPfn = it.next();
        if(!pfnShortVector.contains(tmpPfn)){
          pfnShortVector.add(tmpPfn);
        }
      }
      res.setPfns(pfnShortVector);
      Debug.debug("pfnVector:"+res.getPfns().size(), 2);
      GridPilot.getClassMgr().getStatusBar().setLabel("Querying done.");
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return res;
  }

  public String getDatasetName(String datasetID){
    DBRecord dataset = getDataset(datasetID);
    return dataset.getValue("dsn").toString();
  }

  public String getDatasetID(String datasetName){
    DBResult res = select("SELECT * FROM dataset WHERE dsn = "+datasetName,
       "vuid", true);
    if(res!=null && res.values.length>1){
      Debug.debug("WARNING: inconsistent dataset catalog; " +
          res.values.length + " entries with dsn "+datasetName, 1);
    }
    String ret = "-1";
    try{
      ret = res.get(0).getValue("vuid").toString();
    }
    catch(Exception e){
      error = "Could not get dataset ID from "+datasetName+". "+e.getMessage();
      e.printStackTrace();
      return "-1";
    }
    if(ret==null || ret.equals("")){
      return "-1";
    }
    Debug.debug("Returning id "+ret+" for "+datasetName, 3);
    return ret;
  }

  public DBRecord getDataset(String datasetID){
    DBResult res = select("SELECT * FROM dataset WHERE vuid = "+datasetID,
        "vuid", true);
    if(res.values.length>1){
      Debug.debug("WARNING: inconsistent dataset catalog; " +
          res.values.length + " entries with vuid "+datasetID, 1);
    }
    return res.get(0);
  }
  
  public String[] getFieldNames(String table){
    try{
      Debug.debug("getFieldNames for table "+table, 3);
      if(table.equalsIgnoreCase("file")){
        return new String [] {"dsn", "lfn", "catalogs", "pfns", "guid", "bytes", "checksum"};
      }
      else if(table.equalsIgnoreCase("dataset")){
        return new String [] {"dsn", "vuid", "incomplete", "complete"};
      }
    }
    catch(Exception e){
      e.printStackTrace();
      Debug.debug(e.getMessage(),1);
    }
    return null;
  }
  
  public String getFileID(String datasetName, String fileName){
    DBRecord file = getFileFromName(datasetName, fileName, Database.LOOKUP_PFNS_NONE);
    return file.getValue("guid").toString();
  };
  
  public DBRecord getFile(String dsn, String fileID, int findAllPFNs){
    return getFile(dsn, fileID, "guid", findAllPFNs);
  }

  public DBRecord getFileFromName(String dsn, String fileName, int findAllPFNs){
    return getFile(dsn, fileName, "lfn", findAllPFNs);
  }

  public DBRecord getFile(String dsn, String idOrName, String idOrNameField, int findAllPFNs){
    // "dsn", "lfn", "pfns", "guid"
    
    // NOTICE: this query is NOT supported by DQ2.
    /*DBResult res = select("SELECT * FROM file WHERE guid = "+fileID,
        "guid", true);
    if(res.values.length>1){
      Debug.debug("WARNING: inconsistent dataset catalog; " +
          res.values.length + " entries with guid "+fileID, 1);
      return null;
    }
    return res.getRow(0);*/
    
    // Alternative, hacky (and slow) solution
    String vuid = getDatasetID(dsn);
    DBResult files = getFiles(vuid);
    String lfn = null;
    String guid = null;
    String bytes = "";
    String checksum = "";
    if(idOrNameField.equalsIgnoreCase("guid")){
      DBRecord row;
      try{
        row = files.getRow(idOrName);
        lfn = row.getValue("lfn").toString();
        lfn = lfn.replaceFirst(".*/([^/]+)", "$1");
        guid = row.getValue("guid").toString();
        bytes = row.getValue("bytes").toString();
        checksum = row.getValue("checksum").toString();        
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
    if(lfn==null){
      for(int i=0; i<files.values.length; ++i){
        Debug.debug("matching file id or name "+idOrName, 3);
        if(files.getValue(i, idOrNameField).toString().equals(idOrName)){
          lfn = files.getValue(i, "lfn").toString();
          lfn = lfn.replaceFirst(".*/([^/]+)", "$1");
          guid = files.getValue(i, "guid").toString();
          bytes = files.getValue(i, "bytes").toString();
          checksum = files.getValue(i, "checksum").toString();
          break;
        };
      }
    }
    
    if(lfn==null){
      return null;
    }
    
    // Get the pfns
    Vector<String> resultVector = new Vector<String>();
    String pfns = "";
    String catalogs = "";
    if(findAllPFNs==Database.LOOKUP_PFNS_ONE || findAllPFNs==Database.LOOKUP_PFNS_ALL){
      PFNResult pfnRes = findPFNs(vuid, dsn, guid, lfn, findAllPFNs);
      catalogs = MyUtil.arrayToString(pfnRes.getCatalogs().toArray());
      for(int j=0; j<pfnRes.getPfns().size(); ++j){
        resultVector.add(pfnRes.getPfns().get(j));
      }
      Debug.debug("pfnVector --> "+pfnRes.getPfns(), 3);
      pfns = MyUtil.arrayToString(resultVector.toArray(new String[resultVector.size()]));
      bytes = (bytes.equals("")&&pfnRes.getBytes()!=null&&
          !pfnRes.getBytes().equals("")?pfnRes.getBytes():bytes);
      checksum = (checksum.equals("")&&pfnRes.getChecksum()!=null&&
          !pfnRes.getChecksum().equals("")?pfnRes.getChecksum():checksum);
    }
    else if(findAllPFNs==Database.LOOKUP_PFNS_ONLY_CATALOG_URLS){
      PFNResult pfnRes = findPFNs(vuid, dsn, guid, lfn, Database.LOOKUP_PFNS_ONLY_CATALOG_URLS);
      catalogs = MyUtil.arrayToString(pfnRes.getCatalogs().toArray());
    }
        
    return new DBRecord(fileFields, new String [] {dsn, lfn, catalogs, pfns, guid, bytes, checksum});

  }

  public String [][] getFileURLs(String datasetName, String fileID, boolean findAll){
    String [][] ret = new String [2][];
    try{
      DBRecord file = getFile(datasetName, fileID, findAll?Database.LOOKUP_PFNS_ALL:Database.LOOKUP_PFNS_ONE);
      ret[0] = MyUtil.split((String) file.getValue("catalogs"));
      ret[1] = MyUtil.splitUrls((String) file.getValue("pfns"));
      Debug.debug("catalogs: "+MyUtil.arrayToString(ret[0]), 2);
      Debug.debug("pfns: "+MyUtil.arrayToString(ret[1]), 2);
    }
    catch(Exception e){
      e.printStackTrace();
      Debug.debug("WARNING: could not get URLs. "+e.getMessage(), 1);
    }
    return ret;
  }

  public boolean deleteFiles(String datasetID, String [] fileIDs){
    return deleteFiles(datasetID, fileIDs, false);
  }
  /**
   * Delete file entries in DQ file catalog, delete the corresponding physical files
   * and the entries on MySQL home server.
   */
  public boolean deleteFiles(String datasetID, String [] fileIDs, boolean cleanup){

    if(getStop()){
      return false;
    }
    
    // Find the LFNs to delete.
    // NOTICE: we are assuming that there is a one-to-one mapping between
    //         lfns and guids. This is not necessarily the case...
    // TODO: improve
    DBResult allFiles = null;
    Vector<String> toDeleteGUIDsVec = new Vector<String>();
    Vector<String> toDeleteLFNsVec = new Vector<String>();
    String [] toDeleteGuids = null;
    String [] toDeleteLfns = null;
    try{
      allFiles = getFiles(datasetID);
      for(int i=0; i<allFiles.values.length; ++i){
        if(fileIDs==null){
          toDeleteGUIDsVec.add((String) allFiles.getValue(i, "guid"));
          toDeleteLFNsVec.add((String) allFiles.getValue(i, "lfn"));
          continue;
        }
        for(int j=0; j<fileIDs.length; ++j){
          if(fileIDs[j].equalsIgnoreCase((String) allFiles.getValue(i, "guid"))){
            toDeleteGUIDsVec.add(fileIDs[j]);
            toDeleteLFNsVec.add((String) allFiles.getValue(i, "lfn"));
            break;
          }
        }
      }
      toDeleteGuids = toDeleteGUIDsVec.toArray(new String[toDeleteLFNsVec.size()]);
      toDeleteLfns = toDeleteLFNsVec.toArray(new String[toDeleteLFNsVec.size()]);
    }
    catch(Exception e){
      error = "ERROR: could not delete files "+MyUtil.arrayToString(fileIDs)+" from " +
         datasetID+". Aborting";
      e.printStackTrace();
      logFile.addMessage(error, e);
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
      return false;
    }

    // First, if cleanup is true, check if files are ONLY registered in home file catalogue;
    // if so, delete the physical files and clean up the home file catalogue.
    boolean deletePhysOk = true;
    DQ2Locations locations = null;
    boolean deleteFromCatalogOK = true;
    try{
      locations = getLocations("'"+datasetID+"'")[0];
      if(cleanup){
        if(!forceFileDelete){
          checkIfOnlyOneLocation(locations);
        }
      }
    }
    catch(Exception e){
      error = "WARNING: problem with locations of "+MyUtil.arrayToString(fileIDs)+
      ". There may be orphaned LFNs in DQ2, " +
      "and/or wrongly registered locations in DQ2 and/or " +
      "wrongly registered file catalog entries.";
      logFile.addMessage(error, e);
      return false;
    }
    if(cleanup){
      // Delete physical files
      int deleted = 0;
      String datasetName = getDatasetName(datasetID);
      try{
        deleted = deletePhysicalFiles(datasetID, datasetName, toDeleteGuids, toDeleteLfns);
      }
      catch(Exception e){
        deletePhysOk = false;
      }
      deletePhysOk = (deleted==toDeleteLfns.length);
      
      if(getStop()){
        return false;
      }
      
      // Remove entries from MySQL catalog
      // Notice: we delete ONLY one entry per lfn, the one in the home catalog
      GridPilot.getClassMgr().getStatusBar().setLabel("Cleaning up home catalog...");
      try{
        deleteLFNs(toa.getFileCatalogServer(homeSite, false), toDeleteLfns);
      }
      catch(Exception e){
        deleteFromCatalogOK = false;
        logFile.addMessage("WARNING: failed to delete LFNs "+MyUtil.arrayToString(toDeleteLfns)+
            ". Please delete them by hand.", e);
      }
    }
    
    boolean allFilesDeleted = (allFiles.values.length==fileIDs.length);
    boolean eraseFromDQ2Ok = true;
    if(forceFileDelete || !cleanup || deleteFromCatalogOK){
      eraseFromDQ2Ok = eraseFromDQ2(datasetID, deletePhysOk, fileIDs, allFilesDeleted, locations);
    }
    
    clearCacheEntries("file");
    clearCacheEntries("dataset");
    
    return eraseFromDQ2Ok && deletePhysOk && deleteFromCatalogOK;
  }
  
  /**
   * Checks if the home catalog is the only registered location.
   * @throws Exception if more than one catalog is registered or 
   * only one is registered and it is not the home catalog.
   */
  private void checkIfOnlyOneLocation(DQ2Locations locations) throws Exception{
    //GridPilot.getClassMgr().getStatusBar().setLabel("Finding locations...");
    // Check that there is only one location registered,
    // that it is the homeServer and that it has a MySQL Alias
    Debug.debug("Checking locations "+MyUtil.arrayToString(locations.getComplete())+
        " -- "+MyUtil.arrayToString(locations.getIncomplete())+
        (locations.getIncomplete().length+locations.getComplete().length), 2);
    if(locations.getIncomplete().length+locations.getComplete().length>1){
      error = "More than one location registered: "+
      MyUtil.arrayToString(locations.getIncomplete())+" "+
      MyUtil.arrayToString(locations.getComplete());
      throw new Exception(error);
    }
    String location = null;
    if(locations.getIncomplete().length==1){
      location = locations.getIncomplete()[0];
    }
    else if(locations.getComplete().length==1){
      location = locations.getComplete()[0];
    }
    if(!location.equalsIgnoreCase(homeSite)){
      error = "Can only delete files on home catalog server or MySQL alias. Ignoring "+location;
      throw new Exception(error);
    }
  }
  
  // Deregister the LFNs from this vuid on DQ2.
  // NOTICE that this changes the vuid of the dataset...
  private boolean eraseFromDQ2(String datasetID, boolean deletePhysOk,
      String [] fileIDs, boolean allFilesDeleted, DQ2Locations locations){
    try{
      // If all files were deleted clear the locations (or the home location)
      if(deletePhysOk){
        // If only the home location is registered and files have been deleted
        // succesfully, remove the files from DQ2
        if(!forceFileDelete){
          checkIfOnlyOneLocation(locations);
        }
        // This will only be reached if the deletion of the physical files went well
        // or cleanup was not selected.
        dq2WriteAccess.deleteFiles(datasetID, fileIDs);
        if(allFilesDeleted){
          dq2WriteAccess.deleteFromSite(datasetID, homeSite);
        }
      }
    }
    catch(Exception eee){
      eee.printStackTrace();
      error = "WARNING: could not connect delete files from DQ2.";
      logFile.addMessage(error, eee);
      return false;
    }
    return true;
  }
  
  private int deletePhysicalFiles(String datasetID, String datasetName, String [] toDeleteGuids, String [] toDeleteLfns)
     throws IOException, InterruptedException{
    int deleted = 0;
    // Delete the physical files.
    // First store them all in a Vector.
    Vector<String> pfns = new Vector<String>();
    String [] pfnsArr = null;
    for(int i=0; i<toDeleteLfns.length; ++i){
      if(getStop()){
        throw new InterruptedException();
      }
      // Get the pfns
      try{
        pfnsArr = lookupPFNs(homeSite, datasetName, toDeleteGuids[i], toDeleteLfns[i], true);
        if(pfnsArr!=null && pfnsArr.length>2){
          for(int j=2; j<pfnsArr.length; ++j){
            Debug.debug("Will delete "+pfnsArr[j], 2);
            if(pfnsArr[j]!=null && !pfnsArr[j].equals("")){
              pfns.add(pfnsArr[j]);
            }
          }
        }
      }
      catch(Exception e){
        logFile.addMessage("WARNING: failed to find PFNs to delete, "+
            MyUtil.arrayToString(toDeleteLfns)+" on "+homeSite+". Please delete them by hand.");
        return deleted;
      }
    }      
    // Then construct a HashMap of Vectors of files on the same server.
    HashMap<String, Vector<String>> pfnHashMap = new HashMap<String, Vector<String>>();
    String pfn = null;
    String host = null;
    for(Iterator<String> it=pfns.iterator(); it.hasNext();){
      pfn = it.next();
      try{
        host = (new GlobusURL(pfn)).getHost();
      }
      catch(Exception e){
        logFile.addMessage("WARNING: cannot delete physical file "+
            pfn+". Please delete this file by hand.");
      }
      if(!pfnHashMap.containsKey(host)){
        Vector<String> hostPFNs = new Vector<String>();
        hostPFNs.add(pfn);
        pfnHashMap.put(host, hostPFNs);
      }
      else{
        pfnHashMap.get(host).add(pfn);
      }
    }
    // Now try to delete the batches of files.
    Set<String> hosts = pfnHashMap.keySet();
    Vector<String> hostPFNs = null;
    GlobusURL [] urls = null;
    GridPilot.getClassMgr().getStatusBar().setLabel("Deleting physical files...");
    JProgressBar pb = new JProgressBar();
    pb.setMaximum((hosts.size()));
    GridPilot.getClassMgr().getStatusBar().setProgressBar(pb);
    pb.setToolTipText("click here to cancel");
    pb.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent me){
        setFindPFNs(false);
        stop = true;
      }
    });
    int i = 0;
    for(Iterator<String> it=hosts.iterator(); it.hasNext();){
      if(getStop()){
        break;
      }
      ++i;
      GridPilot.getClassMgr().getStatusBar().setLabel("Record "+i+" : "+hosts.size());
      pb.setValue(i);
      try{
        host = it.next();
        hostPFNs = pfnHashMap.get(host);
        urls = new GlobusURL [hostPFNs.size()];
        int j = 0;
        for(Iterator<String> itt=hostPFNs.iterator(); itt.hasNext(); ++j){
          urls[j] = new GlobusURL(itt.next());
        }
        GridPilot.getClassMgr().getStatusBar().setLabel("Deleting physical file(s)");
        GridPilot.getClassMgr().getTransferControl().deleteFiles(urls);
        deleted = deleted+urls.length;
      }
      catch(Exception e){
        logFile.addMessage("WARNING: failed to delete physical files "+
            MyUtil.arrayToString(urls)+". Please delete these files by hand.");
      }
    }
    GridPilot.getClassMgr().getStatusBar().removeProgressBar(pb);
    return deleted;
  }

  /**
   * Returns the files registered in DQ for a given dataset id (vuid).
   */
  public DBResult getFiles(String datasetID){
    boolean oldFindPFNs = lookupPFNs();
    setFindPFNs(false);
    DBResult res = select("SELECT * FROM file WHERE vuid = "+datasetID, "guid", false);
    setFindPFNs(oldFindPFNs);
    return res;
  }

  public void registerFileLocation(String vuid, String dsn, String guid,
      String lfn, String url, String size, String checksum, boolean datasetComplete) throws Exception {

    boolean datasetExists = false;
    // Check if dataset already exists and has the same id
    try{
      String existingID = null;
      try{
        existingID = getDatasetID(dsn);
      }
      catch(Exception ee){
      }
      if(existingID!=null && !existingID.equals("")){
        if(!existingID.equalsIgnoreCase(vuid)){
          error = "Dataset "+dsn+" already registered in DQ2 with VUID "+
          existingID+"!="+vuid+". Using "+existingID+".";
          logFile.addInfo(error);
          vuid = existingID;
        }
        datasetExists = true;
      }
    }
    catch(Exception e){
      datasetExists =false;
    }
    
    // If the dataset does not exist, create it
    if(!datasetExists){
      try{
        GridPilot.getClassMgr().getStatusBar().setLabel("Creating new dataset "+dsn);
        vuid = dq2WriteAccess.createDataset(dsn, null, null);
        datasetExists = true;
      }
      catch(Exception e){
        error = "WARNING: could not create dataset "+dsn+" in DQ2 "+
        ". Registration of "+lfn+" in DQ2 will NOT be done";
        logFile.addMessage(error, e);
        datasetExists =false;
      }
    }
    
    if(!datasetExists){
      throw new IOException("Cannot register file in non-existing dataset (and could not create) "+dsn);
    }
    
    String dqSize = "-1";
    if(size!=null && !size.equals("") && !size.endsWith("L")){
      // The DQ2 conventions seems to be that the file size is like e.g. 41943040L 
      dqSize = size+"L";
    }
    if(checksum!=null && !checksum.equals("") && !checksum.matches("\\w+:.*")){
      // If no type is given, we assume it's an md5 sum.
      checksum = "md5:"+checksum;
    }
    
    // Strip off the path - DQ2 LFNs don't have a path - the path is only
    // used by LFC
    String shortLfn = lfn.replaceFirst("^.*/([^/]+)$", "$1");
    String [] guids = new String[] {guid};
    String [] lfns = new String[] {shortLfn};
    String [] sizes = new String[] {dqSize};
    String [] checksums = new String[] {checksum};
    // If the LFN is not already registered in DQ2 with this dataset, register it
    String existingGuid = getFileID(dsn, shortLfn);
    if(existingGuid!=null){
      if(guid!=null && !guid.equals(existingGuid)){
        logFile.addInfo("File "+lfn+" already registered with GUID "+existingGuid+". Reusing this instead of "+guid);
        guid = existingGuid;
      }
    }
    else{
      try{
        Debug.debug("Registering new lfn " +shortLfn+
          " with DQ2, "+size+", "+checksum, 2);
        dq2WriteAccess.addLFNsToDataset(vuid, lfns, guids, sizes, checksums);
      }
      catch(Exception e){
        error = "WARNING: could not update dataset "+dsn+" in DQ2 "+
           ". Registration of "+lfn+" in DQ2 NOT done";
        GridPilot.getClassMgr().getStatusBar().setLabel("Registration of " +lfn+
           " with DQ2 FAILED!");
        //logFile.addMessage(error, e);
        throw e;
      }
    }
    
    // Register in home catalog.
    boolean catalogRegOk = true;
    try{
      try{
        String path;
        int lastSlash = lfn.lastIndexOf("/");
        if(lastSlash<0){
          path = lfcUserBasePath;
        }
        else{
          path = lfn.substring(0, lastSlash);
          if(!path.equals("")){
            path = (path.startsWith("/")?"":"/")+path+((path.endsWith("/")?"":"/"));
          }
        }
        String catalog = toa.getFileCatalogServer(homeSite, false);
        String cPath = catalog.replaceFirst("^.+:/([^:]+)$", "$1");
        if(cPath.equals(catalog)){
          cPath = "";
        }
        String basePath = "/"+cPath+(cPath.endsWith("/")?"":"/");
        String finalPath = (basePath+path).replaceAll("([^:])//", "$1/");
        registerLFNs(catalog, finalPath, new String [] {guid},
            new String [] {lfn}, new String [] {url},
            new String [] {size}, new String [] {checksum});
      }
      catch(Exception e){
        logFile.addMessage("WARNING: failed to register LFN "+lfn, e);
      }
      clearCacheEntries("file");
    }
    catch(Exception e){
      //error = "ERROR: cannot register "+url+" in file catalog. "+e.getMessage();
      //logFile.addMessage(error);
      catalogRegOk = false;
      throw e;
    }
    
    if(catalogRegOk){
      // Register home site in DQ2
      try{
        // Check if lfn is registered with homeSite in DQ2
        DQ2Locations [] locations = getLocations("'"+vuid+"'");
        boolean siteRegistered = false;
        String [] incomplete = locations[0].getIncomplete();
        String [] complete = locations[0].getComplete();
        for(int i=0; i<incomplete.length; ++i){
          if(incomplete[i].trim().equalsIgnoreCase(homeSite.trim())){
            siteRegistered = true;
          }
        }
        for(int i=0; i<complete.length; ++i){
          if(complete[i].trim().equalsIgnoreCase(homeSite.trim())){
            siteRegistered = true;
          }
        }
        if(!siteRegistered){
          dq2WriteAccess.registerLocation(vuid, dsn, datasetComplete, homeSite);
        }
        clearCacheEntries("dataset");
      }
      catch(Exception e){
        error = "WARNING: could not update dataset "+dsn+" in DQ2 "+
           ". Registration of "+homeSite+" in DQ2 NOT done";
        logFile.addMessage(error, e);
      }
    }
  }

  public boolean createDataset(String table, String[] fields,
      Object[] values){
    
    if(getStop()){
      return false;
    }
    
    clearCacheEntries("dataset");
    
    Debug.debug("Creating dataset "+MyUtil.arrayToString(fields, ":")+
        " --> "+MyUtil.arrayToString(values, ":"), 2);
    
    String dsn = null;
    String vuid = null;
    Vector<String> fieldStrings = new Vector<String>();
    Vector<String> valueStrings = new Vector<String>();
    
    if(table==null || !table.equalsIgnoreCase("dataset")){
      error = "ERROR: could not use table "+table;
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
      logFile.addMessage(error);
      return false;
    }
    try{
      for(int i=0; i<values.length; ++i){
        if(fields[i].equalsIgnoreCase("dsn")){
          dsn = (String) values[i];
        }
        else if(fields[i].equalsIgnoreCase("vuid") &&
            values[i]!=null && !values[i].toString().trim().equals("")){
          vuid = (String) values[i];
        }
        else if(values[i]!=null && !values[i].toString().trim().equals("") &&
            !values[i].toString().equals("''")){
          fieldStrings.add(fields[i]);
          valueStrings.add((String) values[i]);
        }
      }
      if(dsn==null || dsn.equals("")){
        throw new Exception ("dsn empty: "+dsn);
      }
      
      if(getStop()){
        return false;
      }
      vuid = dq2WriteAccess.createDataset(dsn, null, vuid);
    }
    catch(Exception e){
      appendError(e.getMessage());
      return false;
    }
    if(valueStrings.size()>0){
      String [] fieldArray = new String [fieldStrings.size()];
      String [] valueArray = new String [valueStrings.size()];
      for(int i=0; i<fieldStrings.size(); ++i){
        fieldArray[i] = fieldStrings.get(i);
        valueArray[i] = valueStrings.get(i);
      }
      try{
        if(!updateDataset(vuid, dsn, fieldArray, valueArray)){
          throw new IOException("Update failed.");
        }
      }
      catch(Exception e){
        error = "WARNING: dataset "+dsn+" created but some parameters not correctly set; " +
                "DQ2 probably assigned new VUID.";
        logFile.addMessage(error, e);
      }
      return true;
    }
    else{
      return true;
    }
  }

  /**
   * Update fields: "dsn", "vuid", "incomplete", "complete", or a subset of these.
   * Notice: DQ2 site or file registrations are not touched. Neither are
   *         file catalog entries or physical files.
   */
  public boolean updateDataset(String vuid, String dsn, String[] fields, String[] values){
    
    if(getStop()){
      return false;
    }
    
    clearCacheEntries("dataset");
        
    boolean exists = false;
    try{
      String checkVuid = getDatasetID(dsn);
      if(checkVuid!=null && checkVuid.equals(vuid)){
        exists = true;
      }
    }
    catch(Exception e){
      exists = false;
    }
    
    // If the dataset does not exist, abort
    if(!exists){
      error = "ERROR: dataset "+dsn+" does not exist or has wrong vuid. Cannot update.";
      logFile.addMessage(error);
      return false;
    }
    
    for(int i=0; i<values.length; ++i){
      if(fields[i].equalsIgnoreCase("vuid")){
        if(values[i]!=null && !values[i].equals("") &&
            !values[i].equalsIgnoreCase(vuid)){
          error = "ERROR: cannot change vuid. "+MyUtil.arrayToString(values);
          logFile.addMessage(error);
          return false;
        }
      }
    }
    DQ2Locations [] dqLocations = null;
    for(int i=0; i<values.length; ++i){
      
      if(getStop()){
        return false;
      }
      
      // vuid
      if(fields[i].equalsIgnoreCase("vuid")){
        error = "WARNING: cannot change vuid";
        //GridPilot.getClassMgr().getStatusBar().setLabel(error);
        Debug.debug(error+" "+vuid, 1);
        //return false;
      }
      // dsn
      else if(fields[i].equalsIgnoreCase("dsn")){
        error = "WARNING: cannot change dsn";
        //GridPilot.getClassMgr().getStatusBar().setLabel(error);
        Debug.debug(error+" "+dsn, 1);
        //return false;
      }
      // complete, incomplete
      else if(fields[i].equalsIgnoreCase("incomplete") ||
          fields[i].equalsIgnoreCase("complete")){
        if(dqLocations==null){
          try{
            dqLocations = getLocations("'"+vuid+"'");
          }
          catch(IOException e){
            error = "WARNING: could not find locations for "+vuid;
            GridPilot.getClassMgr().getStatusBar().setLabel(error);
            e.printStackTrace();
            logFile.addMessage(error, e);
          }
        }
        if(dqLocations!=null && dqLocations.length>0){
          try{
            if(fields[i].equalsIgnoreCase("complete")){
              String [] newLocations = MyUtil.split(values[i]);
              Set<String> newLocationsSet = new HashSet<String>();
              Collections.addAll(newLocationsSet, newLocations);
              // Delete locations
              for(int j=0; j<dqLocations[0].getComplete().length; ++j){
                if(newLocationsSet.contains(dqLocations[0].getComplete()[j])){
                  continue;
                }
                Debug.debug("Deleting location "+dqLocations[0].getComplete()[j]+
                    " for vuid "+vuid, 2);
                dq2WriteAccess.deleteFromSite(vuid, dqLocations[0].getComplete()[j]);
              }
              // Set the new ones
              Set<String> oldLocationsSet = new HashSet<String>();
              Collections.addAll(oldLocationsSet, dqLocations[0].getComplete());
              for(int j=0; j<newLocations.length; ++j){
                if(oldLocationsSet.contains(newLocations[j])){
                  continue;
                }
                Debug.debug("Registering location "+newLocations[j]+
                    " for vuid "+vuid, 2);
                dq2WriteAccess.registerLocation(vuid, dsn, true, newLocations[j]);
              }
            }
            //BNLTAPE CERNPROD CYF LYONTAPE CSCS
            else if(fields[i].equalsIgnoreCase("incomplete")){
              String [] newLocations = MyUtil.split(values[i]);
              Set<String> newLocationsSet = new HashSet<String>();
              Collections.addAll(newLocationsSet, newLocations);
              for(int j=0; j<dqLocations[0].getIncomplete().length; ++j){
                if(newLocationsSet.contains(dqLocations[0].getIncomplete()[j])){
                  continue;
                }
                Debug.debug("Deleting location "+dqLocations[0].getIncomplete()[j]+
                    " for vuid "+vuid, 2);
                dq2WriteAccess.deleteFromSite(vuid, dqLocations[0].getIncomplete()[j]);
              }
              Set<String> oldLocationsSet = new HashSet<String>();
              Collections.addAll(oldLocationsSet, dqLocations[0].getIncomplete());
              for(int j=0; j<newLocations.length; ++j){
                if(oldLocationsSet.contains(newLocations[j])){
                  continue;
                }
                Debug.debug("Registering location "+newLocations[j]+
                    " for vuid "+vuid, 2);
                dq2WriteAccess.registerLocation(vuid, dsn, false, newLocations[j]);
              }
            }
          }
          catch(Exception e){
            error = "WARNING: could not update locations for "+vuid;
            GridPilot.getClassMgr().getStatusBar().setLabel(error);
            e.printStackTrace();
            logFile.addMessage(error, e);
          }
        }
      }
    }

    return true;
  }

  public boolean deleteDataset(String datasetID){
    return deleteDataset(datasetID, false);
  }
  
  public boolean deleteDataset(String datasetID, boolean cleanup){
    
    if(getStop()){
      return false;
    }
    
    clearCacheEntries("dataset");
        
    String dsn = null;
    boolean exists = false;
    try{
      dsn = getDatasetName(datasetID);
      Debug.debug("Deleting dsn "+dsn, 2);
      if(dsn!=null && !dsn.equals("")){
        exists = true;
      }
    }
    catch(Exception e){
      exists =false;
    }
    
    // If the dataset does not exist, abort
    if(!exists){
      error = "ERROR: dataset "+dsn+" does not exist, cannot delete.";
      logFile.addMessage(error);
      return false;
    }
    
    if(getStop()){
      return false;
    }

    // First delete files if cleanup is true
    if(cleanup){
      try{
        DBResult files = getFiles(datasetID);
        if(files.values.length>0){
          String [] guids = new String[files.values.length];
          for(int i=0; i<files.values.length; ++i){
            guids[i] = files.getValue(i, "guid").toString();
          }
          deleteFiles(datasetID, guids, cleanup);
        }
      }
      catch(Exception e){
        error = "ERROR: Could not delete files of dataset "+dsn;
        logFile.addMessage(error, e);
        GridPilot.getClassMgr().getStatusBar().setLabel(error);
        return false;
      }
    }
    
    if(getStop()){
      return false;
    }

    // Now, delete the dataset
    try{
      dq2WriteAccess.deleteDataset(dsn, datasetID);
    }
    catch(Exception e){
      error = "ERROR: Could not delete dataset "+dsn;
      logFile.addMessage(error, e);
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
      return false;
    }

    return true;
  }

  public String getRunNumber(String datasetID){
    // TODO
    return null;
  }
  
  public void clearCacheEntries(String table){
    if(!useCaching){
      return;
    }
    String thisSql = null;
    String [] theseTableNames = null;
    HashSet<String> deleteKeys = new HashSet<String>();
    for(Iterator<String> it=queryResults.keySet().iterator(); it.hasNext();){
      thisSql = it.next();
      theseTableNames = MyUtil.getTableNames(thisSql);
      if(theseTableNames==null){
        Debug.debug("WARNING: could not get table name for "+thisSql, 1);
        continue;
      }
      for(int i=0; i<theseTableNames.length; ++i){
        if(theseTableNames[i]==null || theseTableNames[i].equals("")){
          Debug.debug("WARNING: could not get table name for "+thisSql, 1);
          continue;
        }
        Debug.debug("Checking cache: "+theseTableNames[i]+"<->"+table, 2);
        if(theseTableNames[i].equalsIgnoreCase(table)){
          deleteKeys.add(thisSql);
        }
      }
    }
    Debug.debug("Clearing cache entries", 2);
    for(Iterator<String> it=deleteKeys.iterator(); it.hasNext();){
      thisSql = it.next();
      Debug.debug("--> "+thisSql, 3);
      queryResults.remove(thisSql);
    }
  }

  // -------------------------------------------------------------------
  // What's below here is not relevant for this plugin
  // -------------------------------------------------------------------


  public String getDatasetExecutableName(String datasetID){
    return null;
  }

  public String getDatasetExecutableVersion(String datasetID){
    return null;
  }
  public String[] getRuntimeEnvironments(String jobDefID){
    return null;
  }

  public DBResult getRuntimeEnvironments(){
    return null;
  }

  public String [] getRuntimeEnvironmentIDs(String name, String cs){
    return null;
  }

  public DBRecord getRuntimeEnvironment(String runtimeEnvironmentID){
    return null;
  }

  public String getRuntimeInitText(String pack, String cluster){
    return null;
  }

  public boolean createRuntimeEnvironment(Object[] values){
    return false;
  }

  public boolean updateRuntimeEnvironment(String runtimeEnvironmentID,
      String[] fields, String[] values){
    return false;
  }

  public boolean deleteRuntimeEnvironment(String runtimeEnvironmentID){
    return false;
  }

  public DBResult getExecutables(){
    return null;
  }

  public DBRecord getExecutable(String executableID){
    return null;
  }

  public String getExecutableID(String exeName, String exeVersion){
    return "-1";
  }

  public boolean createExecutable(Object[] values){
    return false;
  }

  public boolean updateExecutable(String executableID, String[] fields,
      String[] values){
    return false;
  }

  public boolean deleteExecutable(String executableID){
    return false;
  }

  public String[] getVersions(String executableName){
    return null;
  }

  public String getExecutableRuntimeEnvironment(String executableID){
    return null;
  }

  public String[] getExecutableJobParameters(String executableID){
    return null;
  }

  public String[] getExecutableOutputs(String executableID){
    return null;
  }

  public String[] getExecutableInputs(String executableID){
    return null;
  }

  public DBResult getJobDefinitions(String datasetID, String[] fieldNames,
      String [] statusList, String [] csStatusList){
    return null;
  }

  public DBRecord getJobDefinition(String jobDefID){
    return null;
  }

  public boolean createJobDefinition(String[] values){
    return false;
  }

  public boolean createJobDefinition(String datasetName, String[] cstAttrNames,
      String[] resCstAttr, String[] trpars, String[][] ofmap, String odest,
      String edest){
    return false;
  }

  public boolean deleteJobDefinition(String jobDefID){
    return false;
  }

  public boolean updateJobDefinition(String jobDefID, String[] fields,
      String[] values){
    return false;
  }

  public boolean updateJobDefinition(String jobDefID, String[] values){
    return false;
  }

  public String getJobDefStatus(String jobDefID){
    return null;
  }

  public String getJobDefUserInfo(String jobDefID){
    return null;
  }

  public String getJobDefName(String jobDefID){
    return null;
  }

  public String getJobDefDatasetID(String jobDefID){
    return "-1";
  }

  public String getJobDefExecutableID(String jobDefID){
    return "-1";
  }

  public String getExecutableFile(String jobDefID){
    return null;
  }

  public String getRunInfo(String jobDefID, String key){
    return null;
  }

  public boolean cleanRunInfo(String jobDefID){
    return false;
  }

  public boolean reserveJobDefinition(String jobDefID, String UserName, String cs){
    return false;
  }

  public String[] getOutputFiles(String jobDefID){
    return null;
  }

  public String[] getJobDefInputFiles(String jobDefID){
    return null;
  }

  public String getJobDefOutRemoteName(String jobDefID, String par){
    return null;
  }

  public String getJobDefOutLocalName(String jobDefID, String par){
    return null;
  }

  public String getStdOutFinalDest(String jobDefID){
    return null;
  }

  public String getStdErrFinalDest(String jobDefID){
    return null;
  }

  public String[] getExecutableArguments(String jobDefID){
    return null;
  }

  public String[] getJobDefExecutableParameters(String jobDefID){
    return null;
  }

  public boolean setJobDefsField(String [] identifiers, String field, String value){
    return false;
  }

  public boolean deleteJobDefsFromDataset(String datasetID) throws InterruptedException {
    return false;
  }

  public String getError(){
    return error;
  }

  public void appendError(String _error) {
    error += " "+_error;
  }

  public void clearError() {
    error = "";
  }

  public void executeUpdate(String sql) throws Exception {
    throw new Exception("The database "+dbName+" does not support general purpose SQL updates.");
  }
  
  private boolean waitForWorking(){
    for(int i=0; i<1; ++i){
      if(!getWorking()){
        // retry 10 times with 10 seconds in between
        try{
          Thread.sleep(10000);
        }
        catch(Exception e){
        }
        if(i==9){
          return false;
        }
        else{
          continue;
        }
      }
      else{
        break;
      }
    }
    return true;
  }
  
  // try grabbing the semaphore
  private boolean getWorking(){
    if(!working){
      working = true;
      return true;
    }
    return false;
  }
  // release the semaphore
  private void stopWorking(){
    working = false;
  }

  public String[] getChildrenDatasetNames(String datasetName)
      throws InterruptedException {
    String[] ret = new String[0];
    try{
      if(!datasetName.endsWith("/")){
        return ret;
      }
      String res = dq2ReadAccess.getDatasetsInContainer(datasetName);
      Debug.debug("getDatasetsInContainer --> "+ret, 2);
      ret = MyUtil.split(res.replaceAll("'", "").replaceFirst("\\[", "").replaceFirst("\\]", ""), ", ");
      Debug.debug("container datasets --> "+MyUtil.arrayToString(ret, ":"), 2);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return ret;
  }

  public String[] getParentDatasetNames(String datasetID)
      throws InterruptedException {
    // Not implemented by this plugin (no DQ2 call available?).
    return null;
  }
}
