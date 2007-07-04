package gridpilot.dbplugins.atlas;

import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.rmi.RemoteException;
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

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;

import gridpilot.ConfigFile;
import gridpilot.DBCache;
import gridpilot.DBRecord;
import gridpilot.DBResult;
import gridpilot.Database;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.LogFile;
import gridpilot.MyThread;
import gridpilot.TransferControl;
import gridpilot.Util;

public class ATLASDatabase extends DBCache implements Database{
  
  private String dq2Server;
  private String dq2Port;
  private String dq2SecurePort;
  private String dq2Path;
  private String dq2Url;
  private String dbName;
  private String [] preferredSites;
  private String [] skipSites;
  private LogFile logFile;
  // This same object is used for each PFN lookup.
  // Although the lookups are running in threads, they are not
  // running in parallel. Running them in parallel would
  // require a reengineering of the select method, which does
  // everything serially.
  private Vector pfnVector = null;
  // The same goes for fileBytes and fileChecksum
  private String fileBytes = null;
  private String fileChecksum = null;
  private int fileCatalogTimeout = 1000;
  // If forceDelete is set to true, files will be attempted deleted on
  // all physical locations and on the home catalog server MySQL alias
  // and the home server will be de-registered in DQ, even if other
  // catalog sites are registered in DQ than the home catalog or if there
  // is no home catalog set.
  private HashMap dqLocationsCache = new HashMap();
  private HashMap queryResults = new HashMap();
  private boolean useCaching = false;
  private TiersOfAtlas toa = null;
  private HashSet httpSwitches = new HashSet();

  private static boolean forceDelete = false;
  private static boolean DELETE_FROM_SITES_ON_ALL_CATALOGS = false;

  public String connectTimeout = "10000";
  public String socketTimeout = "30000";
  public String lrcPoolSize = "5";
  public int pathConvention = 1;
  public boolean stop = false;
  public boolean findPFNs = true;
  public int pathConventions = 3;
  public String homeSite;
  public String homeServerMysqlAlias;
  public String error;

  public ATLASDatabase(String _dbName){
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();
    dbName = _dbName;
    
    String useCachingStr = GridPilot.getClassMgr().getConfigFile().getValue(dbName, "cache search results");
    if(useCachingStr==null || useCachingStr.equalsIgnoreCase("")){
      useCaching = false;
    }
    else{
      useCaching = ((useCachingStr.equalsIgnoreCase("yes")||
          useCachingStr.equalsIgnoreCase("true"))?true:false);
    }

    dq2Server = configFile.getValue(dbName, "DQ2 server");
    dq2Port = configFile.getValue(dbName, "DQ2 port");
    dq2SecurePort = configFile.getValue(dbName, "DQ2 secure port");
    dq2Path = configFile.getValue(dbName, "DQ2 path");
    
    if(dq2SecurePort==null){
      error = "WARNING: DQ2 secure port not ocnfigured. Write access disabled.";    
      logFile.addMessage(error);
    }

    if(dq2Server==null || dq2Path==null){
      error = "ERROR: DQ2 not configured. Aborting.";    
      logFile.addMessage(error);
      return;
    }

    //dq2Url = http://atlddmpro.cern.ch:8000/dq2/
    dq2Url = "http://"+dq2Server+(dq2Port==null?"":":"+dq2Port)+
       (dq2Path.startsWith("/")?dq2Path:"/"+dq2Path)+(dq2Path.endsWith("/")?"":"/");
    error = "";
    pfnVector = new Vector();

    // Set preferred download sites
    preferredSites = configFile.getValues(dbName, "preferred sites");
    // Set download sites to be ignored
    skipSites = configFile.getValues(dbName, "ignored sites");
    // Set home server and possible mysql alias
    homeSite = configFile.getValue(dbName, "home site");
    if(homeSite!=null){
      String [] servers = Util.split(homeSite);
      if(servers.length==2){
        homeSite = servers[0];
        homeServerMysqlAlias = servers[1];
      }
      else if(servers.length==0 || servers.length>2){
        homeSite = null;
      }
    }
    // Get and cache the TOA file
    String toaLocation = configFile.getValue(dbName, "tiers of atlas");
    toa = new TiersOfAtlas(toaLocation);
    
    // Set the timeout for querying file catalogs
    String timeout = configFile.getValue(dbName, "file catalog timeout");
    if(timeout!=null){
      try{
        fileCatalogTimeout = Integer.parseInt(timeout);
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not parse file catalog timeout", e);
      }
    }
  }

  public void requestStop(){
    setFindPFNs(false);
    stop = true;
  }
  
  public void requestStopLookup(){
    setFindPFNs(false);
  }
  
  public void clearRequestStopLookup(){
    setFindPFNs(true);
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
    pathConvention = 1;
  }
  
  private void setFindPFNs(boolean doit){
    findPFNs = doit;
  }
  
  public DBResult select(String selectRequest, String identifier, boolean findAll){
    
    if(useCaching && queryResults.containsKey(selectRequest)){
      Debug.debug("Returning cached result for "+selectRequest, 2);
      return (DBResult) queryResults.get(selectRequest);
    }
    
    if(getStop()){
      return null;
    }
    
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
      patt = Pattern.compile(", "+identifier+", ", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll(", ");
      patt = Pattern.compile(" "+identifier+" FROM", Pattern.CASE_INSENSITIVE);
      if(!patt.matcher(req).find()){
        patt = Pattern.compile(" FROM (\\w+)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceAll(", "+identifier+" FROM "+"$1");
      }
    }
    
    Debug.debug("request: "+req, 3);
    String table = req.replaceFirst("SELECT (.+) FROM (\\S+)\\s*.*", "$2");
    String fieldsString = req.replaceFirst("SELECT (.*) FROM (\\S+)\\s*.*", "$1");
    if(fieldsString.indexOf("*")>-1){
      fields = getFieldNames(table);
    }
    else{
      fields = Util.split(fieldsString, ", ");
    }
    Debug.debug("fields: "+Util.arrayToString(fields, ":"), 3);
    
    // TODO: disallow expensive wildcard searches
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
    HashMap excludeMap = new HashMap();
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
        Util.arrayToString(excludeMap.keySet().toArray())+"-->"+
        Util.arrayToString(excludeMap.values().toArray()), 2);

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
      
      get = get.replaceAll("(?i)\\bdsn CONTAINS (\\S+)", "dsn=*$1*");
      get = get.replaceAll("(?i)\\bvuid CONTAINS (\\S+)", "vuid=*$1*");
      get = get.replaceAll("(?i)\\bcomplete CONTAINS (\\S+)", "complete=*$1*");
      get = get.replaceAll("(?i)\\bincomplete CONTAINS (\\S+)", "incomplete=*$1*");
      
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
      if(complete.equals("") && incomplete.equals("")){
        if(vuid.equals("")){
          get = dq2Url+"ws_repository/rpc?operation=queryDatasetByName&version=0&API=0_3_0&"+get;
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
            //    "operation=queryDatasetLocations&API=0_3_0&dsns=[]&vuids="+"["+vuidsString+"]", 3);
            //ret = readGetUrl(new URL(url));
            //ret = URLDecoder.decode(ret, "utf-8");
            DQ2Access dq2Access = new DQ2Access(dq2Server, Integer.parseInt(dq2SecurePort), dq2Path);
            str = dq2Access.getDatasets("['"+vuid+"']").trim();
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
      }
      // DQ2 cannot handle other searches.
      if(str==null){
        error = "WARNING: cannot perform search "+get;
        Debug.debug(error, 1);
        return new DBResult(fields, new String[0][fields.length]);
      }
      
      // Now parse the DQ string and construct DBRecords
      String [] records = Util.split(str, "\\}, ");
      if(records==null || records.length==0){
        Debug.debug("WARNING: no records found with "+str, 2);
        return new DBResult(fields, new String[0][fields.length]);
      }
      
      Vector valuesVector = new Vector();
      String [] record = null;
      String vuidsString = null;
      String [] vuids = null;
      //String duid = null;
      for(int i=0; i<records.length; ++i){
        
        Vector recordVector = new Vector();
        boolean exCheck = true;
        records[i] = records[i].replaceFirst("^\\{", "");
        records[i] = records[i].replaceFirst("\\}\\}$", "");
        record = Util.split(records[i], ": \\{'vuids': ");
        
        if(record!=null && record.length>1){
          // If the string is the result of a dsn=... request, the
          // split went ok and this should work:
          String name = record[0].replaceAll("'", "");
          //duid = record[0].replaceFirst("'duid': '(.*)'", "$1");
          vuidsString = record[1].replaceFirst("\\[(.*)\\], .*", "$1");
          vuids = Util.split(vuidsString, ", ");
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
          vuid = vuid.replaceAll("\\*", ".*");
          complete = complete.replaceAll("\\*", ".*");
          incomplete = incomplete.replaceAll("\\*", ".*");
          Debug.debug("Matching: "+vuid+":"+complete+":"+incomplete, 2);
          for(int j=0; j<vuids.length; ++j){
            Debug.debug("vuid: "+vuids[j], 3);
            recordVector = new Vector();
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
                  Util.arrayToString(dqLocations[j].getIncomplete());
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
                  Util.arrayToString(dqLocations[j].getComplete());
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
              Debug.debug("Adding record "+Util.arrayToString(recordVector.toArray()), 3);
              valuesVector.add(recordVector.toArray());
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
            record = Util.split(records[i], "'[^']+': ");
            String name = records[i].replaceFirst(".*'([^']+)': \\[\\d+\\].*", "$1");
            if(name.equals(records[i])){
              name = "";
            }
            // If some selection boxes have been set, use patterns for restricting.
            complete = complete.replaceAll("\\*", ".*");
            incomplete = incomplete.replaceAll("\\*", ".*");
            dsn = dsn.replaceAll("\\*", ".*");
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
                  Util.arrayToString(dqLocations[0].getIncomplete());
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
                  Util.arrayToString(dqLocations[0].getComplete());
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
              Debug.debug("Adding record "+Util.arrayToString(recordVector.toArray()), 3);
              valuesVector.add(recordVector.toArray());
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
          values[i][j] = ((Object []) valuesVector.get(i))[j].toString();
        }
        Debug.debug("Adding record "+Util.arrayToString(values[i]), 3);
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
        DQ2Access dq2Access = new DQ2Access(dq2Server, Integer.parseInt(dq2SecurePort), dq2Path);
        str = dq2Access.getDatasetFiles("['"+vuid+"']");
        // get rid of time stamp.
        str = str.replaceFirst("\\((.*),[^,]+\\)", "$1").trim();
        Debug.debug("Found files : "+str, 3);
      }
      catch(IOException e1){
        error = "Could not get file names for "+vuid;
        Debug.debug(error, 2);
        return null;
      }
      // Check if the result is of the form {...}
      if(!str.matches("^\\{.*\\}$")){
        Debug.debug("ERROR: cannot parse search result "+str, 1);
        return new DBResult(fields, new String[0][fields.length]);
      }
      // Now parse the DQ string and construct DBRecords
      str = str.replaceFirst("^\\{", "");
      str = str.replaceFirst("\\}$", "");
      String [] records = Util.split(str, "\\}, ");
      if(records==null || records.length==0){
        Debug.debug("WARNING: no records found with "+str, 2);
        return new DBResult(fields, new String[0][fields.length]);
      }    
      Vector valuesVector = new Vector();
      String [] record = null;
      if(!GridPilot.getClassMgr().getStatusBar().isCenterComponentSet()){
        pb = Util.setProgressBar(records.length, dbName);
      }
      for(int i=0; i<records.length; ++i){
        if(getStop()){
          break;
        }
        GridPilot.getClassMgr().getStatusBar().setLabel("Record "+(i+1)+" : "+records.length);
        if(pb!=null){
          pb.setValue(i+1);
        }
        Vector recordVector = new Vector();
        boolean exCheck = true;
        record = Util.split(records[i], ": \\{");
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
          bytes = "";
        }
        checksum = record[1].replaceAll(".*'checksum': '([^']*)'.*", "$1").trim();
        if(checksum.equals(record[1])){
          checksum = "";
        }
        String catalogs = "";
        if(findPFNs){
          try{
            catalogs = Util.arrayToString(findPFNs(vuid, lfn, findAll).toArray());
            bytes = (bytes.equals("")&&fileBytes!=null?fileBytes:bytes);
            checksum = (checksum.equals("")&&fileChecksum!=null?fileChecksum:checksum);
          }
          catch(Exception e){
            e.printStackTrace();
          }
          pfns = Util.arrayToString(pfnVector.toArray());
        }
        else{
          pfns = "";
        }
 
        recordVector = new Vector();
        exCheck = true;
        for(int k=0; k<fields.length; ++k){
          if(fields[k].equalsIgnoreCase("lfn")){
            //recordVector.add(lfn);
            recordVector.add((lfn.startsWith("/")?"":"/")+makeAtlasPath(lfn));
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
          Debug.debug("Adding record "+Util.arrayToString(recordVector.toArray()), 3);
          valuesVector.add(recordVector.toArray());
        }
        else{
          Debug.debug("Excluding record ", 2);
        }       

      }
      setFindPFNs(true);
      if(pb!=null){
        GridPilot.getClassMgr().getStatusBar().removeProgressBar(pb);
        GridPilot.getClassMgr().getStatusBar().clearCenterComponent();
      }
      values = new String[valuesVector.size()][fields.length];
      for(int i=0; i<valuesVector.size(); ++i){
        for(int j=0; j<fields.length; ++j){
          values[i][j] = ((Object []) valuesVector.get(i))[j].toString();
        }
        Debug.debug("Adding record "+Util.arrayToString(values[i]), 3);
      }
      DBResult res = new DBResult(fields, values);
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
    //   "operation=queryDatasetLocations&API=0_3_0&dsns=[]&vuids="+URLEncoder.encode("["+vuidsString+"]", "utf-8");
    String ret = null;
    try{
      //Debug.debug(">>> get string was : "+dq2Url+"ws_location/rpc?"+
      //    "operation=queryDatasetLocations&API=0_3_0&dsns=[]&vuids="+"["+vuidsString+"]", 3);
      //ret = readGetUrl(new URL(url));
      //ret = URLDecoder.decode(ret, "utf-8");
      DQ2Access dq2Access = new DQ2Access(dq2Server, Integer.parseInt(dq2SecurePort), dq2Path);
      ret = dq2Access.getDatasetLocations("["+vuidsString+"]");
    }
    catch(Exception e){
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
    String [] records = Util.split(str, "}, ");
    // we return an array of the same length as the vuid array.
    // If DQ2 returned a shorter result, we pad with the last entry.
    int len = Util.split(vuidsString, ", ").length;
    DQ2Locations [] dqLocations = new DQ2Locations[len];
    Debug.debug("Found "+records.length+" records. "+Util.arrayToString(records, " : "), 2);
    for(int i=0; i<len; ++i){
      
      if(getStop()){
        break;
      }

      if(i>records.length-1){
        dqLocations[i] = dqLocations[records.length-1];
        continue;
      }
      String [] recordEntries = null;
      recordEntries = Util.split(records[i], ": \\{0: ");
      Debug.debug(i+" - Found "+records.length+" entries. "+Util.arrayToString(recordEntries, " : "), 2);
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
      locations = Util.split(recordEntries[1], ", 1: ");
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
        incompleteArr = Util.split(locations[0], "', '");
      }
      String [] completeArr = null;
      if(locations[1].equals("[]")){
        completeArr = new String [] {};;
      }
      else{
        completeArr = Util.split(locations[1], "', '");
      }
      // We don't use DQ2Locations.getDatasetName() anywhere,
      // so we don't have to waste time setting the dataset name
      // and just set it to null
      dqLocations[i] = new DQ2Locations(null, incompleteArr,
          completeArr);
      Debug.debug("final incomplete: "+Util.arrayToString(dqLocations[i].getIncomplete()), 3);
      Debug.debug("final complete: "+Util.arrayToString(dqLocations[i].getComplete()), 3);
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

  // Construct path following ATLAS conventions
  public String makeAtlasPath(String lfn){
    
    String atlasLpn = null;
    String [] lfnMetaData = Util.split(lfn, "\\.");
    Debug.debug("lfnMetaData: "+ lfnMetaData.length+":"+Util.arrayToString(lfnMetaData), 2);
    
    switch(pathConvention){
    
    case 3:
      Debug.debug("Using very new path convention", 2);
      // File trig1_misal1_mc11.007406.singlepart_singlepi7.recon.log.v12003103_tid003805._00003.job.log.tgz.6 ->
      // /grid/atlas/dq2/trig1_misal1_mc11/trig1_misal1_mc11.007406.singlepart_singlepi7.recon.log.v12003103_tid003805/trig1_misal1_mc11.007406.singlepart_singlepi7.recon.log.v12003103_tid003805._00003.job.log.tgz.6
      String baseStr = lfn.replaceFirst("^(.*)\\._[^\\.]+\\..*$", "$1");
      String [] baseMetaData = Util.split(baseStr, "\\.");
      Debug.debug("baseStr: "+baseStr, 2);
      Debug.debug("--> length: "+baseMetaData.length, 2);
      if(baseMetaData.length==6){
        atlasLpn = /*datafiles*/"dq2/"+lfnMetaData[0];
        //atlasLPN += "/"+lfnMetaData[3];
        atlasLpn += "/"+baseStr;
        atlasLpn += "/"+lfn;
      }
      else{
        atlasLpn = lfn;
      }
      break;
      
     case 2:
       Debug.debug("Using old path convention", 2);
       // csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103._00001.pool.root ->
       // /grid/atlas/dq2/csc11/csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103/
       if(lfnMetaData.length==8 || lfnMetaData.length==9 || lfnMetaData.length==10){
         atlasLpn = /*datafiles*/"dq2/"+lfnMetaData[0];
         //atlasLPN += "/"+lfnMetaData[3];
         atlasLpn += "/"+lfnMetaData[0]+"."+lfnMetaData[1]+"."+lfnMetaData[2]+"."+
            lfnMetaData[3]+"."+lfnMetaData[4]+"."+lfnMetaData[5];
         atlasLpn += "/"+lfn;
       }
       else{
         atlasLpn = lfn;
       }
       break;
       
     case 1:
       Debug.debug("Using new path convention", 2);
       // New (or old?) convention:
       // csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103._00001.pool.root ->
       // /grid/atlas/dq2/csc11/csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103/AOD/
       if(lfnMetaData.length==8 || lfnMetaData.length==9 || lfnMetaData.length==10){
         atlasLpn = /*"datafiles/"+*/"dq2/"+lfnMetaData[0];
         atlasLpn += "/"+lfnMetaData[4];
         atlasLpn += "/"+lfnMetaData[0]+"."+lfnMetaData[1]+"."+lfnMetaData[2]+"."+
            lfnMetaData[3]+"."+lfnMetaData[4]+"."+lfnMetaData[5];
         atlasLpn += "/"+lfn;
       }
       else{
         atlasLpn = lfn;
       }
       break;
       
     default:
       Debug.debug("pathConvention not in range: "+pathConvention, 2);
       throw new IndexOutOfBoundsException("pathConvention not in range: "+pathConvention);

    }
    
    return atlasLpn;
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
  
  private String [] lookupPFNs(final String _catalogServer, final String lfn, final boolean findAll){
    MyThread t = new MyThread(){
      String [] res = null;
      public String [] getString2Res(){
        return res;
      }
      public void run(){
        if(getStop() || !findPFNs){
          return;
        }
        try{
          res = doLookupPFNs(_catalogServer, lfn, findAll);
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    };
    t.start();              
    if(!Util.waitForThread(t, dbName, fileCatalogTimeout, "lookup pfns", new Boolean(false))){
      error = "WARNING: timed out for "+lfn+" on "+_catalogServer;
      logFile.addMessage(error);
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
    }
    return t.getString2Res();
  }
  
  /**
   * 
   * @param _catalogServer
   * @param lfn
   * @param findAll
   * @return an array where the first two entries are bytes, checksums, then
   * follows the PFNs. Bytes and checksum may each be null.
   * @throws RemoteException
   * @throws ServiceException
   * @throws MalformedURLException
   * @throws SQLException
   */
  private String [] doLookupPFNs(String _catalogServer, String lfn, boolean findAll)
     throws Exception {
    // get rid of the :/, which GlobusURL doesn't like
    String catalogServer = _catalogServer.replaceFirst("(\\w):/(\\w)", "$1/$2");
    GlobusURL catalogUrl = new GlobusURL(catalogServer);
    if(catalogUrl.getProtocol().equals("lfc")){
      return (new LFCLookupPFN(this, catalogServer, lfn, findAll)).lookup();
    }
    else if(catalogUrl.getProtocol().equals("mysql")){
      return (new MySQLLookupPFN(this, catalogServer, lfn, findAll)).lookup();
    }
    else if(catalogUrl.getProtocol().equals("http")){
      return (new LRCLookupPFN(this, catalogServer, lfn, findAll)).lookup();
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
     throws RemoteException, ServiceException, MalformedURLException, SQLException {
    
    if(getStop()){
      return;
    }
    
    clearCacheEntries("file");
    
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
        user = Util.getGridDatabaseUser();
      }
      // Make the connection
      GridPilot.getClassMgr().sqlConnection(
          alias, driver, database, user, passwd, gridAuth,
          connectTimeout, socketTimeout, lrcPoolSize);
      Connection conn = GridPilot.getClassMgr().getDBConnection(alias);
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
          Vector resultVector = new Vector();
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
          guid = (String) resultVector.get(0);
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
   * Flags a set of LFNs to be deleted on a MySQL alias catalog.
   */
  private void setDeleteLFNs(String _catalogServer, String [] lfns)
     throws RemoteException, ServiceException, MalformedURLException, SQLException {
    
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
        user = Util.getGridDatabaseUser();
      }
      // Make the connection
      GridPilot.getClassMgr().sqlConnection(
          alias, driver, database, user, passwd, gridAuth,
          connectTimeout, socketTimeout, lrcPoolSize);
      Connection conn = GridPilot.getClassMgr().getDBConnection(alias);
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
          Vector resultVector = new Vector();
          Debug.debug(">> "+req, 3);
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
          guid = (String) resultVector.get(0);
          // Now flag this guid for deletion in t_meta
          req = "UPDATE t_meta SET sync = 'delete' WHERE guid ='"+guid+"'";
          Debug.debug(">> "+req, 3);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could flag guid "+guid+" for deletion in t_meta on "+catalogServer;
            logFile.addMessage(error);
          }
        }
        catch(Exception e){
          error = "WARNING: problem deleting lfn "+lfns[i]+" on "+catalogServer;
          logFile.addMessage(error);
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
   * The catalog server string must be of the form
   * mysql://dsdb-reader:dsdb-reader1@db1.usatlas.bnl.gov:3306/localreplicas.
   * LFC is not supported.
   */
  private void registerLFNs(String _catalogServer, String [] guids,
      String [] lfns, String [] pfns, boolean sync)
     throws RemoteException, ServiceException, MalformedURLException, SQLException {
    
    if(getStop()){
      return;
    }
    
    // get rid of the :/, which GlobusURL doesn't like
    String catalogServer = _catalogServer.replaceFirst("(\\w):/(\\w)", "$1/$2");
    GlobusURL catalogUrl = new GlobusURL(catalogServer);
    if(catalogUrl.getProtocol().equals("mysql")){
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
          user = Util.getGridDatabaseUser();
          GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
          GlobusCredential globusCred = null;
          if(credential instanceof GlobusGSSCredentialImpl){
            globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
          }
          if(gridAuth){
            try{
              Util.activateSsl(globusCred);
            }
            catch(Exception e){
              Debug.debug("ERROR: "+e.getMessage(), 1);
              throw e;
            }
          }
        }
        // Make the connection
        GridPilot.getClassMgr().sqlConnection(
            alias, driver, database, user, passwd, gridAuth,
            connectTimeout, socketTimeout, lrcPoolSize);
        conn = GridPilot.getClassMgr().getDBConnection(alias);
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
          if(sync){
            // First, just try and create the metadata entry - in case it's not there already
            try{
              req = "INSERT INTO t_meta (sync, guid) VALUES " +
                 "('', '"+guids[i]+"')";
              Debug.debug(">> "+req, 3);
              rowsAffected = conn.createStatement().executeUpdate(req);
              Debug.debug("rowsAffected "+rowsAffected, 3);
            }
            catch(Exception e){
            }
            try{
              // Now flag this guid for write in t_meta
              req = "UPDATE t_meta SET sync = 'write' WHERE guid ='"+guids[i]+"'";
              Debug.debug(">> "+req, 3);
              rowsAffected = conn.createStatement().executeUpdate(req);
              if(rowsAffected==0){
                error = "WARNING: could flag guid "+guids[i]+" for write in t_meta on "+catalogServer;
                logFile.addMessage(error);
              }
            }
            catch(Exception e){
              error = "WARNING: could flag guid "+guids[i]+" for write in t_meta on "+catalogServer;
              logFile.addMessage(error);
            }
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
    else{
      error = "ERROR: protocol not supported: "+catalogUrl.getProtocol();
      Debug.debug(error, 1);
      throw new MalformedURLException(error);
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
        if(site.equals(skipSites[j])){
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
  private Vector getOrderedLocations(String vuid){
    
    // First check cache
    if(dqLocationsCache.containsKey(vuid)){
      return (Vector) dqLocationsCache.get(vuid);
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
    Vector locations = new Vector();
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
            dqLocations.getComplete()[i].equalsIgnoreCase(preferredSites[ii]) &&
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
    HashSet preferredSet = new HashSet();
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
    Debug.debug("Found locations "+Util.arrayToString(locations.toArray()), 3);
    dqLocationsCache.put(vuid, locations);
    return locations;
  }
  
  /**
   * Fill the vector pfnVector with PFNs registered for lfn.
   * Returns a Vector of catalogServers corresponding to the
   * Vector of PFNs.
   */
  private Vector findPFNs(String vuid, String lfn, boolean findAll){
    // Query all with a timeout of 5 seconds.    
    // First try the home server if configured.
    // Next try the locations with complete datasets, then the incomplete.
    // For each LRC catalog try both the mysql and the http interface.
    String lfName = lfn;
    Vector locations = getOrderedLocations(vuid);
    Vector catalogs = new Vector();
    Object [] locationsArray = locations.toArray();
    try{
      pfnVector.clear();
      fileBytes = null;
      fileChecksum = null;
      for(int i=0; i<locationsArray.length; ++i){
        if(locationsArray[i]==null || ((String) locationsArray[i]).matches("\\s*")){
          continue;
        }
        if(getStop() || !findPFNs){
          return catalogs;
        }
        String [] pfns = null;
        try{
          Debug.debug("Querying TOA for "+i+":"+locationsArray[i], 2);
          String catalogServer = null;
          // If trying to query the home lfc server, first try the mysql alias if possible
          // TODO: fall back to LFC
          String tryAgainServer = null;
          String fallbackServer = null;
          if(homeSite!=null && homeServerMysqlAlias!=null &&
              ((String) locationsArray[i]).equalsIgnoreCase(homeSite)){
            catalogServer = homeServerMysqlAlias;
            tryAgainServer = toa.getFileCatalogServer((String) locationsArray[i], false); 
          }
          else{
            if(!httpSwitches.contains(locationsArray[i])){
              catalogServer = toa.getFileCatalogServer((String) locationsArray[i], false);
              fallbackServer = toa.getFileCatalogServer((String) locationsArray[i], true);
            }
            else{
              catalogServer = toa.getFileCatalogServer((String) locationsArray[i], true);
              fallbackServer = toa.getFileCatalogServer((String) locationsArray[i], false);
            }
          }
          if(catalogServer==null){
            logFile.addMessage("WARNING: could not find catalog server for "+
                locationsArray[i]);
            continue;
          }
          Debug.debug("Querying "+i+"-->"+catalogServer+" for "+lfName, 2);
          GridPilot.getClassMgr().getStatusBar().setLabel("Querying "+catalogServer);
          try{
            try{
              pfns = lookupPFNs(catalogServer, lfName, findAll);
            }
            catch(Exception e){
              e.printStackTrace();
            }
            if(tryAgainServer!=null && (pfns==null || pfns.length==0)){
              Debug.debug("No PFNs found, trying alias "+tryAgainServer, 2);
              pfns = lookupPFNs(tryAgainServer, lfName, findAll);
              catalogServer = tryAgainServer;
            }
            if(fallbackServer!=null && (pfns==null || pfns.length==0)){
              Debug.debug("No PFNs found, trying fallback "+fallbackServer, 2);
              pfns = lookupPFNs(fallbackServer, lfName, findAll);
              catalogServer = fallbackServer;
              if(pfns!=null && pfns.length>2){
                Debug.debug("Switching to http for "+locationsArray[i], 2);
                httpSwitches.add(locationsArray[i]);
              }
            }
            if(pfns!=null && pfns.length>2){
              catalogs.add(catalogServer);
              fileBytes = pfns[0];
              fileChecksum = pfns[1];
              for(int n=2; n<pfns.length; ++n){
                pfnVector.add(pfns[n]);
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
        // break out after first location, if "Find all" is not checked
        if(!findAll && !pfnVector.isEmpty() && pfnVector.get(0)!=null){
          break;
        }
        // if we did not find anything on this location, put it last in the
        // HashMap of locations
        if(pfns==null || pfns.length==0 || pfns[0]==null || pfns[0].equals("")){
          Vector tl = (Vector) dqLocationsCache.get(vuid);
          int j=0;
          for(j=0; j<tl.size(); ++j){
            if(tl.get(j).equals(locationsArray[i])){
              break;
            }
          }
          int len = tl.size();
          Debug.debug("Deprecating location: "+locationsArray[i]+" -->"+
              j+":"+len+":"+(-len+j+1), 2);
          Collections.rotate(((Vector) dqLocationsCache.get(vuid)).subList(j, len),
              len-j-1);
          Debug.debug("New location cache for "+vuid+
              ": "+Util.arrayToString(((Vector) dqLocationsCache.get(vuid)).toArray()), 2);
        }
      }
      // Eliminate duplicates
      Vector pfnShortVector = new Vector();
      String tmpPfn = null;
      for(Iterator it=pfnVector.iterator(); it.hasNext();){
        tmpPfn = (String) it.next();
        if(!pfnShortVector.contains(tmpPfn)){
          pfnShortVector.add(tmpPfn);
        }
      }
      pfnVector = pfnShortVector;
      Debug.debug("pfnVector:"+pfnVector.size(), 2);
      GridPilot.getClassMgr().getStatusBar().setLabel("Querying done.");
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return catalogs;
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
      ret = res.getRow(0).getValue("vuid").toString();
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
    return res.getRow(0);
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
    DBRecord file = getFile(datasetName, fileName, 0);
    return file.getValue("guid").toString();
  };
  
  public DBRecord getFile(String dsn, String fileID, int findAllPFNs){
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
    
    // Alternative, hacky solution
    
    String [] fields= getFieldNames("file");
    
    String vuid = getDatasetID(dsn);
    
    DBResult files = getFiles(vuid);
    String lfn = null;
    for(int i=0; i<files.values.length; ++i){
      Debug.debug("matching fileID "+fileID, 3);
      if(files.getValue(i, "guid").toString().equals(fileID)){
        lfn = files.getValue(i, "lfn").toString();
        lfn = lfn.replaceFirst(".*/([^/]+)", "$1");
        break;
      };
    }
    
    // Get the pfns
    Vector resultVector = new Vector();
    String pfns = "";
    String catalogs = "";
    String bytes = "";
    String checksum = "";
    if(findAllPFNs!=0){
      catalogs = Util.arrayToString(findPFNs(vuid, lfn, findAllPFNs==2).toArray());
      for(int j=2; j<pfnVector.size(); ++j){
        resultVector.add((String) pfnVector.get(j));
      }
      bytes = (fileBytes==null?"":fileBytes);
      checksum = (fileBytes==null?"":fileChecksum);
      pfns = Util.arrayToString(resultVector.toArray());
    }
        
    return new DBRecord(fields, new String [] {dsn, lfn, catalogs, pfns, fileID, bytes, checksum});

  }

  public String [][] getFileURLs(String datasetName, String fileID, boolean findAll){
    String [][] ret = new String [2][];
    try{
      DBRecord file = getFile(datasetName, fileID, findAll?2:1);
      Debug.debug("catalogs: "+file.getValue("catalogs"), 2);
      ret[0] = Util.split((String) file.getValue("catalogs"));
      ret[1] = Util.splitUrls((String) file.getValue("pfns"));
    }
    catch(Exception e){
      e.printStackTrace();
      Debug.debug("WARNING: could not get URLs. "+e.getMessage(), 1);
    }
    return ret;
  }

  /**
   * Delete file entries in DQ file catalog, delete the corresponding physical files
   * and the entries on MySQL home server.
   */
  public boolean deleteFiles(String datasetID, String [] fileIDs, boolean cleanup){

    if(getStop()){
      return false;
    }
    
    clearCacheEntries("file");
    
    // Find the LFNs to keep and those to delete.
    String [] toDeleteLfns = null;
    String [] toKeepGuids = null;
    String [] toKeepLfns = null;
    String [] toKeepSizes = null;
    String [] toKeepChecksums = null;
    // NOTICE: we are assuming that there is a one-to-one mapping between
    //         lfns and guids. This is not necessarily the case...
    // TODO: improve
    try{
      GridPilot.getClassMgr().getStatusBar().setLabel("Finding LFNs...");
      DBResult currentFiles = getFiles(datasetID);
      toDeleteLfns = new String[fileIDs.length];
      toKeepGuids = new String[currentFiles.values.length-fileIDs.length];
      toKeepLfns = new String[currentFiles.values.length-fileIDs.length];
      toKeepSizes = new String[currentFiles.values.length-fileIDs.length];
      toKeepChecksums = new String[currentFiles.values.length-fileIDs.length];
      int count = 0;
      int count1 = 0;
      boolean staysThere = false;
      for(int i=0; i<currentFiles.values.length; ++i){
        staysThere = false;
        for(int j=0; j<fileIDs.length; ++j){
          if(currentFiles.getValue(i, "guid").toString().equalsIgnoreCase(fileIDs[j])){
            staysThere = true;
            break;
          }
        }
        if(!staysThere){
          toKeepGuids[count] = currentFiles.getValue(i, "guid").toString();
          toKeepLfns[count] = currentFiles.getValue(i, "lfn").toString();
          toKeepSizes[count] = currentFiles.getValue(i, "size").toString();
          toKeepChecksums[count] = currentFiles.getValue(i, "checksum").toString();
          if(toKeepGuids[count]==null || toKeepLfns[count]==null ||
              toKeepGuids[count].equals("") || toKeepLfns[count].equals("")){
            error = "ERROR: no guid/lfn for "+toKeepGuids[count]+"/"+toKeepLfns[count]+
            ". Aborting delete; nothing deleted.";
            logFile.addMessage(error);
            GridPilot.getClassMgr().getStatusBar().setLabel(error);
            return false;
          }
          ++count;
        }
        else{
          toDeleteLfns[count1] = currentFiles.getValue(i, "lfn").toString();
          ++count1;
        }
      }
      if(count!=currentFiles.values.length-fileIDs.length){
        error = "ERROR: inconsistency, cannot delete. Aborting; nothing deleted";
        logFile.addMessage(error);
        GridPilot.getClassMgr().getStatusBar().setLabel(error);
        return false;
      }
    }
    catch(Exception e){
      error = "ERROR: could not delete files "+Util.arrayToString(fileIDs)+" from " +
         datasetID+". Aborting";
      e.printStackTrace();
      logFile.addMessage(error, e);
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
      return false;
    }

    // First, if cleanup is true, check if files are ONLY registered in home file catalogue;
    // if so, delete the physical files and clean up the home file catalogue.
    boolean complete = false;
    boolean atLeastOneDeleted = false;
    boolean deletePhysOk = true;
    DQ2Locations locations = null;
    boolean deleteFromCatalogOK = true;
    if(cleanup){
      if(!forceDelete){
        try{
          locations = getLocations("'"+datasetID+"'")[0];
          if(checkIfOnlyOneLocation(locations)){
            complete = (locations.getComplete().length>0);
          }
          else{
            return false;
          }
        }
        catch(Exception e){
          error = "WARNING: problem with locations. There may be orphaned LFNs in DQ2, " +
          "and/or wrongly registered locations in DQ2 and/or " +
          "wrongly registered file catalog entries.";
          logFile.addMessage(error, e);
          return false;
        }
      }

      // Delete physical files
      int deleted = 0;
      try{
        deleted = deletePhysicalFiles(datasetID, toDeleteLfns);
      }
      catch(Exception e){
        deletePhysOk = false;
      }
      atLeastOneDeleted = (deleted>0);
      deletePhysOk = (deleted==toDeleteLfns.length);
      
      if(getStop()){
        return false;
      }
      
      // Remove entries from MySQL catalog
      // Notice: we delete ONLY one entry per lfn, the one in the home catalog
      GridPilot.getClassMgr().getStatusBar().setLabel("Cleaning up home catalog...");
      try{
        // if we're using an alias, just flag for deletion
        if(homeServerMysqlAlias!=null){
          setDeleteLFNs(homeServerMysqlAlias, toDeleteLfns);
        }
        // otherwise, it is assumed that we're using a mysql catalog and that we can delete
        else{
          deleteLFNs(toa.getFileCatalogServer(homeSite, false), toDeleteLfns);
        }
      }
      catch(Exception e){
        deleteFromCatalogOK = false;
        logFile.addMessage("WARNING: failed to delete LFNs "+Util.arrayToString(toDeleteLfns)+
            " on "+homeServerMysqlAlias+". Please delete them by hand.");
      }
    }
    
    boolean eraseFromDQ2Ok = true;
    if(forceDelete || !cleanup){
      eraseFromDQ2Ok = eraseFromDQ2(datasetID, complete, atLeastOneDeleted, deletePhysOk,
          toKeepGuids, toKeepLfns, toKeepSizes, toKeepChecksums, locations);
    }
    return eraseFromDQ2Ok && deletePhysOk && deleteFromCatalogOK;
  }
  
  /**
   * Checks if the home catalog is the only registered location.
   * If not, throws an exception, if yes, returns a list containing
   * only the location to delete.
   * @throws Exception 
   */
  private boolean checkIfOnlyOneLocation(DQ2Locations locations) throws Exception{
    //GridPilot.getClassMgr().getStatusBar().setLabel("Finding locations...");
    // Check that there is only one location registered,
    // that it is the homeServer and that it has a MySQL Alias
    if(locations.getIncomplete().length+locations.getComplete().length>1){
      error = "More than one location registered: "+
      Util.arrayToString(locations.getIncomplete())+" "+
      Util.arrayToString(locations.getComplete());
      logFile.addMessage(error);
      return false;
    }
    String location = null;
    if(locations.getIncomplete().length==1){
      location = locations.getIncomplete()[0];
    }
    else if(locations.getComplete().length==1){
      location = locations.getComplete()[0];
    }
    if(!location.equalsIgnoreCase(homeSite) || homeServerMysqlAlias==null ||
        homeServerMysqlAlias.equals("")){
      error = "Can only delete files on home catalog server MySQL alias. Ignoring "+location;
      logFile.addMessage(error);
      return false;
    }
    return true;
  }
  
  // Deregister the LFNs from this vuid on DQ2.
  // NOTICE that this changes the vuid of the dataset...
  private boolean eraseFromDQ2(String datasetID, boolean complete, boolean atLeastOneDeleted,
      boolean deletePhysOk, String [] toKeepGuids, String [] toKeepLfns, String [] toKeepSizes,
      String [] toKeepChecksums, DQ2Locations locations){
    String dsn = null;
    dsn = getDatasetName(datasetID);
    DQ2Access dq2Access = null;
    String [] completeLocations = null;
    String [] incompleteLocations = null;
    try{
      dq2Access = new DQ2Access(dq2Server, Integer.parseInt(dq2SecurePort), dq2Path);
      try{
        completeLocations = locations.getComplete();
        // Delete all locations from old vuid (a new vuid will be created)
        for(int i=0; i<completeLocations.length; ++i){
          if(getStop()){
            return false;
          }
          try{
            dq2Access.deleteFromSite(datasetID, locations.getComplete()[i]);
          }
          catch(Exception e){
          }
        }
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
      try{
        incompleteLocations = locations.getIncomplete();
        for(int i=0; i<incompleteLocations.length; ++i){
          if(getStop()){
            return false;
          }
          try{
            dq2Access.deleteFromSite(datasetID, locations.getComplete()[i]);
          }
          catch(Exception e){
          }
        }
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
      // Clear the lfns by creating new dataset with the same dsn
      Debug.debug("Creating new version of dataset "+dsn, 2);
      dq2Access.createNewDatasetVersion(dsn);
      // Add the lfns we don't delete
      Debug.debug("Re-adding original LFNs "+Util.arrayToString(toKeepLfns), 2);
      if(toKeepLfns.length>0 && toKeepGuids.length>0){
        dq2Access.addLFNsToDataset(datasetID, toKeepGuids, toKeepLfns, toKeepSizes,
            toKeepChecksums);
      }
      // Re-register all locations
      Debug.debug("Re-registering original locations "+Util.arrayToString(toKeepLfns), 2);
      if(completeLocations!=null && completeLocations.length>0){
        for(int i=0; i<completeLocations.length; ++i){
          if(getStop()){
            return false;
          }
          try{
            if(locations.getComplete()[i].equalsIgnoreCase(homeSite)){
              continue;
            }
            dq2Access.registerLocation(datasetID, dsn,
                true, completeLocations[i]);
          }
          catch(Exception e){
          }
        }
      }
      if(incompleteLocations!=null && incompleteLocations.length>0){
        for(int i=0; i<incompleteLocations.length; ++i){
          if(getStop()){
            return false;
          }
          try{
            if(locations.getIncomplete()[i].equalsIgnoreCase(homeSite)){
              continue;
            }
            dq2Access.registerLocation(datasetID, dsn,
                false, incompleteLocations[i]);
          }
          catch(Exception e){
          }
        }
      }
      // Re-register home location if we failed to delete all files
      if(!deletePhysOk || toKeepGuids.length>0){
        try{
          dq2Access.registerLocation(datasetID, dsn,
              (complete && !atLeastOneDeleted), homeSite);
        }
        catch(Exception ee){
          ee.printStackTrace();
        }
      }
      else{
        // Otherwise clear the home location
        try{
          dq2Access.deleteFromSite(datasetID, homeSite);
        }
        catch(Exception ee){
        }
      }
    }
    catch(Exception eee){
      eee.printStackTrace();
      error = "WARNING: could not connect to "+dq2Url+" on port "+dq2SecurePort+". Writing " +
         "not possible";
      logFile.addMessage(error, eee);
      return false;
    }
    return true;
  }
  
  private int deletePhysicalFiles(String datasetID, String [] toDeleteLfns)
     throws IOException, InterruptedException{
    int deleted = 0;
    // Delete the physical files.
    // First store them all in a Vector.
    Vector pfns = new Vector();
    for(int i=0; i<toDeleteLfns.length; ++i){
      if(getStop()){
        throw new InterruptedException();
      }
      // Get the pfns
      if(DELETE_FROM_SITES_ON_ALL_CATALOGS){
        findPFNs(datasetID, toDeleteLfns[i], false);
        for(int j=2; j<pfnVector.size(); ++j){
          pfns.add((String) pfnVector.get(j));
        }
      }
      else{
        try{
          // if we're using an alias
          if(homeServerMysqlAlias!=null){
            Collections.addAll(pfns, lookupPFNs(homeServerMysqlAlias, toDeleteLfns[i], true));
          }
          // otherwise, it is assumed that we're using a mysql catalog
          else{
            Collections.addAll(pfns, lookupPFNs(homeSite, toDeleteLfns[i], true));
          }
        }
        catch(Exception e){
          logFile.addMessage("WARNING: failed to find PFNs to delete, "+
              Util.arrayToString(toDeleteLfns)+
              " on "+homeServerMysqlAlias+" or "+homeSite+". Please delete them by hand.");
        }
      }
    }      
    // Then construct a HashMap of Vectors of files on the same server.
    HashMap pfnHashMap = new HashMap();
    String pfn = null;
    String host = null;
    for(Iterator it=pfns.iterator(); it.hasNext();){
      pfn = (String) it.next();
      try{
        host = (new GlobusURL(pfn)).getHost();
      }
      catch(Exception e){
        logFile.addMessage("WARNING: cannot delete physical file "+
            pfn+". Please delete this file by hand.");
      }
      if(!pfnHashMap.containsKey(host)){
        Vector hostPFNs = new Vector();
        hostPFNs.add(pfn);
        pfnHashMap.put(host, hostPFNs);
      }
      else{
        ((Vector) pfnHashMap.get(host)).add(pfn);
      }
    }
    // Now try to delete the batches of files.
    Set hosts = pfnHashMap.keySet();
    Vector hostPFNs = null;
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
    for(Iterator it=hosts.iterator(); it.hasNext();){
      if(getStop()){
        break;
      }
      ++i;
      GridPilot.getClassMgr().getStatusBar().setLabel("Record "+i+" : "+hosts.size());
      pb.setValue(i);
      try{
        host = (String) it.next();
        hostPFNs = (Vector) pfnHashMap.get(host);
        urls = new GlobusURL [hostPFNs.size()];
        int j = 0;
        for(Iterator itt=hostPFNs.iterator(); itt.hasNext(); ++j){
          urls[j] = new GlobusURL((String) itt.next());
        }
        GridPilot.getClassMgr().getStatusBar().setLabel("Deleting "+
           Util.arrayToString(urls));
        TransferControl.deleteFiles(urls);
        deleted = deleted+urls.length;
      }
      catch(Exception e){
        logFile.addMessage("WARNING: failed to delete physical files "+
            Util.arrayToString(urls)+". Please delete these files by hand.");
      }
    }
    GridPilot.getClassMgr().getStatusBar().removeProgressBar(pb);
    return deleted;
  }

  /**
   * Returns the files registered in DQ for a given dataset id (vuid).
   */
  public DBResult getFiles(String datasetID){
    boolean oldFindPFNs = findPFNs;
    setFindPFNs(false);
    DBResult res = select("SELECT * FROM file WHERE vuid = "+datasetID, "guid", false);
    setFindPFNs(oldFindPFNs);
    return res;
  }

  public void registerFileLocation(String vuid, String dsn, String guid,
      String lfn, String url, String size, String checksum, boolean datasetComplete) throws Exception {

    DQ2Access dq2Access = null;
    try{
      dq2Access = new DQ2Access(dq2Server, Integer.parseInt(dq2SecurePort), dq2Path);
    }
    catch(Exception e){
      error = "WARNING: could not connect to DQ2 at "+dq2Url+" on port "+dq2SecurePort+
      ". Registration of "+lfn+" in DQ2 will NOT be done";
      logFile.addMessage(error, e);
    }
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
          error = "WARNING: dataset "+dsn+" already registered in DQ2 with name "+
          existingID+"!="+vuid+". Using "+existingID+".";
          logFile.addMessage(error);
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
        vuid = dq2Access.createDataset(dsn, null, null);
        datasetExists = true;
      }
      catch(Exception e){
        error = "WARNING: could not create dataset "+dsn+" in DQ2 "+
        ". Registration of "+lfn+" in DQ2 will NOT be done";
        logFile.addMessage(error, e);
        datasetExists =false;
      }
    }
    
    if(datasetExists){
      if(size!=null && !size.endsWith("L")){
        // The DQ2 conventions seems to be that the file size is like e.g. 41943040L 
        size = size+"L";
      }
      if(checksum!=null && !checksum.matches("\\w+:.*")){
        // If no type is given, we assume it's an md5 sum.
        checksum = "md5:"+checksum;
      }
      
      String [] guids = new String[] {guid};
      String [] lfns = new String[] {lfn};
      String [] sizes = new String[] {size};
      String [] checksums = new String[] {checksum};
      try{
        GridPilot.getClassMgr().getStatusBar().setLabel("Registering new lfn " +lfn+
          " with DQ2");
        dq2Access.addLFNsToDataset(vuid, lfns, guids, sizes, checksums);
      }
      catch(Exception e){
        error = "WARNING: could not update dataset "+dsn+" in DQ2 "+
        ". Registration of "+lfn+" in DQ2 NOT done";
        GridPilot.getClassMgr().getStatusBar().setLabel("Registration of " +lfn+
        " with DQ2 FAILED!");
        logFile.addMessage(error, e);
        throw e;
      }
    }
    
    // Register in home MySQL catalog.
    boolean catalogRegOk = true;
    try{
      if(homeServerMysqlAlias==null || homeServerMysqlAlias.equals("")){
        throw new Exception("Cannot register when no home mysql server defined");
      }
      GridPilot.getClassMgr().getStatusBar().setLabel("Registering new location " +url+
          " in file catalog "+homeServerMysqlAlias);
      
      try{
        // if we're using an alias, write in alias and flag for writing
        if(homeServerMysqlAlias!=null){
          registerLFNs(homeServerMysqlAlias, new String [] {guid},
              new String [] {lfn}, new String [] {url}, true);
        }
        // otherwise, assume that home server is a mysql server and just write there
        else{
          registerLFNs(toa.getFileCatalogServer(homeSite, false), new String [] {guid},
              new String [] {lfn}, new String [] {url}, false);
        }
      }
      catch(Exception e){
        logFile.addMessage("WARNING: failed to register LFN "+lfn+
            " on "+homeServerMysqlAlias+". Please delete them by hand.", e);
      }
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
        GridPilot.getClassMgr().getStatusBar().setLabel("Checking if lfn " +lfn+
           " is registered with "+homeSite+" in DQ2");
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
          GridPilot.getClassMgr().getStatusBar().setLabel("Registering new lfn " +lfn+
          " with "+homeSite+" in DQ2");
          dq2Access.registerLocation(vuid, dsn, datasetComplete, homeSite);
        }
        else{
          GridPilot.getClassMgr().getStatusBar().setLabel("Yes!");
        }
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
    
    Debug.debug("Creating dataset "+Util.arrayToString(fields)+
        " --> "+Util.arrayToString(values), 2);
    
    String dsn = null;
    String vuid = null;
    Vector fieldStrings = new Vector();
    Vector valueStrings = new Vector();
    
    if(table==null || !table.equalsIgnoreCase("dataset")){
      error = "ERROR: could not use table "+table;
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
      logFile.addMessage(error);
      return false;
    }
    DQ2Access dq2Access = null;
    try{
      dq2Access = new DQ2Access(dq2Server, Integer.parseInt(dq2SecurePort), dq2Path);
      for(int i=0; i<values.length; ++i){
        if(fields[i].equalsIgnoreCase("dsn")){
          dsn = (String) values[i];
        }
        if(fields[i].equalsIgnoreCase("vuid") &&
            values[i]!=null && !values[i].toString().equals("")){
          vuid = (String) values[i];
        }
        else if(values[i]!=null && !values[i].toString().equals("") &&
            !values[i].toString().equals("''")){
          fieldStrings.add((String) fields[i]);
          valueStrings.add((String) values[i]);
        }
      }
      if(dsn==null || dsn.equals("")){
        throw new Exception ("dsn empty: "+dsn);
      }
      
      if(getStop()){
        return false;
      }
      vuid = dq2Access.createDataset(dsn, vuid, null);
    }
    catch(Exception e){
      error = "ERROR: could not connect to DQ2 dataset at "+dq2Server+" on port "+dq2SecurePort+
      " and with path "+dq2Path;
      logFile.addMessage(error, e);
      return false;
    }
    if(valueStrings.size()>0){
      String [] fieldArray = new String [fieldStrings.size()];
      String [] valueArray = new String [valueStrings.size()];
      for(int i=0; i<fieldStrings.size(); ++i){
        fieldArray[i] = (String) fieldStrings.get(i);
        valueArray[i] = (String) valueStrings.get(i);
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
    
    DQ2Access dq2Access = null;
    try{
      dq2Access = new DQ2Access(dq2Server, Integer.parseInt(dq2SecurePort), dq2Path);
    }
    catch(Exception e){
      error = "ERROR: could connect to DQ2 at "+dq2Url+" on port "+dq2SecurePort;
      logFile.addMessage(error, e);
      return false;
    }
    
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
          error = "ERROR: cannot change vuid. "+Util.arrayToString(values);
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
        logFile.addMessage(error+" "+vuid);
        //return false;
      }
      // dsn
      else if(fields[i].equalsIgnoreCase("dsn")){
        error = "WARNING: cannot change dsn";
        //GridPilot.getClassMgr().getStatusBar().setLabel(error);
        logFile.addMessage(error+" "+dsn);
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
              String [] newLocations = Util.split(values[i]);
              Set newLocationsSet = new HashSet();
              Collections.addAll(newLocationsSet, newLocations);
              // Delete locations
              for(int j=0; j<dqLocations[0].getComplete().length; ++j){
                if(newLocationsSet.contains(dqLocations[0].getComplete()[j])){
                  continue;
                }
                Debug.debug("Deleting location "+dqLocations[0].getComplete()[j]+
                    " for vuid "+vuid, 2);
                dq2Access.deleteFromSite(vuid, dqLocations[0].getComplete()[j]);
              }
              // Set the new ones
              Set oldLocationsSet = new HashSet();
              Collections.addAll(oldLocationsSet, dqLocations[0].getComplete());
              for(int j=0; j<newLocations.length; ++j){
                if(oldLocationsSet.contains(newLocations[j])){
                  continue;
                }
                Debug.debug("Registering location "+newLocations[j]+
                    " for vuid "+vuid, 2);
                dq2Access.registerLocation(vuid, dsn, true, newLocations[j]);
              }
            }
            //BNLTAPE CERNPROD CYF LYONTAPE CSCS
            else if(fields[i].equalsIgnoreCase("incomplete")){
              String [] newLocations = Util.split(values[i]);
              Set newLocationsSet = new HashSet();
              Collections.addAll(newLocationsSet, newLocations);
              for(int j=0; j<dqLocations[0].getIncomplete().length; ++j){
                if(newLocationsSet.contains(dqLocations[0].getIncomplete()[j])){
                  continue;
                }
                Debug.debug("Deleting location "+dqLocations[0].getIncomplete()[j]+
                    " for vuid "+vuid, 2);
                dq2Access.deleteFromSite(vuid, dqLocations[0].getIncomplete()[j]);
              }
              Set oldLocationsSet = new HashSet();
              Collections.addAll(oldLocationsSet, dqLocations[0].getIncomplete());
              for(int j=0; j<newLocations.length; ++j){
                if(oldLocationsSet.contains(newLocations[j])){
                  continue;
                }
                Debug.debug("Registering location "+newLocations[j]+
                    " for vuid "+vuid, 2);
                dq2Access.registerLocation(vuid, dsn, false, newLocations[j]);
              }
            }
          }
          catch(IOException e){
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

  public boolean deleteDataset(String datasetID, boolean cleanup){
    
    if(getStop()){
      return false;
    }
    
    clearCacheEntries("dataset");
    
    DQ2Access dq2Access = null;
    try{
      dq2Access = new DQ2Access(dq2Server, Integer.parseInt(dq2SecurePort), dq2Path);
    }
    catch(Exception e){
      error = "ERROR: could connect to DQ2 at "+dq2Url+" on port "+dq2SecurePort;
      logFile.addMessage(error, e);
      return false;
    }
    
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
      dq2Access.deleteDataset(dsn, datasetID);
    }
    catch(IOException e){
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
    HashSet deleteKeys = new HashSet();
    for(Iterator it=queryResults.keySet().iterator(); it.hasNext();){
      thisSql = (String) it.next();
      theseTableNames = Util.getTableNames(thisSql);
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
    for(Iterator it=deleteKeys.iterator(); it.hasNext();){
      thisSql = (String) it.next();
      Debug.debug("--> "+thisSql, 3);
      queryResults.remove(thisSql);
    }
  }

  // -------------------------------------------------------------------
  // What's below here is not relevant for this plugin
  // -------------------------------------------------------------------


  public String getDatasetTransformationName(String datasetID){
    return null;
  }

  public String getDatasetTransformationVersion(String datasetID){
    return null;
  }
  public String[] getRuntimeEnvironments(String jobDefID){
    return null;
  }

  public DBResult getRuntimeEnvironments(){
    return null;
  }

  public String getRuntimeEnvironmentID(String name, String cs){
    return "-1";
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

  public DBResult getTransformations(){
    return null;
  }

  public DBRecord getTransformation(String transformationID){
    return null;
  }

  public String getTransformationID(String transName, String transVersion){
    return "-1";
  }

  public boolean createTransformation(Object[] values){
    return false;
  }

  public boolean updateTransformation(String transformatinID, String[] fields,
      String[] values){
    return false;
  }

  public boolean deleteTransformation(String transformationID){
    return false;
  }

  public String[] getVersions(String transformationName){
    return null;
  }

  public String getTransformationRuntimeEnvironment(String transformationID){
    return null;
  }

  public String[] getTransformationJobParameters(String transformationID){
    return null;
  }

  public String[] getTransformationOutputs(String transformationID){
    return null;
  }

  public String[] getTransformationInputs(String transformationID){
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

  public boolean deleteJobDefinition(String jobDefID, boolean cleanup){
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

  public String getJobDefTransformationID(String jobDefID){
    return "-1";
  }

  public String getTransformationScript(String jobDefID){
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

  public String[] getTransformationArguments(String jobDefID){
    return null;
  }

  public String[] getJobDefTransPars(String jobDefID){
    return null;
  }

  public boolean setJobDefsField(String [] identifiers, String field, String value){
    return false;
  }

  public String getError(){
    return error;
  }

}
