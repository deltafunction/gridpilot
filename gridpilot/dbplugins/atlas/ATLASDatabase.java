package gridpilot.dbplugins.atlas;

import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JProgressBar;
import javax.xml.rpc.ServiceException;

import org.globus.util.GlobusURL;

import gridpilot.ConfigFile;
import gridpilot.DBRecord;
import gridpilot.DBResult;
import gridpilot.Database;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.LogFile;
import gridpilot.MyThread;
import gridpilot.TransferControl;
import gridpilot.Util;

public class ATLASDatabase implements Database{
  
  private String error;
  private String dq2Server;
  private String dq2Port;
  private String dq2SecurePort;
  private String dq2Path;
  private String dq2Url;
  private String dbName;
  private String toa;
  private String homeServer;
  private String homeServerMysqlAlias;
  private LogFile logFile;
  private File toaFile;
  private Vector pfnVector = null;
  private int fileCatalogTimeout = 1000;
  // Hash of catalog server mappings found in TiersOfAtlas
  private HashMap fileCatalogs = new HashMap();
  private boolean findPFNs = true;
  // If forceDelete is set to true, files will be attempted deleted on
  // all physical locations and on the home catalog server MySQL alias
  // and the home server will be de-registered in DQ, even if other
  // catalog sites are registered in DQ than the home catalog or if there
  // is no home catalog set.
  private boolean forceDelete = false;

  public ATLASDatabase(String _dbName){
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();
    dbName = _dbName;

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

    // Set home server and possible mysql alias
    homeServer = configFile.getValue(dbName, "home catalog server");
    if(homeServer!=null){
      String [] servers = Util.split(homeServer);
      if(servers.length==2){
        homeServer = servers[0];
        homeServerMysqlAlias = servers[1];
      }
      else if(servers.length==0 || servers.length>2){
        homeServer = null;
      }
    }
    // Get and cache the TOA file
    toa = configFile.getValue(dbName, "tiers of atlas");
    try{
      URL toaURL = null;
      toaFile = File.createTempFile(/*prefix*/"GridPilot-TOA", /*suffix*/"");
      toaFile.delete();
      toa.replaceFirst("^file:///+", "/");
      toa.replaceFirst("^file://", "");
      toa.replaceFirst("^file:", "");
      if(toa.startsWith("~")){
        toa = System.getProperty("user.home") + File.separator +
        toa.substring(1);
      }
      if(toa.matches("\\w:.*") || toa.indexOf(":")<0){
        toaURL = (new File(toa)).toURL();
      }
      else{
        toaURL = new URL(toa);
      }
      BufferedReader in = new BufferedReader(new InputStreamReader(toaURL.openStream()));
      PrintWriter out = new PrintWriter(
          new FileWriter(toaFile)); 
      String line = null;
      while((line = in.readLine())!=null){
        out.println(line);
      }
      in.close();
      out.close();
      // hack to have the diretory deleted on exit
      GridPilot.tmpConfFile.put(toaFile.getName(), toaFile);
    }
    catch(Exception e){
      error = "WARNING: could not load tiers of atlas. File catalog lookups " +
          "are disabled";    
      logFile.addMessage(error, e);
    }
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

  public boolean isFileCatalog(){
    return true;
  }

  public boolean isJobRepository(){
    return false;
  }
  
  public String connect(){
    return null;
  }

  public void disconnect(){
  }

  public void clearCaches(){
  }
  
  private void setFindPFNs(boolean doit){
    findPFNs = doit;
  }
  
  public DBResult select(String selectRequest, String identifier, boolean findAll){
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
      return new DBResult(fields, new String[0][fields.length]);
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

      // Construct get string
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
      
      get = dq2Url+"ws_repository/dataset?version=0&"+get;

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
      String str = readGetUrl(url);
      // Check if the result is of the form {...}
      if(str== null || !str.matches("^\\{.*\\}$")){
        Debug.debug("WARNING: search returned an error "+str, 1);
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
                vuids[j] = vuids[j].replace("'", "");
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
            record = Util.split(records[i], "'dsn': ");
            String name = record[1].replaceFirst(", 'version': \\d+\\}", "");
            name = name.replaceAll("'", "");
            // If some selection boxes have been set, use patterns for restricting.
            complete = complete.replaceAll("\\*", ".*");
            incomplete = incomplete.replaceAll("\\*", ".*");
            dsn = dsn.replaceAll("\\*", ".*");
            for(int k=0; k<fields.length; ++k){
              if(fields[k].equalsIgnoreCase("dsn")){
                if(dsn==null || dsn.equals("") ||
                    name.matches("(?i)"+dsn)){
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
      return res;
    }
   //---------------------------------------------------------------------
    else if(table.equalsIgnoreCase("file")){
      // "dsn", "lfn", "pfns", "guid" - to save lookups allow also search on vuid
      String dsn = "";
      String lfn = "";
      String pfns = "";
      String guid = "";      
      String vuid = "";

      // Construct get string
      get = conditions.replaceAll("(?i)\\bvuid = (\\S+)", "vuid=$1");
      get = get.replaceAll("(?i)\\bdsn = (\\S+)", "dsn=$1");
      get = get.replaceAll("(?i)\\blfn = (\\S+)", "lfn=$1");
      get = get.replaceAll("(?i)\\bpfns = (\\S+)", "pfns=$1");
      get = get.replaceAll("(?i)\\bguid = (\\S+)", "guid=$1");
      
      get = get.replaceAll("(?i)\\bvuid CONTAINS (\\S+)", "vuid=*$1*");
      get = get.replaceAll("(?i)\\bdsn CONTAINS (\\S+)", "dsn=*$1*");
      get = get.replaceAll("(?i)\\blfn CONTAINS (\\S+)", "lfn=*$1*");
      get = get.replaceAll("(?i)\\bpfns CONTAINS (\\S+)", "pfns=*$1*");
      get = get.replaceAll("(?i)\\bguid CONTAINS (\\S+)", "guid=*$1*");

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
      
      get = dq2Url+"ws_content/files?"+get;

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
      String str = readGetUrl(url);
      // Check if the result is of the form {...}
      if(!str.matches("^\\{.*\\}$")){
        Debug.debug("WARNING: search returned an error "+str, 1);
        return new DBResult(fields, new String[0][fields.length]);
      }
      // Now parse the DQ string and construct DBRecords
      str = str.replaceFirst("^\\{", "");
      str = str.replaceFirst("\\}$", "");
      String [] records = Util.split(str, ", ");
      if(records==null || records.length==0){
        Debug.debug("WARNING: no records found with "+str, 2);
        return new DBResult(fields, new String[0][fields.length]);
      }
      
      Vector valuesVector = new Vector();
      String [] record = null;
      JProgressBar pb = new JProgressBar();
      pb.setMaximum((records.length));
      GridPilot.getClassMgr().getStatusBar().setProgressBar(pb);
      pb.setToolTipText("click here to cancel");
      pb.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent me){
          setFindPFNs(false);
        }
      });
      for(int i=0; i<records.length; ++i){
        GridPilot.getClassMgr().getStatusBar().setLabel("Record "+(i+1)+" : "+records.length);
        pb.setValue(i+1);
        Vector recordVector = new Vector();
        boolean exCheck = true;
        record = Util.split(records[i], ": ");
        
        if(record!=null && record.length>1){
          // If the string is the result of a vuid=... request, the
          // split went ok and this should work:
          guid = record[0].replaceAll("'", "");
          lfn = record[1].replaceAll("'", "");
          
          if(findPFNs){
            try{
              findPFNs(vuid, lfn, findAll);
            }
            catch(Exception e){
              e.printStackTrace();
            }
            pfns = getPFNsString();
          }
          else{
            pfns = "";
          }
   
          recordVector = new Vector();
          exCheck = true;
          for(int k=0; k<fields.length; ++k){
            if(fields[k].equalsIgnoreCase("lfn")){
              recordVector.add(lfn);
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
      }
      setFindPFNs(true);
      GridPilot.getClassMgr().getStatusBar().removeProgressBar(pb);
      values = new String[valuesVector.size()][fields.length];
      for(int i=0; i<valuesVector.size(); ++i){
        for(int j=0; j<fields.length; ++j){
          values[i][j] = ((Object []) valuesVector.get(i))[j].toString();
        }
        Debug.debug("Adding record "+Util.arrayToString(values[i]), 3);
      }
      DBResult res = new DBResult(fields, values);
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
    /*curl --user-agent "dqcurl" --silent --get --insecure --data
    "dsns=%5B%5D" --data "vuids=%5B%27cdced2bd-5217-423a-9690-8b2bb5b48fa8%27%5D"
    http://atlddmpro.cern.ch:8000/dq2/ws_location/dataset
    dsns=[]&vuids=['cdced2bd-5217-423a-9690-8b2bb5b48fa8']*/
    String url = dq2Url+"ws_location/dataset?"+
       "dsns=[]&vuids="+URLEncoder.encode("["+vuidsString+"]", "utf-8");
    String ret = null;
    try{
      Debug.debug(">>> get string was : "+dq2Url+"ws_location/dataset?"+
          "dsns=[]&vuids="+"["+vuidsString+"]", 3);
      ret = readGetUrl(new URL(url));
      ret = URLDecoder.decode(ret, "utf-8");
    }
    catch(Exception e){
      error = "WARNING: problem with URL "+url+". "+e.getMessage();
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
    String dec = "";
    try{
      dec = URLDecoder.decode(str.toString(), "utf-8");
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
    return dec;
  }
  
  /**
   * Returns an array of SURLs for the given file name (lfn).
   * The catalog server string must be of the form
   * lfc://lfc-fzk.gridka.de:/grid/atlas/ or
   * mysql://dsdb-reader:dsdb-reader1@db1.usatlas.bnl.gov:3306/localreplicas .
   * The lfn must be of the form
   * csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103._00001.pool.root;
   * then, in the case of LFC, the following lfn is looked for
   * /grid/atlas/datafiles/
   * csc11/recon/
   * csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103/
   * csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103._00001.pool.root.
   */
  private String [] getPFNs(String _catalogServer, String lfn, boolean findAll)
     throws RemoteException, ServiceException, MalformedURLException, SQLException {
    String [] pfns = null;
    // get rid of the :/, which GlobusURL doesn't like
    String catalogServer = _catalogServer.replaceFirst("(\\w):/(\\w)", "$1/$2");
    GlobusURL catalogUrl = new GlobusURL(catalogServer);
    if(catalogUrl.getProtocol().equals("lfc")){
      String path = catalogUrl.getPath()==null ? "" : catalogUrl.getPath();
      String host = catalogUrl.getHost();
      DataLocationInterface dli = new DataLocationInterfaceLocator();
      /*e.g. "http://lfc-atlas.cern.ch:8085", "http://lxb1941.cern.ch:8085"
             "http://lfc-atlas-test.cern.ch:8085" */
      Debug.debug("Connecting to DLI web service at http://"+host+":8085", 3);
      URL dliUrl = new URL("http://"+host+":8085");
      // Prepend the directory, following ATLAS conventions, e.g.
      // csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103._00001.pool.root ->
      // /grid/atlas/dq2/csc11/csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103/
      String atlasLPN = "/"+path+(path.endsWith("/")?"":"/");
      String [] lfnMetaData = Util.split(lfn, "\\.");
      if(lfnMetaData.length==8 || lfnMetaData.length==9 || lfnMetaData.length==10){
        atlasLPN += /*datafiles*/"dq2/"+lfnMetaData[0];
        //atlasLPN += "/"+lfnMetaData[3];
        atlasLPN += "/"+lfnMetaData[0]+"."+lfnMetaData[1]+"."+lfnMetaData[2]+"."+
           lfnMetaData[3]+"."+lfnMetaData[4]+"."+lfnMetaData[5];
        atlasLPN += "/"+lfn;
      }
      else{
        atlasLPN = atlasLPN+lfn;
      }
      Debug.debug("LPN: "+atlasLPN, 3);
      pfns = dli.getDataLocationInterface(dliUrl).listReplicas(
          "lfn", atlasLPN);
      if(!findAll && pfns!=null && pfns.length>1){
        pfns = new String [] {pfns[0]};
      }
      Debug.debug("PFNs: "+Util.arrayToString(pfns), 3);
      return pfns;
    }
    else if(catalogUrl.getProtocol().equals("mysql")){
      // Set parameters
      String driver = "org.gjt.mm.mysql.Driver";
      String port = catalogUrl.getPort()==-1 ? "" : ":"+catalogUrl.getPort();
      String user = catalogUrl.getUser()==null ? "" : catalogUrl.getUser();
      String passwd = catalogUrl.getPwd()==null ? "" : catalogUrl.getPwd();
      String path = catalogUrl.getPath()==null ? "" : "/"+catalogUrl.getPath();
      String host = catalogUrl.getHost();
      String database = "jdbc:mysql://"+host+port+path;
      boolean gridAuth = false;
      // The (GridPilot) convention is that if no user name is given (in TOA), we use
      // gridAuth to authenticate
      if(user.equals("")){
        gridAuth = true;
      }
      // Make the connection
      Connection conn = Util.sqlConnection(driver, database, user, passwd, gridAuth);
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
        error = "ERROR: No guid with found for lfn "+lfn;
        Debug.debug(error, 1);
        throw new SQLException(error);
      }
      else if(resultVector.size()>1){
        error = "WARNING: More than one ("+resultVector.size()+") guids with found for lfn "+lfn;
        Debug.debug(error, 1);
      }
      guid = (String) resultVector.get(0);
      // Now query the t_pfn table to get the pfn
      req = "SELECT pfname FROM t_pfn WHERE guid ='"+guid+"'";
      Debug.debug(">> "+req, 3);
      rset = conn.createStatement().executeQuery(req);
      resultVector = new Vector();
      String [] res = null;
      while(rset.next()){
        res = Util.split(rset.getString("pfname"));
        for(int i=0; i<res.length; ++i){
          resultVector.add(res[i]);
        }
      }
      if(resultVector.size()==0){
        error = "ERROR: No pfns with found for guid "+guid;
        Debug.debug(error, 1);
        throw new SQLException(error);
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
    }
    else{
      error = "ERROR: protocol not supported: "+catalogUrl.getProtocol();
      Debug.debug(error, 1);
      throw new MalformedURLException(error);
    }
    return pfns;
  }

  /**
   * Deletes an array of SURLs for the given file name (lfn).
   * The catalog server string must be of the form
   * mysql://dsdb-reader:dsdb-reader1@db1.usatlas.bnl.gov:3306/localreplicas.
   * LFC is not supported.
   */
  private void deleteLFNs(String _catalogServer, String [] lfns)
     throws RemoteException, ServiceException, MalformedURLException, SQLException {
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
      String database = "jdbc:mysql://"+host+port+path;
      boolean gridAuth = false;
      // The (GridPilot) convention is that if no user name is given (in TOA), we use
      // gridAuth to authenticate
      if(user.equals("")){
        gridAuth = true;
      }
      // Make the connection
      Connection conn = Util.sqlConnection(driver, database, user, passwd, gridAuth);
      String lfn = null;
      int rowsAffected = 0;
      for(int i=0; i<lfns.length; ++i){
        try{
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
          // Now delete this guid from the t_lfn, t_pfn and t_meta tables
          req = "DELETE FROM t_lfn WHERE guid ='"+guid+"'";
          Debug.debug(">> "+req, 3);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not delete guid "+guid+" from t_lfn on "+catalogServer;
            logFile.addMessage(error);
          }
          req = "DELETE FROM t_pfn WHERE guid ='"+guid+"'";
          Debug.debug(">> "+req, 3);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not delete guid "+guid+" from t_pfn on "+catalogServer;
            logFile.addMessage(error);
          }
          req = "DELETE FROM t_meta WHERE guid ='"+guid+"'";
          Debug.debug(">> "+req, 3);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not delete guid "+guid+" from t_meta on "+catalogServer;
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
   * Flags a set of LFNs to be deleted on a MySQL alias catalog.
   */
  private void setDeleteLFNs(String _catalogServer, String [] lfns)
     throws RemoteException, ServiceException, MalformedURLException, SQLException {
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
      String database = "jdbc:mysql://"+host+port+path;
      boolean gridAuth = false;
      // The (GridPilot) convention is that if no user name is given (in TOA), we use
      // gridAuth to authenticate
      if(user.equals("")){
        gridAuth = true;
      }
      // Make the connection
      Connection conn = Util.sqlConnection(driver, database, user, passwd, gridAuth);
      String lfn = null;
      int rowsAffected = 0;
      for(int i=0; i<lfns.length; ++i){
        try{
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
      String database = "jdbc:mysql://"+host+port+path;
      boolean gridAuth = false;
      // The (GridPilot) convention is that if no user name is given (in TOA), we use
      // gridAuth to authenticate
      if(user.equals("")){
        gridAuth = true;
      }
      // Make the connection
      Connection conn = Util.sqlConnection(driver, database, user, passwd, gridAuth);
      int rowsAffected = 0;
      String req = null;
      // Do the insertions in t_lfn and t_pfn
      for(int i=0; i<lfns.length; ++i){
        try{
          req = "INSERT INTO t_lfn (lfname, guid) VALUES " +
             "("+lfns[i]+", "+guids[i]+")";
          Debug.debug(">> "+req, 3);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not insert lfn "+lfns[i]+" on "+catalogServer;
            logFile.addMessage(error);
          }
          req = "INSERT INTO t_pfn (pfname, guid) VALUES " +
          "("+pfns[i]+", "+guids[i]+")";
          Debug.debug(">> "+req, 3);
          rowsAffected = conn.createStatement().executeUpdate(req);
          if(rowsAffected==0){
            error = "WARNING: could not insert pfn "+pfns[i]+" on "+catalogServer;
            logFile.addMessage(error);
          }
        }
        catch(Exception e){
          error = "ERROR: could not insert lfn or lfn/pfn "+lfns[i]+"/"+pfns[i]+" on "+catalogServer;
          logFile.addMessage(error);
          GridPilot.getClassMgr().getStatusBar().setLabel(error);
        }
        if(sync){
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
    else{
      error = "ERROR: protocol not supported: "+catalogUrl.getProtocol();
      Debug.debug(error, 1);
      throw new MalformedURLException(error);
    }
  }

  private String getFileCatalogServer(String name)
     throws MalformedURLException, IOException {
    
    if(fileCatalogs.containsKey(name)){
      return (String) fileCatalogs.get(name);
    }
    
    String catalogSite = null;
    String catalogServer = null;
    // Parse TOA file
    BufferedReader in = new BufferedReader(new InputStreamReader((toaFile.toURL()).openStream()));
    String line = null;
    
    Debug.debug("Trying to match "+name, 3);
    while((line = in.readLine())!=null){
      // Say, we havea name 'CSCS'; first look for lines like
      // 'FZKSITES': [ 'FZK', 'FZU', 'CSCS', 'CYF', 'DESY-HH', 'DESY-ZN', 'UNI-FREIBURG', 'WUP' ],
      // 'FZK': [ 'FZKDISK', 'FZKTAPE' ],
      if(line.matches("^\\W*(\\w*SITES)\\W*\\s*:\\s*\\[.*\\W+"+name+"\\W+.*")){
        catalogSite = line.replaceFirst("^\\W*(\\w*SITES)\\W*\\s*:.*", "$1");
        Debug.debug("Catalog site: "+catalogSite, 3);
      }
      else if(line.matches("^\\W*(\\w*)\\W*\\s*:\\s*\\[.*\\W+"+name+"\\W+.*") &&
          line.indexOf("SITES")<0){
        catalogSite = line.replaceFirst("^\\W*(\\w*)\\W*\\s*:.*",
            "$1")+
        "SITES";
        Debug.debug("Catalog site: "+catalogSite, 3);
      }
      // Now look for
      // FZKLFC = 'lfc://lfc-fzk.gridka.de:/grid/atlas/'
      if(catalogSite!=null &&
          line.matches("^\\s*"+catalogSite.substring(0, catalogSite.length()-5)+
              "LFC\\s*=\\s*'(.+)'.*")){
        catalogServer = line.replaceFirst("^\\s*"+catalogSite.substring(0, catalogSite.length()-5)+
            "LFC\\s*=\\s*'(.+)'.*", "$1");
        Debug.debug("Catalog server: "+catalogServer, 3);
        break;
      }
      else if(catalogSite!=null &&
          line.matches("^\\s*"+catalogSite.substring(0, catalogSite.length()-5)+
              "LRC\\s*=\\s*'(.+)'.*")){
        catalogServer = line.replaceFirst("^\\s*"+catalogSite.substring(0, catalogSite.length()-5)+
            "LRC\\s*=\\s*'(.+)'.*", "$1");
        Debug.debug("Catalog server: "+catalogServer, 3);
        break;
      }
      else if(catalogSite!=null &&
          line.matches("^\\s*"+name+
              "LFC\\s*=\\s*'(.+)'.*")){
        catalogServer = line.replaceFirst("^\\s*"+catalogSite.substring(0, catalogSite.length()-5)+
            "LFC\\s*=\\s*'(.+)'.*", "$1");
        Debug.debug("Catalog server: "+catalogServer, 3);
        break;
      }
      else if(catalogSite!=null &&
          line.matches("^\\s*"+name+
              "LRC\\s*=\\s*'(.+)'.*")){
        catalogServer = line.replaceFirst("^\\s*"+catalogSite.substring(0, catalogSite.length()-5)+
            "LRC\\s*=\\s*'(.+)'.*", "$1");
        Debug.debug("Catalog server: "+catalogServer, 3);
        break;
      }
    }
    in.close();
    fileCatalogs.put(name, catalogServer);
    return catalogServer;
  }

  /**
   * Help functions, needed because we query the file catalogs in threads.
   */
  private void addPFN(String pfn){
    pfnVector.add(pfn);
  }

  private void clearPFNs(){
    pfnVector.clear();
  }
  
  private String getPFNsString(){
    return Util.arrayToString(pfnVector.toArray());
  }
  
  private Object [] getPFNs(){
    return pfnVector.toArray();
  }
  
  /**
   * Fill the vector pfnVector with PFNs registered for lfn.
   */
  private void findPFNs(String vuid, String lfn, final boolean findAll){
    clearPFNs();
    // Get the PFNs. Start with the DQ locations
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
    // make sure homeServer is first in the list
    for(int i=0; i<dqLocations.getComplete().length; ++i){
      if(dqLocations.getComplete()[i].equalsIgnoreCase(homeServer)){
        locations.add(homeServer);
        break;
      }
    }
    for(int i=0; i<dqLocations.getIncomplete().length; ++i){
      if(dqLocations.getIncomplete()[i].equalsIgnoreCase(homeServer)){
        locations.add(homeServer);
        break;
      }
    }
    for(int i=0; i<dqLocations.getComplete().length; ++i){
      if(!dqLocations.getComplete()[i].equalsIgnoreCase(homeServer)){
        locations.add(dqLocations.getComplete()[i]);
      }
    }
    for(int i=0; i<dqLocations.getIncomplete().length; ++i){
      if(!dqLocations.getIncomplete()[i].equalsIgnoreCase(homeServer)){
        locations.add(dqLocations.getIncomplete()[i]);
      }
    }
    Debug.debug("Found locations "+Util.arrayToString(locations.toArray()), 3);
    // Query all with a timeout of 5 seconds.    
    // First try the home server if configured
    // Next try the locations with complete datasets, then the incomplete
    final String lfName = lfn;
    final Vector finalLocations = locations;
    try{
      for(int i=0; i<locations.size(); ++i){
        final int ii = i;
        try{
          MyThread t = new MyThread(){
            public void run(){
              try{
                Debug.debug("Querying TOA for "+finalLocations.get(ii), 2);
                String catalogServer = null;
                // If trying to query the home lfc server, use the mysql alias if possible
                if(homeServer!=null && homeServerMysqlAlias!=null &&
                    finalLocations.get(ii).toString().equalsIgnoreCase(homeServer)){
                  catalogServer = homeServerMysqlAlias;
                }
                else{
                  catalogServer = getFileCatalogServer((String) finalLocations.get(ii)); 
                }
                if(catalogServer==null){
                  logFile.addMessage("WARNING: could not find catalog server for "+
                      finalLocations.get(ii));
                  return;
                }
                Debug.debug("Querying "+catalogServer, 2);
                GridPilot.getClassMgr().getStatusBar().setLabel("Querying "+catalogServer);
                try{
                  String [] pfns = getPFNs(catalogServer, lfName, findAll);
                  for(int n=0; n<pfns.length; ++n){
                    addPFN(pfns[n]);
                  }
                }
                catch(Exception e){
                }
              }
              catch(Throwable t){
                logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                                   " from plugin " + dbName, t);
              }
            }
          };
          t.start();              
          if(!Util.waitForThread(t, dbName, fileCatalogTimeout, "select", new Boolean(false))){
            error = "WARNING: timed out waiting for "+locations.get(i);
            logFile.addMessage(error);
            GridPilot.getClassMgr().getStatusBar().setLabel(error);
          }
        }
        catch(Exception e){
        }
        if(!findAll && getPFNsString()!=null && getPFNsString().length()>0){
          break;
        }
      }
      GridPilot.getClassMgr().getStatusBar().setLabel("Querying done.");
    }
    catch(Exception e){
    }
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
      Debug.debug("Found id "+ret+" from "+datasetName, 3);
    }
    catch(Exception e){
      error = "Could not get dataset ID from "+datasetName+". "+e.getMessage();
      e.printStackTrace();
      return "-1";
    }
    if(ret==null){
      return "-1";
    }
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
        return new String [] {"dsn", "lfn", "pfns", "guid"};
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
    DBRecord file = getFile(datasetName, fileName);
    return file.getValue("guid").toString();
  };
  
  public DBRecord getFile(String dsn, String fileID){
    // "dsn", "lfn", "pfns", "guid"
    
    // NOTICE: this query is NOT supported by DQ2. Yak!
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
        break;
      };
    }
    
    // Get the pfns
    Vector pfnVector = new Vector();
    findPFNs(vuid, lfn, false);
    for(int j=0; j<getPFNs().length; ++j){
      pfnVector.add((String) getPFNs()[j]);
    }
    String pfns = Util.arrayToString(pfnVector.toArray());
        
    return new DBRecord(fields, new String [] {dsn, lfn, pfns, fileID});

  }

  public String [] getFileURLs(String datasetName, String fileID){
    String [] ret = null;
    try{
      DBRecord file = getFile(datasetName, fileID);
      ret = Util.split(file.getValue("pfns").toString());
    }
    catch(Exception e){
      Debug.debug("WARNING: could not get URLs. "+e.getMessage(), 1);
    }
    return ret;
  }

  /**
   * Delete file entries in DQ file catalog, delete the corresponding physical files
   * and the entries on MySQL home server.
   */
  public boolean deleteFiles(String datasetID, String [] fileIDs, boolean cleanup){
    // Find the LFNs to keep and those to delete.
    String [] toDeleteLfns = null;
    String [] toKeepLfns = null;
    String [] toKeepGuids = null;
    String dsn = null;
    // NOTICE: we are assuming that there is a one-to-one mapping between
    //         lfns and guids. This is not necessarily the case...
    // TODO: improve
    try{
      GridPilot.getClassMgr().getStatusBar().setLabel("Finding LFNs...");
      dsn = getDatasetName(datasetID);
      DBResult currentFiles = getFiles(datasetID);
      toDeleteLfns = new String[fileIDs.length];
      toKeepGuids = new String[currentFiles.values.length-fileIDs.length];
      toKeepLfns = new String[currentFiles.values.length-fileIDs.length];
      int count = 0;
      int count1 = 0;
      for(int i=0; i<currentFiles.values.length; ++i){
        for(int j=0; j<fileIDs.length; ++j){
          if(!currentFiles.getValue(i, "guid").toString().equalsIgnoreCase(fileIDs[j])){
            toKeepGuids[count] = currentFiles.getValue(i, "guid").toString();
            toKeepLfns[count] = currentFiles.getValue(i, "lfn").toString();
            if(toKeepGuids[count]==null || toKeepLfns[count]==null ||
                toKeepGuids[count].equals("") || toKeepLfns[count].equals("")){
              error = "ERROR: no guid/lfn for "+toKeepGuids[count]+"/"+toKeepLfns[count]+
              ". Aborting delete; nothing deleted.";
              logFile.addMessage(error);
              GridPilot.getClassMgr().getStatusBar().setLabel(error);
              return false;
            }
            ++count;
            break;
          }
          else{
            toDeleteLfns[count1] = currentFiles.getValue(i, "lfn").toString();
            ++count1;
          }
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
      logFile.addMessage(error, e);
      GridPilot.getClassMgr().getStatusBar().setLabel(error);
      return false;
    }

    // First, if cleanup is true, check if ONLY registered in home file catalogue;
    // if so, delete the physical files and clean up the home file catalogue.
    boolean ok = true;
    boolean atLeastOneDeleted = false;
    boolean complete = false;
    DQ2Locations locations = null;
    if(cleanup){
      GridPilot.getClassMgr().getStatusBar().setLabel("Finding locations...");
      // Check that the location is homeServer and that it has a MySQL Alias
      try{
        // Find the locations (to keep)
        locations = getLocations("'"+datasetID+"'")[0];
        if(locations.getIncomplete().length+locations.getIncomplete().length>1){
          throw new Exception("More than one location registered: "+
              Util.arrayToString(locations.getIncomplete())+" "+
              Util.arrayToString(locations.getComplete()));
        }
        String location = null;
        if(locations.getIncomplete().length==1){
          location = locations.getIncomplete()[0];
        }
        else if(locations.getComplete().length==1){
          complete = true;
          location = locations.getComplete()[0];
        }
        if(!location.equalsIgnoreCase(homeServer) || homeServerMysqlAlias==null ||
            homeServerMysqlAlias.equals("")){
          throw new Exception("Can only delete files on home catalog server MySQL alias.");
        }
      }
      catch(Exception e){
        if(!forceDelete){
          error = "ERROR: problem with locations. Aborting.";
          logFile.addMessage(error, e);
          return false;
        }
        else{
          error = "WARNING: problem with locations. There may be orphaned LFNs in DQ2, " +
              "and/or wrongly registered locations in DQ2 and/or " +
              "wrongly registered file catalog entries.";
          logFile.addMessage(error, e);
        }
      }
      
      // Delete the physical files.
      // First store them all in a Vector.
      Vector pfns = new Vector();
      for(int i=0; i<toDeleteLfns.length; ++i){
        // Get the pfns
        findPFNs(datasetID, toDeleteLfns[i], false);
        for(int j=0; j<getPFNs().length; ++j){
          pfns.add((String) getPFNs()[j]);
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
          ok = false;
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
        }
      });
      int i = 0;
      for(Iterator it=hosts.iterator(); it.hasNext();){
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
          atLeastOneDeleted = true;
        }
        catch(Exception e){
          logFile.addMessage("WARNING: failed to delete physical files "+
              Util.arrayToString(urls)+". Please delete these files by hand.");
          ok = false;
        }
      }
      GridPilot.getClassMgr().getStatusBar().removeProgressBar(pb);

      // Remove entries from MySQL catalog
      GridPilot.getClassMgr().getStatusBar().setLabel("Cleaning up home catalog...");
      try{
        // if we're using an alias, just flag for deletion
        if(homeServerMysqlAlias!=null){
          setDeleteLFNs(homeServerMysqlAlias, toDeleteLfns);
        }
        // otherwise, it is assumed that we're using a mysql catalog and we delete
        else{
          deleteLFNs(getFileCatalogServer(homeServer), toDeleteLfns);
        }
      }
      catch(Exception e){
        logFile.addMessage("WARNING: failed to delete LFNs "+Util.arrayToString(toDeleteLfns)+
            " on "+homeServerMysqlAlias+". Please delete them by hand.");
      }  
    }
        
    // Deregister the LFNs from this vuid on DQ2.
    // NOTICE that this changes the vuid of the dataset...
    GridPilot.getClassMgr().getStatusBar().setLabel("Cleaning up DQ dataset catalog...");
    DQ2Access dq2Access = null;
    try{
      dq2Access = new DQ2Access(dq2Server, Integer.parseInt(dq2SecurePort), dq2Path);
      // Delete all locations from old vuid (a new vuid will be created)
      for(int i=0; i<locations.getComplete().length; ++i){
        try{
          dq2Access.deleteFromSite(datasetID, locations.getComplete()[i]);
        }
        catch(Exception e){
        }
      }
      for(int i=0; i<locations.getIncomplete().length; ++i){
        try{
          dq2Access.deleteFromSite(datasetID, locations.getComplete()[i]);
        }
        catch(Exception e){
        }
      }
      // Clear the lfns by creating new dataset with the same dsn
      dq2Access.createNewDatasetVersion(dsn);
      // Add the lfns we don't delete
      dq2Access.addLFNsToDataset(toKeepLfns, toKeepGuids, datasetID);
      // Re-register all locations
      for(int i=0; i<locations.getComplete().length; ++i){
        try{
          if(locations.getComplete()[i].equalsIgnoreCase(homeServer)){
            continue;
          }
          dq2Access.registerLocation(datasetID, dsn,
              true, locations.getComplete()[i]);
        }
        catch(Exception e){
        }
      }
      for(int i=0; i<locations.getIncomplete().length; ++i){
        try{
          if(locations.getIncomplete()[i].equalsIgnoreCase(homeServer)){
            continue;
          }
          dq2Access.registerLocation(datasetID, dsn,
              false, locations.getIncomplete()[i]);
        }
        catch(Exception e){
        }
      }
      // Re-register home location if we failed to delete all files
      if(!ok || toKeepGuids.length>0){
        try{
          dq2Access.registerLocation(datasetID, dsn,
              (complete && !atLeastOneDeleted), homeServer);
        }
        catch(Exception ee){
          ee.printStackTrace();
        }
      }
      else{
        // Otherwise clear the home location
        try{
          dq2Access.deleteFromSite(datasetID, homeServer);
        }
        catch(Exception ee){
        }
      }
    }
    catch(Exception e){
      error = "WARNING: could not connect to "+dq2Url+" on port "+dq2SecurePort+". Writing " +
         "not possible";
      logFile.addMessage(error, e);
      return false;
    }
    return true;
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
      String lfn, String url, boolean datasetComplete) throws Exception {

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
        vuid = dq2Access.createDataset(dsn);
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
      String [] guids = new String[] {guid};
      String [] lfns = new String[] {guid};
      try{
        GridPilot.getClassMgr().getStatusBar().setLabel("Registering new lfn " +lfn+
          " with DQ2");
        dq2Access.addLFNsToDataset(lfns, guids, vuid);
      }
      catch(Exception e){
        error = "WARNING: could not update dataset "+dsn+" in DQ2 "+
        ". Registration of "+lfn+" in DQ2 NOT done";
        logFile.addMessage(error, e);
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
          registerLFNs(homeServerMysqlAlias, new String [] {vuid},
              new String [] {lfn}, new String [] {url}, true);
        }
        // otherwise, assume that home server is a mysql server and just write there
        else{
          registerLFNs(this.getFileCatalogServer(homeServer), new String [] {vuid},
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
           " is registered with "+homeServer+" in DQ2");
        DQ2Locations [] locations = getLocations("'"+vuid+"'");
        boolean siteRegistered = false;
        String [] incomplete = locations[0].getIncomplete();
        String [] complete = locations[0].getComplete();
        for(int i=0; i<incomplete.length; ++i){
          if(incomplete[i].equalsIgnoreCase(homeServer)){
            siteRegistered = true;
          }
        }
        for(int i=0; i<complete.length; ++i){
          if(complete[i].equalsIgnoreCase(homeServer)){
            siteRegistered = true;
          }
        }
        if(!siteRegistered){
          GridPilot.getClassMgr().getStatusBar().setLabel("Registering new lfn " +lfn+
          " with "+homeServer+" in DQ2");
          dq2Access.registerLocation(vuid, dsn, datasetComplete, homeServer);
        }
        else{
          GridPilot.getClassMgr().getStatusBar().setLabel("Yes!");
        }
      }
      catch(Exception e){
        error = "WARNING: could not update dataset "+dsn+" in DQ2 "+
           ". Registration of "+homeServer+" in DQ2 NOT done";
        logFile.addMessage(error, e);
      }
    }
  }

  public boolean createDataset(String table, String[] fields,
      Object[] values){
    
    String dsn = null;
    String vuid = null;
    String [] valueStrings = new String [values.length];
    
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
          dsn = values[i].toString();
        }
        valueStrings[i] = values[i].toString();
      }
      vuid = dq2Access.createDataset(dsn);
    }
    catch(Exception e){
      error = "ERROR: could not connect to DQ2 dataset at "+dq2Server+" on port "+dq2SecurePort+
      " and with path "+dq2Path;
      logFile.addMessage(error, e);
      return false;
    }

    return updateDataset(vuid, fields, valueStrings);
  }

  /**
   * Update fields: "dsn", "vuid", "incomplete", "complete", or a subset of these.
   * Notice: DQ2 site or file registrations are not touched. Neither are
   *         file catalog entries or physical files.
   */
  public boolean updateDataset(String vuid, String[] fields, String[] values){
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
      dsn = getDatasetName(vuid);
      if(dsn!=null && !dsn.equals("")){
        exists = true;
      }
    }
    catch(Exception e){
      exists =false;
    }
    
    // If the dataset does not exist, abort
    if(!exists){
      error = "ERROR: dataset "+dsn+" does not exist, cannot update.";
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
      // vuid
      if(fields[i].equalsIgnoreCase("vuid")){
        error = "WARNING: cannot change vuid";
        GridPilot.getClassMgr().getStatusBar().setLabel(error);
        logFile.addMessage(error+" "+vuid);
        return false;
      }
      // dsn
      else if(fields[i].equalsIgnoreCase("dsn")){
        error = "WARNING: cannot change dsn";
        GridPilot.getClassMgr().getStatusBar().setLabel(error);
        logFile.addMessage(error+" "+dsn);
        return false;
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
            // Delete all locations
            for(int j=0; j<dqLocations[0].getComplete().length; ++j){
              dq2Access.deleteFromSite(vuid, dqLocations[0].getComplete()[j]);
            }
            for(int j=0; j<dqLocations[0].getComplete().length; ++j){
              dq2Access.deleteFromSite(vuid, dqLocations[0].getComplete()[j]);
            }
            // Set the new ones
            if(fields[i].equalsIgnoreCase("complete")){
              String [] newLocations = Util.split(values[i]);
              for(int j=0; j<newLocations.length; ++j){
                dq2Access.registerLocation(vuid, dsn, true, values[i]);
              }
            }
            else if(fields[i].equalsIgnoreCase("incomplete")){
              String [] newLocations = Util.split(values[i]);
              for(int j=0; j<newLocations.length; ++j){
                dq2Access.registerLocation(vuid, dsn, false, values[i]);
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
      error = "ERROR: dataset "+dsn+" does not exist, cannot update.";
      logFile.addMessage(error);
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
    
    // Now, delete the dataset
    try{
      dq2Access.deleteDataset(dsn);
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

  // -------------------------------------------------------------------
  // What's below here is not relevant for this plugin
  // -------------------------------------------------------------------


  public String getDatasetTransformationName(String datasetID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getDatasetTransformationVersion(String datasetID){
    // TODO Auto-generated method stub
    return null;
  }
  public String[] getRuntimeEnvironments(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public DBResult getRuntimeEnvironments(){
    // TODO Auto-generated method stub
    return null;
  }

  public String getRuntimeEnvironmentID(String name, String cs){
    // TODO Auto-generated method stub
    return "-1";
  }

  public DBRecord getRuntimeEnvironment(String runtimeEnvironmentID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getRuntimeInitText(String pack, String cluster){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean createRuntimeEnvironment(Object[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateRuntimeEnvironment(String runtimeEnvironmentID,
      String[] fields, String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean deleteRuntimeEnvironment(String runtimeEnvironmentID){
    // TODO Auto-generated method stub
    return false;
  }

  public DBResult getTransformations(){
    // TODO Auto-generated method stub
    return null;
  }

  public DBRecord getTransformation(String transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getTransformationID(String transName, String transVersion){
    // TODO Auto-generated method stub
    return "-1";
  }

  public boolean createTransformation(Object[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateTransformation(String transformatinID, String[] fields,
      String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean deleteTransformation(String transformationID){
    // TODO Auto-generated method stub
    return false;
  }

  public String[] getVersions(String transformationName){
    // TODO Auto-generated method stub
    return null;
  }

  public String getTransformationRuntimeEnvironment(String transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getTransformationJobParameters(String transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getTransformationOutputs(String transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getTransformationInputs(String transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public DBResult getJobDefinitions(String datasetID, String[] fieldNames){
    // TODO
    return null;
  }

  public DBRecord getJobDefinition(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean createJobDefinition(String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean createJobDefinition(String datasetName, String[] cstAttrNames,
      String[] resCstAttr, String[] trpars, String[][] ofmap, String odest,
      String edest){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean deleteJobDefinition(String jobDefID){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateJobDefinition(String jobDefID, String[] fields,
      String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateJobDefinition(String jobDefID, String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public String getJobDefStatus(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getJobDefUserInfo(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getJobDefName(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getJobDefDatasetID(String jobDefID){
    // TODO Auto-generated method stub
    return "-1";
  }

  public String getJobDefTransformationID(String jobDefID){
    // TODO Auto-generated method stub
    return "-1";
  }

  public String getTransformationScript(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getRunInfo(String jobDefID, String key){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean cleanRunInfo(String jobDefID){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean reserveJobDefinition(String jobDefID, String UserName, String cs){
    // TODO Auto-generated method stub
    return false;
  }

  public String[] getOutputFiles(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getJobDefInputFiles(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getJobDefOutRemoteName(String jobDefID, String par){
    // TODO Auto-generated method stub
    return null;
  }

  public String getJobDefOutLocalName(String jobDefID, String par){
    // TODO Auto-generated method stub
    return null;
  }

  public String getStdOutFinalDest(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getStdErrFinalDest(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getTransformationArguments(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getJobDefTransPars(String jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean setJobDefsField(String [] identifiers, String field, String value){
    // TODO Auto-generated method stub
    return false;
  }

  public String getError(){
    // TODO Auto-generated method stub
    return error;
  }

}
