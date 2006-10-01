package gridpilot.dbplugins.atlas;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gridpilot.DBRecord;
import gridpilot.DBResult;
import gridpilot.Database;
import gridpilot.Debug;
import gridpilot.Util;

public class ATLASDatabase implements Database{
  
  private String error;
  private String dq2Url;
  private String [] lfcServers;

  public ATLASDatabase(String _dbName, String _dq2Url, String _lfcServers){
    dq2Url = _dq2Url;
    lfcServers = Util.split(_lfcServers);
    error = "";
  }

  public String connect(){
    return null;
  }

  public void disconnect(){
  }

  public void clearCaches(){
  }

  public DBResult select(String selectRequest, String identifier){
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
      patt = Pattern.compile(", "+identifier+" ", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll(" ");
      patt = Pattern.compile("SELECT "+identifier+" FROM", Pattern.CASE_INSENSITIVE);
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
    Debug.debug("fields: "+Util.arrayToString(fields), 3);
    
    // TODO: disallow expensive wildcard searches
    // When searching for nothing, for this database we return
    // nothing. In other cases we return a complete wildcard search result.
    if(req.matches("SELECT (.+) FROM (\\S+)")){
      error = "ERROR: this is a too expensive search pattern; "+req;
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
      // "dsn", "vuid"
      
      String name = "";
      String vuid = "";

      // Construct get string
      if(conditions.matches("(?i)dsn = (\\S+)")){
        name = conditions.replaceFirst("(?i)dsn = (\\S+)", "$1");
        if(name.indexOf("*")>-1){
          name = "";
        }
      }
      if(conditions.matches("(?i)vuid = (\\S+)")){
        vuid = conditions.replaceFirst("(?i)vuid = (\\S+)", "$1");
        if(vuid.indexOf("*")>-1){
          vuid = "";
        }
      }
      get = conditions.replaceAll("(?i)dsn = (\\S+)", "dsn=$1");
      get = get.replaceAll("(?i)vuid = (\\S+)", "vuid=$1");
      
      get = get.replaceAll("(?i)dsn CONTAINS (\\S+)", "dsn=*$1*");
      get = get.replaceAll("(?i)vuid CONTAINS (\\S+)", "vuid=*$1*");
      
      get = get.replaceAll(" AND ", "&");
      get = dq2Url+"ws_repository/dataset?version=0&"+get;

      Debug.debug(">>> get string was : "+get, 3);
      
      // TODO: catch searches on all other keys than dsn and
      // redirect to LFC catalogs
      
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
          name = record[0].replaceAll("'", "");
          //duid = record[0].replaceFirst("'duid': '(.*)'", "$1");
          vuidsString = record[1].replaceFirst("\\[(.*)\\], .*", "$1");
          vuids = Util.split(vuidsString, ", ");
          Debug.debug("Found "+vuids.length+" vuids: "+vuidsString, 3);
          recordVector = new Vector();
          exCheck = true;
          for(int j=0; j<vuids.length; ++j){
            exCheck = true;
            for(int k=0; k<fields.length; ++k){
              if(fields[k].equalsIgnoreCase("dsn")){
                recordVector.add(name);
              }
              else if(fields[k].equalsIgnoreCase("vuid")){
                vuids[j] = vuids[j].replace("'", "");
                Debug.debug("Adding vuid: "+vuids[j], 3);
                recordVector.add(vuids[j]);
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
        else{
          // Otherwise it should be the result of a vuid=... request and
          // this should work:
          if(vuid!=null && vuid.length()>0){
            record = Util.split(records[i], "'dsn': ");
            name = record[1].replaceFirst(", 'version': \\d+\\}", "");
            name = name.replaceAll("'", "");
            for(int k=0; k<fields.length; ++k){
              if(fields[k].equalsIgnoreCase("dsn")){
                recordVector.add(name);
              }
              else if(fields[k].equalsIgnoreCase("vuid")){
                Debug.debug("Adding vuid: "+vuid, 3);
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
      // "datasetName", "name", "pfns", "bytes", "lastModified", "guid"
      String dsn = "";
      String lfn = "";
      String pfns = "";
      String guid = "";      
      String vuid = "-1";

      // Construct get string
      if(conditions.matches(".*(?i)dsn = (\\S+).*")){
        dsn = conditions.replaceFirst("(?i)dsn = (\\S+)", "$1");
        if(dsn.indexOf("*")>-1){
          dsn = "";
        }
      }
      if(conditions.matches("(?i)lfn = (\\S+)")){
        lfn = conditions.replaceFirst("(?i)lfn = (\\S+)", "$1");
        if(lfn.indexOf("*")>-1){
          lfn = "";
        }
      }
      if(conditions.matches("(?i)pfns = (\\S+)")){
        pfns = conditions.replaceFirst("(?i)pfns = (\\S+)", "$1");
        if(pfns.indexOf("*")>-1){
          pfns = "";
        }
      }
      if(conditions.matches("(?i)guid = (\\S+)")){
        guid = conditions.replaceFirst("(?i)guid = (\\S+)", "$1");
        if(guid.indexOf("*")>-1){
          guid = "";
        }
      }
      
      if(dsn!=null && !dsn.equals("")){
        Debug.debug("dataset name: "+dsn, 3);
        vuid = getDatasetID(dsn);
        Debug.debug("dataset id: "+vuid, 3);
      }
      
      get = conditions.replaceAll("(?i)dsn = (\\S+)", "vuid="+vuid);
      get = get.replaceAll("(?i)lfn = (\\S+)", "lfn=$1");
      get = get.replaceAll("(?i)pfns = (\\S+)", "pfns=$1");
      get = get.replaceAll("(?i)guid = (\\S+)", "guid=$1");
      
      get = get.replaceAll(" AND ", "&");
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
      for(int i=0; i<records.length; ++i){      
        Vector recordVector = new Vector();
        boolean exCheck = true;
        record = Util.split(records[i], ": ");
        
        if(record!=null && record.length>1){
          // If the string is the result of a vuid=... request, the
          // split went ok and this should work:
          guid = record[0].replaceAll("'", "");
          lfn = record[1].replaceAll("'", "");
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
    
  public String getDatasetName(String datasetID){
    DBRecord dataset = getDataset(datasetID);
    return dataset.getValue("dsn").toString();
  }

  public String getDatasetID(String datasetName){
    DBResult res = select("SELECT * FROM dataset WHERE dsn = "+datasetName,
    "vuid");
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
        "vuid");
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
        return new String [] {"dsn", "lfn", "pfns", "bytes", "created", "lastModified", "guid"};
      }
      else if(table.equalsIgnoreCase("dataset")){
        return new String [] {"dsn", "vuid"};
      }
    }
    catch(Exception e){
      e.printStackTrace();
      Debug.debug(e.getMessage(),1);
    }
    return null;
  }
  
  public DBRecord getFile(String fileID){
    DBResult res = select("SELECT * FROM file WHERE guid = "+fileID,
        "guid");
    if(res.values.length>1){
      Debug.debug("WARNING: inconsistent dataset catalog; " +
          res.values.length + " entries with guid "+fileID, 1);
    }
    return res.getRow(0);
  }

  public String [] getFileURLs(String fileID){
    String [] ret = null;
    try{
      DBRecord file = getFile(fileID);
      ret = Util.split(file.getValue("pfns").toString());
    }
    catch(Exception e){
      Debug.debug("WARNING: could not get URLs. "+e.getMessage(), 1);
    }
    return ret;
  }

  public void registerFileLocation(String fileID, String url){
    // TODO Auto-generated method stub
  }

  public String getRunNumber(String datasetID){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean createDataset(String targetTable, String[] fields,
      Object[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateDataset(String datasetID, String[] fields, String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean deleteDataset(String datasetID, boolean cleanup){
    // TODO Auto-generated method stub
    return false;
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

  public String[] getOutputMapping(String jobDefID){
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

  public DBResult getFiles(String datasetID){
    // TODO
    return null;
  }

  public String getError(){
    // TODO Auto-generated method stub
    return error;
  }

}
