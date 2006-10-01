package gridpilot.dbplugins.hsqldb;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.sql.ResultSet;
import java.sql.Statement;

import org.hsqldb.Server;

import gridpilot.ConfigFile;
import gridpilot.Database;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.Util;
import gridpilot.DBResult;
import gridpilot.DBRecord;

public class HSQLDBDatabase implements Database{
  
  private String driver = "";
  private String database = "";
  private String user = "";
  private String passwd = "";
  private Connection conn = null;
  private String error = "";
  private Server server = null;
  private String [] transformationFields = null;
  private String [] jobDefFields = null;
  private String [] datasetFields = null;
  private String [] runtimeEnvironmentFields = null;
  private HashMap datasetFieldTypes = new HashMap();
  private String databaseName = null;
  private String dbName = null;
  private ConfigFile configFile = null;
  
  private static String SERVER_RUNNING = "no";

  public HSQLDBDatabase(String _dbName,
      String _driver, String _database, String _user, String _passwd){
    driver = _driver;
    database = _database;
    user = _user;
    passwd = _passwd;
    dbName = _dbName;
    
    configFile = GridPilot.getClassMgr().getConfigFile();

    
    boolean showDialog = true;
    // if csNames is set, this is a reload
    if(GridPilot.csNames==null){
      showDialog = false;
    }
        
    String [] up = null;
    
    for(int rep=0; rep<3; ++rep){
      if(showDialog ||
          user==null || passwd==null || database==null){
        up = GridPilot.userPwd(user, passwd, database);
        if(up==null){
          return;
        }
        else{
          user = up[0];
          passwd = up[1];
          database = up[2];
        }
      }
      if(connect()!=null){
        break;
      }
      Debug.debug("WARNING: connection to HSQLDB failed, retrying.", 1);
      try{
        Thread.sleep(5000);
      }
      catch(Exception e){
      }
    }
    try{
      setFieldNames();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    if(datasetFields==null || datasetFields.length<1){
      makeTable("dataset");
    }
    if(jobDefFields==null || jobDefFields.length<1){
      makeTable("jobDefinition");
    }
    if(transformationFields==null || transformationFields.length<1){
      makeTable("transformation");
    }
    if(runtimeEnvironmentFields==null || runtimeEnvironmentFields.length<1){
      makeTable("runtimeEnvironment");
    }
    setFieldNames();
  }
  
  public String connect(){
    try{
      Class.forName(driver).newInstance();
    }
    catch(Exception e){
      Debug.debug("Could not load the driver "+driver, 3);
      e.printStackTrace();
      return null;
    }
    try{
      if(database.startsWith("file:")){
        // In-memory database
        conn = DriverManager.getConnection("jdbc:hsqldb:"+database,
           user, passwd);
      }
      else if(database.startsWith("hsql:")){
        synchronized(SERVER_RUNNING){
          // Database server
          if(SERVER_RUNNING.equalsIgnoreCase("no") &&
              database.startsWith("hsql://localhost/")){
            // If there is not a server running and server
            // should run on localhost, start one
            Debug.debug("Starting local database server. "+
                SERVER_RUNNING, 3);
            server = new Server();
            String dbPath = database.substring(16);
            server.setDatabasePath(0, dbPath);
            databaseName = database.substring(database.lastIndexOf("/")+1);
            server.setDatabaseName(0, databaseName);
            Debug.debug("Starting database server, "+databaseName+":"+
                dbPath, 1);
            server.start();
            database = "hsql://localhost/"+databaseName;
            SERVER_RUNNING = "yes";
          }
          conn = DriverManager.getConnection("jdbc:hsqldb:"+database,
              user, passwd);
        }
      }
    }
    catch(Exception e){
      Debug.debug("Could not connect to db "+database+
          ", "+user+", "+passwd+" : "+e, 3);
      e.printStackTrace();
      return null;
    }  
    try{
      conn.setAutoCommit(true);
    }
    catch(Exception e){
      Debug.debug("failed setting auto commit to true: "+e.getMessage(), 2);
    }
    return "";
  }
  
  private void setFieldNames(){
    //ConfigFile tablesConfig = new ConfigFile("gridpilot/dbplugins/hsqldb/tables.conf");
    configFile = GridPilot.getClassMgr().getConfigFile();
    datasetFields = getFieldNames("dataset");
    //String [] dsFieldTypes = Util.split(tablesConfig.getValue("tables", "dataset field types"), ",");
    String [] dsFieldTypes = Util.split(configFile.getValue(dbName, "dataset field types"), ",");
    for(int i=0; i<datasetFields.length; ++i){
      datasetFieldTypes.put(datasetFields[i], dsFieldTypes[i]);
    }
    jobDefFields = getFieldNames("jobDefinition");
    transformationFields = getFieldNames("transformation");
    // only used for checking
    runtimeEnvironmentFields = getFieldNames("runtimeEnvironment");
  }

  private boolean makeTable(String table){
    Debug.debug("Creating table "+table, 3);
    //ConfigFile tablesConfig = new ConfigFile("gridpilot/dbplugins/hsqldb/tables.conf");
    String [] fields = Util.split(configFile.getValue(dbName, table+" field names"), ",");
    String [] fieldTypes = Util.split(configFile.getValue(dbName, table+" field types"), ",");
    String sql = "CREATE TABLE "+table+"(";
    for(int i=0; i<fields.length; ++i){
      if(i>0){
        sql += ", ";
      }
      //Debug.debug("-->"+fields[i], 3);
      sql += fields[i];
      //Debug.debug("-->"+fieldTypes[i], 3);
      sql += " "+fieldTypes[i];
    }
    sql += ")";
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Debug.debug("Creating table. "+sql, 1);
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      e.printStackTrace();
      error = e.getMessage();
    }
    String sql1 = "";
    if(table.equalsIgnoreCase("dataset")){
      sql1 = "ALTER TABLE "+table+" ADD UNIQUE (name)";
      try{
        Statement stmt = conn.createStatement();
        Debug.debug("Altering table. "+sql1, 1);
        stmt = conn.createStatement();
        stmt.executeUpdate(sql1);
      }
      catch(Exception e){
        execok = false;
        Debug.debug(e.getMessage(), 2);
        e.printStackTrace();
        error = e.getMessage();
      }
    }
    return execok;
  }
    
  public synchronized void clearCaches(){
    // nothing for now
  }

  public synchronized boolean cleanRunInfo(String jobDefID){
    String sql = "UPDATE jobDefinition SET jobID = ''," +
        "outTmp = '', validationResult = '' " +
        "WHERE identifier = '"+
    jobDefID+"'";
    boolean ok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    }
    catch(Exception e){
      error = e.getMessage();
      ok = false;
    }
    return ok;
  }

  public void disconnect(){
    try{
      if(server!=null){
        try{
          Statement stmt = conn.createStatement();
          stmt.executeUpdate("SHUTDOWN");
        }
        catch(Exception e){
          Debug.debug("Shutting down server failed. "+
              e.getCause().toString()+"\n"+e.getMessage(),1);
        }
        conn.close();
        server.stop();
      }
      else{
        try{
          Statement stmt = conn.createStatement();
          stmt.executeUpdate("SHUTDOWN COMPACT");
        }
        catch(Exception e){
          Debug.debug("Compacting database failed. "+
              e.getCause().toString()+"\n"+e.getMessage(),1);
        }
        conn.close();
      }
    }
    catch(SQLException e){
      Debug.debug("Closing connection failed. "+
          e.getCause().toString()+"\n"+e.getMessage(),1);
    }
  }

  public synchronized String [] getFieldNames(String table){
    try{
      Debug.debug("getFieldNames for table "+table, 3);
      if(table.equalsIgnoreCase("file")){
        return new String [] {"datasetName", "name", "url"};
      }
      Statement stmt = conn.createStatement();
      // TODO: Do we need to execute a query to get the metadata?
      ResultSet rset = stmt.executeQuery("SELECT LIMIT 0 1 * FROM "+table);
      ResultSetMetaData md = rset.getMetaData();
      String [] res = new String[md.getColumnCount()];
      for(int i=1; i<=md.getColumnCount(); ++i){
        res[i-1] = md.getColumnName(i);
      }
      Debug.debug("found "+Util.arrayToString(res), 3);
      return res;
    }
    catch(Exception e){
      e.printStackTrace();
      Debug.debug(e.getMessage(),1);
      return null;
    }
  }

  public synchronized String getTransformationID(String transName, String transVersion){
    String req = "SELECT identifier from transformation where name = '"+transName + "'"+
    " AND version = '"+transVersion+"'";
    String id = null;
    Vector vec = new Vector();
    try{
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      while(rset.next()){
        id = rset.getString("identifier");
        if(id!=null){
          Debug.debug("Adding id "+id, 3);
          vec.add(id);
        }
        else{
          Debug.debug("WARNING: identifier null for name "+
              transName, 1);
        }
      }
      rset.close();  
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
      error = e.getMessage();
      return "-1";
    }
    if(vec.size()>1){
      Debug.debug("WARNING: More than one ("+vec.size()+
          ") transformation found with name:version "+transName+":"+transVersion, 1);
    }
    if(vec.size()==0){
      return "-1";
    }
    else{
      return vec.get(0).toString();
    }
  }

  public synchronized String getRuntimeEnvironmentID(String name, String cs){
    String req = "SELECT identifier from runtimeEnvironment where name = '"+name + "'"+
    " AND computingSystem = '"+cs+"'";
    String id = null;
    Vector vec = new Vector();
    try{
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      while(rset.next()){
        id = rset.getString("identifier");
        if(id!=null){
          Debug.debug("Adding id "+id, 3);
          vec.add(id);
        }
        else{
          Debug.debug("WARNING: identifier null for name "+
              name, 1);
        }
      }
      rset.close();  
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
      error = e.getMessage();
      return "-1";
    }
    if(vec.size()>1){
      Debug.debug("WARNING: More than one ("+vec.size()+
          ") runtimeEnvironment found with name:cs "+name+":"+cs, 1);
    }
    if(vec.size()==0){
      return "-1";
    }
    else{
      return vec.get(0).toString();
    }
  }

  public String [] getTransformationJobParameters(String transformationID){
    String res = getTransformation(transformationID).getValue("arguments").toString(); 
    return Util.split(res);
  }

  public String [] getOutputMapping(String jobDefID){
    String outMap = getJobDefinition(jobDefID).getValue("outFileMapping").toString();
    return Util.split(outMap);
  }

  public String [] getJobDefInputFiles(String jobDefID){
    String inputs = getJobDefinition(jobDefID).getValue("inputFileNames").toString();
    return Util.split(inputs);
  }

  public String [] getJobDefTransPars(String jobDefID){
    String args = getJobDefinition(jobDefID).getValue("transPars").toString();
    return Util.split(args);
  }

  public String getJobDefOutLocalName(String jobDefID, String par){
    String transID = getJobDefTransformationID(jobDefID);
    String [] fouts = Util.split(getTransformation(transID).getValue("outputFiles").toString());
    String maps = getJobDefinition(jobDefID).getValue("outFileMapping").toString();
    String[] map = Util.split(maps);
    String name = "";
    for(int i=0; i<fouts.length; i++){
      if(par.equals(fouts[i])){
        name = map[i*2];
      }
    }
    return name;
  }

  public String getJobDefOutRemoteName(String jobDefID, String par){
    String transID = getJobDefTransformationID(jobDefID);
    String [] fouts = Util.split(getTransformation(transID).getValue("outputFiles").toString());
    String maps = getJobDefinition(jobDefID).getValue("outFileMapping").toString();
    Debug.debug("output file mapping: "+maps+" : "+Util.arrayToString(fouts), 3);
    String[] map = Util.split(maps);
    String name = "";
    for(int i=0; i<fouts.length; i++){
      Debug.debug("checking: "+par+"<->"+fouts[i]+" : "+map[i*2+1], 3);
      if(par.equals(fouts[i])){
        name = map[i*2+1];
        break;
      }
    }
    Debug.debug("output file: "+name, 3);
    return name;
  }
  
  public String getStdOutFinalDest(String jobDefID){
    return getJobDefinition(jobDefID).getValue("stdoutDest").toString();
  }

  public String getStdErrFinalDest(String jobDefID){
    return getJobDefinition(jobDefID).getValue("stderrDest").toString();
  }

  public String getTransformationScript(String jobDefID){
    String transformationID = getJobDefTransformationID(jobDefID);
    String script = getTransformation(
        transformationID).getValue("script").toString();
    return script;
  }

  public String [] getRuntimeEnvironments(String jobDefID){
    String transformationID = getJobDefTransformationID(jobDefID);
    String rts = getTransformation(transformationID).getValue("runtimeEnvironmentName").toString();
    return Util.split(rts);
  }

  public String [] getTransformationArguments(String jobDefID){
    String transformationID = getJobDefTransformationID(jobDefID);
    String args =  getTransformation(transformationID).getValue("arguments").toString();
    return Util.split(args);
  }

  public String getTransformationRuntimeEnvironment(String transformationID){
    return getTransformation(transformationID).getValue("runtimeEnvironmentName").toString();
  }

  public String getJobDefUserInfo(String jobDefinitionID){
    Object userInfo = getJobDefinition(jobDefinitionID).getValue("userInfo");
    if(userInfo==null){
      return "";
    }
    else{
      return userInfo.toString();
    }
  }

  public String getJobDefStatus(String jobDefinitionID){
    return getJobDefinition(jobDefinitionID).getValue("status").toString();
  }

  public String getJobDefName(String jobDefinitionID){
    return getJobDefinition(jobDefinitionID).getValue("name").toString();
  }

  public String getJobDefDatasetID(String jobDefinitionID){
    String datasetName = getJobDefinition(jobDefinitionID).getValue("datasetName").toString();
    String datasetID = getDatasetID(datasetName);
    return getDataset(datasetID).getValue("identifier").toString();
  }

 public String getRuntimeInitText(String runTimeEnvironmentName, String csName){
    String initTxt = getRuntimeEnvironment(
         getRuntimeEnvironmentID(runTimeEnvironmentName, csName)
      ).getValue("initLines").toString();
    return initTxt;
  }

  public synchronized String getJobDefTransformationID(String jobDefinitionID){
    DBRecord dataset = getDataset(getJobDefDatasetID(jobDefinitionID));
    String transformation = dataset.getValue("transformationName").toString();
    String version = dataset.getValue("transformationVersion").toString();
    String transID = null;
    String req = "SELECT identifier FROM "+
    "transformation WHERE name = '"+transformation+"' AND version = '"+version+"'";
    Vector vec = new Vector();
    Debug.debug(req, 2);
    try{
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      while(rset.next()){
        if(transID!=null){
          Debug.debug("WARNING: more than one transformation for name, version :" +
              transformation+", "+version, 1);
          break;
        }
        transID = rset.getString("identifier");
        if(transID!=null){
          Debug.debug("Adding version "+transID, 3);
          vec.add(version);
        }
        else{
          Debug.debug("WARNING: identifier null", 1);
        }
      }
      rset.close();  
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
    }
    return transID;
  }

  public boolean reserveJobDefinition(String jobDefID, String userInfo, String cs){
    boolean ret = updateJobDefinition(
        jobDefID,
        new String [] {"status", "userInfo", "computingSystem"},
        new String [] {"Submitted", userInfo, cs}
        );
    clearCaches();
    return ret;
  }

  public synchronized DBResult select(String selectRequest, String identifier){
    
    String req = selectRequest;
    boolean withStar = false;
    int identifierColumn = -1;
    boolean fileTable = false;
    int urlColumn = -1;
    Pattern patt;
    Matcher matcher;

    // Make sure we have identifier as last column.
    // *, row1, row2 -> *
    if(selectRequest.matches("SELECT \\* FROM.*")){
      withStar = true;
    }
    else if(selectRequest.matches("SELECT \\*\\,.*")){
      withStar = true;
      Debug.debug("Correcting non-valid select pattern", 3);
      patt = Pattern.compile("SELECT \\*\\, (.+) FROM", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll("SELECT * FROM");
    }
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
    
    // The "file" table is a pseudo table constructed from "jobDefinitions".
    // We replace "url" with "outFileMapping" and parse the values of
    // outFileMapping later.
    if(req.matches("SELECT (.+) url\\, (.+) FROM file (.+)")){
      patt = Pattern.compile("SELECT (.+) url\\, (.+) FROM file (.+)", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll("SELECT $1 outFileMapping, $2 FROM file $3");
    }
    if(req.matches("SELECT (.+) url FROM file (.+)")){
      patt = Pattern.compile("SELECT (.+) url FROM file (.+)", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll("SELECT $1 outFileMapping FROM file $2");
    }
    if(req.matches("SELECT (.+) FROM file (.+)")){
      fileTable = true;
      patt = Pattern.compile("SELECT (.+) FROM file (.+)", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll("SELECT $1 FROM jobDefinition $2");
    }

    patt = Pattern.compile("CONTAINS (\\S+)", Pattern.CASE_INSENSITIVE);
    matcher = patt.matcher(req);
    req = matcher.replaceAll("LIKE '%$1%'");
    
    patt = Pattern.compile("([<>=]) (\\S+)", Pattern.CASE_INSENSITIVE);
    matcher = patt.matcher(req);
    req = matcher.replaceAll("$1 '$2'");
    
    Debug.debug(">>> sql string was : "+req, 3);
    
    try{
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      ResultSetMetaData md = rset.getMetaData();
      String[] fields = new String[md.getColumnCount()];
      //find out how many rows
      int i = 0;
      while(rset.next()){
        i++;
      }
      String [][] values = new String[i][md.getColumnCount()];
      for(int j=0; j<md.getColumnCount(); ++j){
        fields[j] = md.getColumnName(j+1);
        // If we did select *, make sure that the identifier
        // row is at the end as it should be
        if(withStar && fields[j].equalsIgnoreCase(identifier)  && 
            j!=md.getColumnCount()-1){
          identifierColumn = j;
        }
        // Find the outFileMapping column number
        if(fileTable && fields[j].equalsIgnoreCase("outFileMapping")  && 
            j!=md.getColumnCount()-1){
          urlColumn = j;
          // replace "outFileMapping" with "url" in the Table.
          fields[j] = "URL";
        }
      }
      if(withStar && identifierColumn>-1){
        fields[identifierColumn] = md.getColumnName(md.getColumnCount());
        fields[md.getColumnCount()-1] = identifier;
      }
      rset = stmt.executeQuery(req);
      i=0;
      while(rset.next()){
        for(int j=0; j<md.getColumnCount(); ++j){
          if(withStar && identifierColumn>-1){
            if(j==identifierColumn){
              // identifier column is not at the end, so we swap
              // identifier column and the last column
              String foo = rset.getString(md.getColumnCount());
              Debug.debug("values "+i+" "+foo, 2);
              values[i][j] = foo;
            }
            else if(j==md.getColumnCount()-1){
              String foo = rset.getString(identifierColumn+1);
              Debug.debug("values "+i+" "+foo, 2);
              values[i][j] = foo;
            }
            else{
              String foo =  rset.getString(j+1);
              Debug.debug("values "+i+" "+foo, 2);
              values[i][j] = foo;
            }
          }
          else if(fileTable && urlColumn>-1 && j==urlColumn){
            // The first output file specified in outFileMapping
            // is be convention *the* output file.
            String [] foos = Util.split(rset.getString(j+1));
            String foo = "";
            if(foos.length>1){
              foo = foos[1];
            }
            else{
              Debug.debug("WARNING: no output file found!", 1);
            }
            Debug.debug("values "+i+" "+foo, 2);
            values[i][j] = foo;
          }
          else{
            String foo = rset.getString(j+1);
            Debug.debug("values "+i+" "+foo, 2);
            values[i][j] = foo;
          }
        }
        i++;
      }
      return new DBResult(fields, values);
    }
    catch(SQLException e){
      Debug.debug(e.getMessage(),1);
      return new DBResult();
    }
  }
  
  public synchronized DBRecord getDataset(String datasetID){
    
    DBRecord task = null;
    String req = "SELECT "+datasetFields[0];
    if(datasetFields.length>1){
      for(int i=1; i<datasetFields.length; ++i){
        req += ", "+datasetFields[i];
      }
    }
    req += " FROM dataset";
    req += " WHERE identifier = '"+ datasetID+"'";
    try{
      Debug.debug(">> "+req, 3);
      ResultSet rset = conn.createStatement().executeQuery(req);
      Vector taskVector = new Vector();
      while(rset.next()){
        String values[] = new String[datasetFields.length];
        for(int i=0; i<datasetFields.length;i++){
          if(datasetFields[i].endsWith("FK") || datasetFields[i].endsWith("ID") &&
              !datasetFields[i].equalsIgnoreCase("grid") ||
              datasetFields[i].endsWith("COUNT")){
            int tmp = rset.getInt(datasetFields[i]);
            values[i] = Integer.toString(tmp);
          }
          else{
            values[i] = rset.getString(datasetFields[i]);
          }
          //Debug.debug(datasetFields[i]+"-->"+values[i], 3);
        }
        DBRecord jobd = new DBRecord(datasetFields, values);
        taskVector.add(jobd);
      }
      rset.close();
      if(taskVector.size()==0){
        Debug.debug("ERROR: No dataset with id "+datasetID, 1);
      }
      else{
        task = ((DBRecord) taskVector.get(0));
      }
      if(taskVector.size()>1){
        Debug.debug("WARNING: More than one ("+rset.getRow()+") dataset found with id "+datasetID, 1);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No dataset found with id "+datasetID+". "+e.getMessage(), 1);
      return task;
    }
     return task;
  }
  
  public String getDatasetTransformationName(String datasetID){
    return getDataset(datasetID).getValue("transformationName").toString();
  }
  
  public String getDatasetTransformationVersion(String datasetID){
    return getDataset(datasetID).getValue("transformationVersion").toString();
  }

  public String getDatasetName(String datasetID){
    return getDataset(datasetID).getValue("name").toString();
  }

  public synchronized String getDatasetID(String datasetName){
    String req = "SELECT identifier from dataset where name = '"+datasetName + "'";
    String id = null;
    Vector vec = new Vector();
    try{
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      while(rset.next()){
        id = rset.getString("identifier");
        if(id!=null){
          Debug.debug("Adding id "+id, 3);
          vec.add(id);
        }
        else{
          Debug.debug("WARNING: identifier null for name "+
              datasetName, 1);
        }
      }
      rset.close();  
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
      error = e.getMessage();
      return "-1";
    }
    if(vec.size()>1){
      Debug.debug("WARNING: More than one ("+vec.size()+
          ") dataset found with name "+datasetName, 1);
    }
    if(vec.size()==0){
      return "-1";
    }
    else{
      return vec.get(0).toString();
    }
  }

  public String getRunNumber(String datasetID){
    return getDataset(datasetID).getValue("runNumber").toString();
  }

  public synchronized DBRecord getRuntimeEnvironment(String runtimeEnvironmentID){
    
    DBRecord pack = null;
    String req = "SELECT "+runtimeEnvironmentFields[0];
    if(runtimeEnvironmentFields.length>1){
      for(int i=1; i<runtimeEnvironmentFields.length; ++i){
        req += ", "+runtimeEnvironmentFields[i];
      }
    }
    req += " FROM runtimeEnvironment";
    req += " WHERE identifier = '"+ runtimeEnvironmentID+"'";
    try{
      Debug.debug(">> "+req, 3);
      ResultSet rset = conn.createStatement().executeQuery(req);
      Vector runtimeEnvironmentVector = new Vector();
      String [] jt = new String[runtimeEnvironmentFields.length];
      int i = 0;
      while(rset.next()){
        jt = new String[runtimeEnvironmentFields.length];
        for(int j=0; j<runtimeEnvironmentFields.length; ++j){
          try{
            jt[j] = rset.getString(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+rset.getString(j+1)+" in "+
                runtimeEnvironmentFields[j]+". "+e.getMessage(),1);
          }
        }
        Debug.debug("Adding value "+jt[0], 3);
        runtimeEnvironmentVector.add(new DBRecord(runtimeEnvironmentFields, jt));
        Debug.debug("Added value "+((DBRecord) runtimeEnvironmentVector.get(i)).getAt(0), 3);
        ++i;
      }
      if(i==0){
        Debug.debug("ERROR: No runtime environment found with id "+runtimeEnvironmentID, 1);
      }
      else{
        pack = ((DBRecord) runtimeEnvironmentVector.get(0));
      }
      if(i>1){
        Debug.debug("WARNING: More than one ("+rset.getRow()+") runtime environment found with id "+runtimeEnvironmentID, 1);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No runtime environment with id "+runtimeEnvironmentID+". "+e.getMessage(), 1);
    }
     return pack;
  }
  
  public synchronized DBRecord getTransformation(String transformationID){
    
    DBRecord transformation = null;
    String req = "SELECT "+transformationFields[0];
    if(transformationFields.length>1){
      for(int i=1; i<transformationFields.length; ++i){
        req += ", "+transformationFields[i];
      }
    }
    req += " FROM transformation";
    req += " WHERE identifier = '"+ transformationID+"'";
    try{
      Debug.debug(">> "+req, 3);
      ResultSet rset = conn.createStatement().executeQuery(req);
      Vector transformationVector = new Vector();
      String [] jt = new String[transformationFields.length];
      int i = 0;
      while(rset.next()){
        jt = new String[transformationFields.length];
        for(int j=0; j<transformationFields.length; ++j){
          try{
            jt[j] = rset.getString(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+rset.getString(j+1)+" in "+
                transformationFields[j]+". "+e.getMessage(),1);
          }
        }
        Debug.debug("Adding value "+jt[0], 3);
        transformationVector.add(new DBRecord(transformationFields, jt));
        Debug.debug("Added value "+((DBRecord) transformationVector.get(i)).getAt(0), 3);
        ++i;
      }
      if(i==0){
        Debug.debug("ERROR: No transformation found with id "+transformationID, 1);
      }
      else{
        transformation = ((DBRecord) transformationVector.get(0));
      }
      if(i>1){
        Debug.debug("WARNING: More than one ("+rset.getRow()+") transformation found with id "+transformationID, 1);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No transformation with id "+transformationID+". "+e.getMessage(), 1);
    }
     return transformation;
  }
  
  public String getRunInfo(String jobDefID, String key){
    DBRecord jobDef = getJobDefinition(jobDefID);
    Debug.debug("Getting value for key "+key, 3);
    return jobDef.getValue(key).toString();
  }

  /*
   * Find runtimeEnvironment records
   */
  private synchronized DBRecord [] getRuntimeEnvironmentRecords(){
    
    ResultSet rset = null;
    String req = "";
    DBRecord [] allRuntimeEnvironmentRecords = null;   
    try{      
      req = "SELECT "+runtimeEnvironmentFields[0];
      if(runtimeEnvironmentFields.length>1){
        for(int i=1; i<runtimeEnvironmentFields.length; ++i){
          req += ", "+runtimeEnvironmentFields[i];
        }
      }
      req += " FROM runtimeEnvironment";
      Debug.debug(req, 3);
      rset = conn.createStatement().executeQuery(req);
      Vector runtimeEnvironmentVector = new Vector();
      String [] jt = new String[runtimeEnvironmentFields.length];
      int i = 0;
      while(rset.next()){
        jt = new String[runtimeEnvironmentFields.length];
        for(int j=0; j<runtimeEnvironmentFields.length; ++j){
          try{
            jt[j] = rset.getString(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+rset.getString(j+1)+" in "+
                runtimeEnvironmentFields[j]+". "+e.getMessage(),1);
          }
        }
        Debug.debug("Adding value "+jt[0], 3);
        runtimeEnvironmentVector.add(new DBRecord(runtimeEnvironmentFields, jt));
        Debug.debug("Added value "+((DBRecord) runtimeEnvironmentVector.get(i)).getAt(0), 3);
        ++i;
      }
      allRuntimeEnvironmentRecords = new DBRecord[i];
      for(int j=0; j<i; ++j){
        allRuntimeEnvironmentRecords[j] = ((DBRecord) runtimeEnvironmentVector.get(j));
        Debug.debug("Added value "+allRuntimeEnvironmentRecords[j].getAt(0), 3);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No runtime environments found. "+e.getMessage(), 1);
    }
    return allRuntimeEnvironmentRecords;
  }

  /*
   * Find transformation records
   */
  private synchronized DBRecord [] getTransformationRecords(){
    
    ResultSet rset = null;
    String req = "";
    DBRecord [] allTransformationRecords = null;   
    try{      
      req = "SELECT "+transformationFields[0];
      if(transformationFields.length>1){
        for(int i=1; i<transformationFields.length; ++i){
          req += ", "+transformationFields[i];
        }
      }
      req += " FROM transformation";
      Debug.debug(req, 3);
      rset = conn.createStatement().executeQuery(req);
      Vector transformationVector = new Vector();
      String [] jt = new String[transformationFields.length];
      int i = 0;
      while(rset.next()){
        jt = new String[transformationFields.length];
        for(int j=0; j<transformationFields.length; ++j){
          try{
            jt[j] = rset.getString(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+rset.getString(j+1)+" in "+
                transformationFields[j]+". "+e.getMessage(),1);
          }
        }
        transformationVector.add(new DBRecord(transformationFields, jt));
        ++i;
      }
      allTransformationRecords = new DBRecord[i];
      for(int j=0; j<i; ++j){
        allTransformationRecords[j] = ((DBRecord) transformationVector.get(j));
        Debug.debug("Added values "+
            Util.arrayToString(allTransformationRecords[j].values, " : "), 3);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No transformations found. "+e.getMessage(), 1);
    }
    return allTransformationRecords;
  }

  // Selects only the fields listed in fieldNames. Other fields are set to "".
  public synchronized DBRecord getJobDefinition(String jobDefinitionID){
    
    String req = "SELECT *";
    req += " FROM jobDefinition where identifier = '"+
    jobDefinitionID + "'";
    Vector jobdefv = new Vector();
    Debug.debug(req, 2);
    try{
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      while(rset.next()){
        String values[] = new String[jobDefFields.length];
        for(int i=0; i<jobDefFields.length;i++){
          String fieldname = jobDefFields[i];
          String val = "";
          for(int j=0; j<jobDefFields.length; ++j){
            if(fieldname.equalsIgnoreCase(jobDefFields[j])){
              if((fieldname.endsWith("FK") || fieldname.endsWith("ID")) &&
                  !fieldname.equalsIgnoreCase("jobid")){
                int tmp = rset.getInt(fieldname);
                val = Integer.toString(tmp);
              }
              else{
                val = rset.getString(fieldname);
              }
              break;
            }
            val = "";
          }
          values[i] = val;
          //Debug.debug(fieldname+"-->"+val, 3);
        }
        DBRecord jobd = new DBRecord(jobDefFields, values);
        jobdefv.add(jobd);
      }
      rset.close();
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 2);
    }
    if(jobdefv.size()>1){
      Debug.debug("WARNING: More than one jobDefinition with jobDefinitionID "+
          jobDefinitionID, 1);
    }
    if(jobdefv.size()<1){
      Debug.debug("WARNING: No jobDefinition with jobDefinitionID "+
          jobDefinitionID, 1);
      return null;
    }
    DBRecord def = (DBRecord)jobdefv.get(0);
    jobdefv.removeAllElements();
    return def;
  }

  public DBRecord getFile(String jobDefinitionID){
    DBRecord jobDef = getJobDefinition(jobDefinitionID);
    String [] fields = getFieldNames("file");
    String [] values = new String[fields.length];
    DBRecord file = new DBRecord(fields, values);
    for(int i=0; i<fields.length; ++i){
      try{
        file.setValue(fields[i], jobDef.getValue(fields[i]).toString());
      }
      catch(Exception e){
        Debug.debug("WARNING: could not set field "+fields[i]+". "+e.getMessage(), 2);
      }
    }
    for(int i=0; i<jobDef.fields.length; ++i){
      if(jobDef.fields[i].equalsIgnoreCase("outputFileMapping")){
        String [] map = Util.split(jobDef.getValue(fields[i]).toString());
        try{
          file.setValue("url", map[1]);
        }
        catch(Exception e){
          Debug.debug("WARNING: could not set URL. "+e.getMessage(), 2);
        }
      }
    }
    return file;
  }

  // This is not really a file catalog: THE output file is
  // the first in the list of fn -> pfn mappings of outputFileMapping
  public String [] getFileURLs(String fileID){
    String ret = null;
    try{
      DBRecord file = getFile(fileID);
      ret = file.getValue("url").toString();
    }
    catch(Exception e){
      Debug.debug("WARNING: could not get URLs. "+e.getMessage(), 1);
    }
    return new String [] {ret};
  }

  // Selects only the fields listed in fieldNames. Other fields are set to "".
  public synchronized DBRecord [] selectJobDefinitions(String datasetID, String [] fieldNames){
    
    String req = "SELECT";
    for(int i=0; i<fieldNames.length; ++i){
      if(i>0){
        req += ",";
      }
      req += " "+fieldNames[i];
    }
    req += " FROM jobDefinition";
    if(!datasetID.equals("-1")){
      req += " where datasetName = '"+getDatasetName(datasetID) + "'";
    }
    Vector jobdefv = new Vector();
    Debug.debug(req, 2);
    try{
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      while(rset.next()){
        String values[] = new String[jobDefFields.length];
        for(int i=0; i<jobDefFields.length;i++){
          String fieldname = jobDefFields[i];
          String val = "";
          for(int j=0; j<fieldNames.length; ++j){
            if(fieldname.equalsIgnoreCase(fieldNames[j])){
              if(fieldname.endsWith("FK") || fieldname.endsWith("ID") &&
                  !fieldname.equalsIgnoreCase("jobID")){
                int tmp = rset.getInt(fieldname);
                val = Integer.toString(tmp);
              }
              else{
                val = rset.getString(fieldname);
              }
              break;
            }
            val = "";
          }
          values[i] = val;
          //Debug.debug(fieldname+"-->"+val, 2);
        }
        DBRecord jobd = new DBRecord(jobDefFields, values);
        jobdefv.add(jobd);
      
      };
      rset.close();
    
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 2);
    }
    DBRecord[] defs = new DBRecord[jobdefv.size()];
    for(int i=0; i<jobdefv.size(); i++) defs[i] = (DBRecord)jobdefv.get(i);
    jobdefv.removeAllElements();
    return defs;
  }

  public DBResult getRuntimeEnvironments(){
    DBRecord jt [] = getRuntimeEnvironmentRecords();
    DBResult res = new DBResult(runtimeEnvironmentFields.length, jt.length);
    res.fields = runtimeEnvironmentFields;
    for(int i=0; i<jt.length; ++i){
      for(int j=0; j<runtimeEnvironmentFields.length; ++j){
        res.values[i][j] = jt[i].values[j];
      }
    }
    return res;
  }
    
  public DBResult getTransformations(){
    DBRecord jt [] = getTransformationRecords();
    DBResult res = new DBResult(transformationFields.length, jt.length);
    res.fields = transformationFields;
    for(int i=0; i<jt.length; ++i){
      for(int j=0; j<transformationFields.length; ++j){
        res.values[i][j] = jt[i].values[j];
      }
    }
    return res;
  }
  
  public DBResult getJobDefinitions(String datasetID, String [] fieldNames){
    
    DBRecord jt [] = selectJobDefinitions(datasetID, fieldNames);
    DBResult res = new DBResult(fieldNames.length, jt.length);
    
    System.arraycopy(
        fieldNames, 0,
        res.fields, 0, fieldNames.length);
            
    for(int i=0; i<jt.length; ++i){
      for(int j=0; j<fieldNames.length;j++){
        for(int k=0; k<jobDefFields.length;k++){
          if(fieldNames[j].equalsIgnoreCase(jobDefFields[k])){
            try{
              if(jt[i].getValue(fieldNames[j])==null){
                res.values[i][j] = "";
              }
              else{
                res.values[i][j] = jt[i].getValue(fieldNames[j]).toString();
              }
            }
            catch(Throwable e){
              Debug.debug("Could not get value for "+i+" "+j+". "+e.getMessage(), 2);
              e.printStackTrace();
              res.values[i][j] = "";
            }
            break;
          }
        }
      }
    }
    
    return res;
  }
  
  public synchronized boolean createJobDefinition(String [] values){
    
    if(jobDefFields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          jobDefFields.length+"!="+values.length, 1);
      return false;
    }

    String sql = "INSERT INTO jobDefinition (";
    for(int i=1; i<jobDefFields.length; ++i){
      sql += jobDefFields[i];
      if(jobDefFields.length>2 && i<jobDefFields.length - 1){
        sql += ",";
      }
    }
    sql += ") VALUES (";
    for(int i = 1; i < jobDefFields.length; ++i){
      
      if(jobDefFields[i].equalsIgnoreCase("created")){
        try{
          values[i] = makeDate(values[i].toString());
        }
        catch(Exception e){
          values[i] = makeDate("");
        }
      }
      else if(jobDefFields[i].equalsIgnoreCase("lastModified")){
        values[i] = makeDate("");
      }
      else{
        values[i] = "'"+values[i]+"'";
      }
      
      sql += values[i];
      if(jobDefFields.length>1 && i<jobDefFields.length - 1){
        sql += ",";
      }
    }
    sql += ")";
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    return execok;
  }
  
  public synchronized boolean createJobDefinition(
      String datasetName,
      String [] cstAttrNames,
      String [] resCstAttr,
      String [] trpars,
      String [] [] ofmap,
      String odest,
      String edest){
    
    error = "";
    String ofmapstr = "" ;
    String trparsstr = "" ;
    //trparsstr = Util.webEncode(trpars);
    trparsstr = Util.arrayToString(trpars);
    for (int i=0 ; i<ofmap.length ; i++){  
      ofmapstr += ofmap[i] [0] + " " + ofmap[i] [1] + " ";
    }
    clearCaches();
    // Update DB with "request" and return success/failure
    // Fetch current date and time
    SimpleDateFormat dateFormat = new SimpleDateFormat(GridPilot.dateFormatString);
    dateFormat.setTimeZone(TimeZone.getDefault());
    String dateString = dateFormat.format(new Date());
    // NOTICE: there must be a field jobDefinition.status
    String arg = "INSERT INTO jobDefinition (datasetName, status, ";
    for(int i=0; i<cstAttrNames.length; ++i){
      arg += cstAttrNames[i]+", ";
    }
    arg += "transPars, outFileMapping, stdoutDest," +
            "stderrDest, created, lastModified";
    arg += ") values ('"+datasetName+"', 'Defined', ";
    for(int i=0; i<resCstAttr.length; ++i){
      arg += "'"+resCstAttr[i]+"', ";
    }

    arg += "'"
              +trparsstr+"', '"
              +ofmapstr+"', '"
              +odest+"', '"
              +edest+"', '"
              +dateString+"', '"
              +dateString+
          "')";
    if(datasetName!=null && !datasetName.equals("")){
      Debug.debug(arg, 3);
      boolean execok = true;
      try{
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(arg);
        conn.commit();
      }
      catch(Exception e){
        execok = false;
        Debug.debug(e.getMessage(), 2);
        error = e.getMessage();
      }
      return execok;
    }
    else{
      Debug.debug("ERROR: Could not get dataset of job definition", 1);
      return false;
    }
  }
  
  public synchronized boolean createDataset(String table,
      String [] fields, Object [] _values){
    Object [] values = new Object [_values.length];
    for(int i=0; i<values.length; ++i){
      values[i] = _values[i];
    }
    String nonMatchedStr = "";
    Vector nonMatchedFields = new Vector();
    boolean match = false;
    for(int i=1; i<fields.length; ++i){
      match = false;
      for(int j=1; j<datasetFields.length; ++j){
        if(fields[i].equalsIgnoreCase(datasetFields[j])){
          match = true;
          break;
        }
      }
      if(!match){
        nonMatchedFields.add(new Integer(i));
        if(i>0){
          nonMatchedStr += "\n";
        }
        nonMatchedStr += fields[i]+" : "+values[i];
      }
    }
    String sql = "INSERT INTO "+table+" (";
    for(int i=1; i<datasetFields.length; ++i){
      if(!nonMatchedFields.contains(new Integer(i))){
        sql += datasetFields[i];
        if(datasetFields.length>0 && i<datasetFields.length-1){
          sql += ",";
        }
      }
    }
    sql += ") VALUES (";
    Debug.debug("Checking fields. "+
        datasetFieldTypes.keySet().size()+"\n"+
        Util.arrayToString(datasetFieldTypes.keySet().toArray())+"\n"+
        Util.arrayToString(datasetFieldTypes.values().toArray())+"\n"+
        Util.arrayToString(datasetFields)+"\n"+
        Util.arrayToString(fields), 3);
    for(int i=1; i<datasetFields.length; ++i){
      if(!nonMatchedFields.contains(new Integer(i))){
        if(!nonMatchedStr.equals("") &&
            datasetFields[i].equalsIgnoreCase("comment")){
          values[i] = nonMatchedStr;
        }
        if(datasetFields[i].equalsIgnoreCase("created")){
          try{
            values[i] = makeDate(values[i].toString());
          }
          catch(Exception e){
            values[i] = makeDate("");
          }
        }
        else if(datasetFields[i].equalsIgnoreCase("lastModified")){
          values[i] = makeDate("");
        }
        else{
          values[i] = values[i].toString().replaceAll("\n","\\\\n");
          values[i] = "'"+values[i].toString()+"'";
        }
        // Set empty numeric fields to 0.
        // Empty fields should not be allowed in central dataset catalogs,
        // but here, locally, it should be ok.
        Debug.debug("Value of  "+datasetFields[i]+": "+values[i], 3);
        try{
          if(fields[i]!=null && values[i].toString().equals("''") && 
              (datasetFieldTypes.get(datasetFields[i]).toString().toLowerCase().startsWith("int") ||
               datasetFieldTypes.get(datasetFields[i]).toString().toLowerCase().startsWith("bigint") ||
               datasetFieldTypes.get(datasetFields[i]).toString().toLowerCase().startsWith("tinyint") ||
               datasetFieldTypes.get(datasetFields[i]).toString().toLowerCase().startsWith("float"))){
            Debug.debug("Fixing "+datasetFields[i]+":"+values[i]+
                " - "+datasetFieldTypes.get(datasetFields[i]).toString().toLowerCase(), 3);
            values[i] = "'0'";
          }
        }
        catch(Exception e){
          e.printStackTrace();
        }
        sql += values[i].toString();
        if(datasetFields.length>0 && i<datasetFields.length-1){
          sql += ",";
        }
      }
    }
    sql += ")";
    // When pasting to non-mathing schema, there will be these
    // if  there are multiple non-matching fields
    sql = sql.replaceFirst(",\\) VALUES ", ") VALUES ");
    sql = sql.replaceFirst(",\\)$", ")");
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    return execok;
  }

  public synchronized boolean createTransformation(Object [] values){

    String sql = "INSERT INTO transformation (";
    for(int i=1; i<transformationFields.length; ++i){
      sql += transformationFields[i];
      if(transformationFields.length>2 && i<transformationFields.length - 1){
        sql += ",";
      }
    }
    sql += ") VALUES (";
    for(int i=1; i<transformationFields.length; ++i){
      if(transformationFields[i].equalsIgnoreCase("created")){
        try{
          values[i] = makeDate(values[i].toString());
        }
        catch(Exception e){
          values[i] = makeDate("");
        }
      }
      else if(transformationFields[i].equalsIgnoreCase("lastModified")){
        values[i] = makeDate("");
      }
      else{
        values[i] = "'"+values[i].toString()+"'";
      }

      sql += values[i].toString();
      if(transformationFields.length>1 && i<transformationFields.length - 1){
        sql += ",";
      }
    }
    sql += ")";
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    return execok;
  }

  public synchronized boolean createRuntimeEnvironment(Object [] values){
  
    String sql = "INSERT INTO runtimeEnvironment (";
    for(int i=1; i<runtimeEnvironmentFields.length; ++i){
      sql += runtimeEnvironmentFields[i];
      if(runtimeEnvironmentFields.length>2 && i<runtimeEnvironmentFields.length - 1){
        sql += ",";
      }
    }
    sql += ") VALUES (";
    for(int i=1; i<runtimeEnvironmentFields.length; ++i){
      if(runtimeEnvironmentFields[i].equalsIgnoreCase("created")){
        try{
          values[i] = makeDate(values[i].toString());
        }
        catch(Exception e){
          values[i] = makeDate("");
        }
      }
      else if(runtimeEnvironmentFields[i].equalsIgnoreCase("lastModified")){
        values[i] = makeDate("");
      }
      else{
        values[i] = "'"+values[i].toString()+"'";
      }
  
      sql += values[i].toString();
      if(runtimeEnvironmentFields.length>1 && i<runtimeEnvironmentFields.length - 1){
        sql += ",";
      }
    }
    sql += ")";
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    return execok;
  }
  
  public synchronized boolean setJobDefsField(String [] identifiers,
      String field, String value){
    String sql = "UPDATE jobDefinition SET ";
    sql += field+"='"+value+"' WHERE ";
    // Not very elegant, but we need to use Identifier instead of
    // identifier, because identifier will only have been set if
    // a JobDefinition object has already been made, which may not
    // be the case.
    for(int i=0; i<identifiers.length; ++i){
      sql += "identifier"+"="+identifiers[i];
      if(identifiers.length > 1 && i < identifiers.length - 1){
        sql += " OR ";
      }
    }
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    Debug.debug("update exec: "+execok, 2);
    return execok;
  }
  
  public boolean updateJobDefinition(String jobDefID,
      String [] values){
    return updateJobDefinition(
        jobDefID,
        new String [] {"userInfo", "jobID", "name", "outTmp", "errTmp"},
        values
    );
  }
  
  public synchronized boolean updateJobDefinition(String jobDefID, String [] fields,
      String [] values){
    
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      error = "The number of fields and values do not agree, "+
         fields.length+"!="+values.length;
      return false;
    }
    if(fields.length>jobDefFields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+jobDefFields.length, 1);
      error = "The number of fields is too large, "+
         fields.length+">"+jobDefFields.length;
    }

    String sql = "UPDATE jobDefinition  SET ";
    int addedFields = 0;
    for(int i=0; i<jobDefFields.length; ++i){
      if(!jobDefFields[i].equalsIgnoreCase("identifier")){
        for(int j=0; j<fields.length; ++j){
          // only add if present in jobDefFields
          if(jobDefFields[i].equalsIgnoreCase(fields[j])){
            if(jobDefFields[i].equalsIgnoreCase("created")){
              try{
                values[i] = makeDate(values[i].toString());
              }
              catch(Exception e){
                values[i] = makeDate("");
              }
            }
            else if(jobDefFields[i].equalsIgnoreCase("lastModified")){
              values[i] = makeDate("");
            }
            else if((jobDefFields[i].equalsIgnoreCase("outputFileSize") ||
                jobDefFields[i].equalsIgnoreCase("CPU")) &&
                (values[j]==null || values[j].equals(""))){
              values[j] = "'0'";
            }
            else{
              values[j] = "'"+values[j]+"'";
            }            
            if(addedFields>0){
              sql += ",";
            }
            sql += fields[j];
            sql += "=";
            sql += values[j];
            ++addedFields;
            break;
          }
        }
      }
    }
    sql += " WHERE identifier="+jobDefID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    Debug.debug("update exec: "+execok, 2);
    return execok;
  }
  
  public synchronized boolean updateDataset(String datasetID, String [] fields,
      String [] values){

    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      error = "The number of fields and values do not agree, "+
         fields.length+"!="+values.length;
      return false;
    }
    if(fields.length>datasetFields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+datasetFields.length, 1);
    }

    String sql = "UPDATE dataset SET ";
    int addedFields = 0;
    for(int i=0; i < datasetFields.length; ++i){
      if(!datasetFields[i].equalsIgnoreCase("identifier")){
        for(int j=0; j<fields.length; ++j){
          // only add if present in datasetFields
          if(datasetFields[i].equalsIgnoreCase(fields[j])){
            if(datasetFields[i].equalsIgnoreCase("created")){
              try{
                values[i] = makeDate(values[i].toString());
              }
              catch(Exception e){
                values[i] = makeDate("");
              }
            }
            else if(datasetFields[i].equalsIgnoreCase("lastModified")){
              values[i] = makeDate("");
            }
            else{
              values[j] = "'"+values[j]+"'";
            }
            
            sql += fields[j];
            sql += "=";
            sql += values[j];
            ++addedFields;
            break;
          }
        }
        if(addedFields>0 && addedFields<fields.length-1){
          sql += ",";
        }
      }
    }
    sql += " WHERE identifier="+datasetID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    Debug.debug("update exec: "+execok, 2);
    return execok;
  }

  public synchronized boolean updateTransformation(String transformationID, String [] fields,
      String [] values){
    
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      error = "The number of fields and values do not agree, "+
         fields.length+"!="+values.length;
      return false;
    }
    if(fields.length>transformationFields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+transformationFields.length, 1);
      error = "The number of fields is too large, "+
         fields.length+">"+transformationFields.length;
    }

    String sql = "UPDATE transformation SET ";
    int addedFields = 0;
    for(int i=0; i<transformationFields.length; ++i){
      if(!transformationFields[i].equalsIgnoreCase("identifier")){
        for(int j=0; j<fields.length; ++j){
          // only add if present in transformationFields
          if(transformationFields[i].equalsIgnoreCase(fields[j])){
            if(transformationFields[i].equalsIgnoreCase("created")){
              try{
                values[i] = makeDate(values[i].toString());
              }
              catch(Exception e){
                values[i] = makeDate("");
              }
            }
            else if(transformationFields[i].equalsIgnoreCase("lastModified")){
              values[i] = makeDate("");
            }
            else{
              values[j] = "'"+values[j]+"'";
            }
            
            sql += fields[j];
            sql += "=";
            sql += values[j];
            ++addedFields;
            break;
          }
        }
        if(addedFields>0 && addedFields<fields.length-1){
          sql += ", ";
        }
      }
    }
    sql += " WHERE identifier="+transformationID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    }
    catch(Exception e){
      execok = false;
      error = e.getMessage();
      Debug.debug(e.getMessage(), 2);
    }
    Debug.debug("update exec: "+execok, 2);
    return execok;
  }

  public synchronized boolean updateRuntimeEnvironment(String runtimeEnvironmentID, String [] fields,
      String [] values){
    
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      error = "The number of fields and values do not agree, "+
         fields.length+"!="+values.length;
      return false;
    }
    if(fields.length>runtimeEnvironmentFields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+runtimeEnvironmentFields.length, 1);
      error = "The number of fields is too large, "+
         fields.length+">"+runtimeEnvironmentFields.length;
    }
  
    String sql = "UPDATE runtimeEnvironment SET ";
    int addedFields = 0;
    for(int i=0; i<runtimeEnvironmentFields.length; ++i){
      if(!runtimeEnvironmentFields[i].equalsIgnoreCase("identifier")){
        for(int j=0; j<fields.length; ++j){
          // only add if present in runtimeEnvironmentFields
          if(runtimeEnvironmentFields[i].equalsIgnoreCase(fields[j])){
            if(runtimeEnvironmentFields[i].equalsIgnoreCase("created")){
              try{
                values[i] = makeDate(values[i].toString());
              }
              catch(Exception e){
                values[i] = makeDate("");
              }
            }
            else if(runtimeEnvironmentFields[i].equalsIgnoreCase("lastModified")){
              values[i] = makeDate("");
            }
            else{
              values[j] = "'"+Util.dbEncode(values[j])+"'";
            }
            
            sql += fields[j];
            sql += "=";
            sql += values[j];
            ++addedFields;
            break;
          }
        }
        if(addedFields>0 && addedFields<fields.length-1){
          sql += ", ";
        }
      }
    }
    sql += " WHERE identifier="+runtimeEnvironmentID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    Debug.debug("update exec: "+execok, 2);
    return execok;
  }
  
  public synchronized boolean deleteJobDefinition(String jobDefId){
    boolean ok = true;
    try{
      String sql = "DELETE FROM jobDefinition WHERE identifier = '"+
      jobDefId+"'";
      Debug.debug(sql, 3);
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
      ok = false;
    }
    return ok;
  }
  
    public synchronized boolean deleteDataset(String datasetID, boolean cleanup){
      boolean ok = true;
      try{
        String sql = "DELETE FROM dataset WHERE identifier = '"+
        datasetID+"'";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
      }
      catch(Exception e){
        Debug.debug(e.getMessage(), 2);
        error = e.getMessage();
        ok = false;
      }
      if(ok && cleanup){
        ok = deleteJobDefsFromDataset(datasetID);
        if(!ok){
          Debug.debug("ERROR: Deleting job definitions of dataset #"+
              datasetID+" failed."+" Please clean up by hand.", 1);
          error = "ERROR: Deleting job definitions of dataset #"+
             datasetID+" failed."+" Please clean up by hand.";
        }
      }
      return ok;
    }

    public synchronized boolean deleteJobDefsFromDataset(String datasetID){
      boolean ok = true;
      try{
        String sql = "DELETE FROM jobDefinition WHERE dataset = '"+
        getDatasetName(datasetID)+"'";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
      }
      catch(Exception e){
        Debug.debug(e.getMessage(), 1);
        ok = false;
      }
      return ok;
    }

    public synchronized boolean deleteTransformation(String transformationID){
      boolean ok = true;
      try{
        String sql = "DELETE FROM transformation WHERE identifier = '"+
        transformationID+"'";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
      }
      catch(Exception e){
        Debug.debug(e.getMessage(), 2);
        error = e.getMessage();
        ok = false;
      }
      return ok;
    }
      
    public synchronized boolean deleteRuntimeEnvironment(String runtimeEnvironmentID){
      boolean ok = true;
      try{
        String sql = "DELETE FROM runtimeEnvironment WHERE identifier = '"+
        runtimeEnvironmentID+"'";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
      }
      catch(Exception e){
        Debug.debug(e.getMessage(), 2);
        error = e.getMessage();
        ok = false;
      }
      return ok;
    }
      
  public synchronized String [] getVersions(String transformation){   
    String req = "SELECT identifier, version FROM "+
    "transformation WHERE name = '"+transformation+"'";
    Vector vec = new Vector();
    Debug.debug(req, 2);
    String version;
    try{
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      while(rset.next()){
        version = rset.getString("version");
        if(version!=null){
          Debug.debug("Adding version "+version, 3);
          vec.add(version);
        }
        else{
          Debug.debug("WARNING: version null for identifier "+
              rset.getInt("identifier"), 1);
        }
      }
      rset.close();  
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
    }
    Vector vec1 = new Vector();
    if(vec.size()>0){
      Collections.sort(vec);
      vec1.add(vec.get(0));
    }
    for(int i=0; i<vec.size(); ++i){
      if(i>0 && !vec.get(i).toString().equalsIgnoreCase(vec.get(i-1).toString())){
        vec1.add(vec.get(i));
      }
    }
    String [] ret = new String[vec1.size()];
    for(int i=0; i<vec1.size(); ++i){
      ret[i] = vec1.get(i).toString();
    }
    return ret;
  }
  
  public String [] getTransformationOutputs(String transformationID){    
    String outputs = getTransformation(transformationID).getValue("outputFiles").toString();
    return Util.split(outputs);
  }

  public String [] getTransformationInputs(String transformationID){    
    String inputs = getTransformation(transformationID).getValue("inputFiles").toString();
    return Util.split(inputs);
  }
  
  public DBResult getFiles(String datasetID){
    // TODO
    return null;
  }

  public String getError(){
    return error;
  }
  
  private String makeDate(String dateInput){
    try{
      SimpleDateFormat df = new SimpleDateFormat(GridPilot.dateFormatString);
      String dateString = "";
      if(dateInput == null || dateInput.equals("") || dateInput.equals("''")){
        dateString = df.format(Calendar.getInstance().getTime());
      }
      else{
        java.util.Date date = df.parse(dateInput);
        dateString = df.format(date);
      }
      return "'"+dateString+"'";
    }
    catch(Throwable e){
      Debug.debug("Could not set date. "+e.getMessage(), 1);
      e.printStackTrace();
      return dateInput;
    }
  }

  public void registerFileLocation(String fileID, String url){
    // not applicable, not a file catalog
  }

}
