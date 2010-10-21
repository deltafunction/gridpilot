package gridpilot.dbplugins.mysql;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.sql.ResultSet;
import java.sql.Statement;

import javax.swing.SwingUtilities;

import org.logicalcobwebs.proxool.ProxoolException;
import org.logicalcobwebs.proxool.ProxoolFacade;
import org.safehaus.uuid.UUIDGenerator;

import com.mysql.jdbc.NotImplemented;

import gridfactory.common.ConfigFile;
import gridfactory.common.DBCache;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.ResThread;

import gridpilot.Database;
import gridpilot.GridPilot;
import gridpilot.MyLogFile;
import gridpilot.MyUtil;


/**
 * Plugin to access mysql databases.
 * If the _database ends with /, the database name
 * is the grid certificate subject with slash and space
 * replaced with | and _ respectively.
 * If _user is "" or null, the user is the cksum of the grid
 * certificate subject and X509 authentication is used.
 */
public class MySQLDatabase extends DBCache implements Database {
  
  private String driver = "";
  private String database = "";
  private String user = "";
  private String passwd = "";
  private String error = "";
  private String [] executableFields = null;
  private String [] jobDefFields = null;
  private String [] datasetFields = null;
  private String [] runtimeEnvironmentFields = null;
  private String [] t_lfnFields = null;
  private String [] t_pfnFields = null;
  private String [] t_metaFields = null;
  private boolean gridAuth;
  private boolean fileCatalog = false;
  private boolean jobRepository = false;
  private String connectTimeout = null;
  private String socketTimeout = null;
  private HashMap<String, String []> tableFieldNames = new HashMap<String, String []>();
  private boolean stop = false;
  private ConfigFile configFile = null;
  private String dbName;
  
  private static String MAX_CONNECTIONS = "15";

  public MySQLDatabase(String _dbName,
      String _driver, String _database,
      String _user, String _passwd) throws IOException, GeneralSecurityException{
    driver = _driver;
    database = _database;
    user = _user;
    passwd = _passwd;
    dbName = _dbName;
    
    connectTimeout = "0";
    socketTimeout = "0";
    
    configFile = GridPilot.getClassMgr().getConfigFile();
    
    String _connectTimeout = configFile.getValue(dbName, "connect timeout");
    if(_connectTimeout!=null && !_connectTimeout.equals("")){
      connectTimeout = _connectTimeout;
    }
    String _socketTimeout = configFile.getValue(dbName, "socket timeout");
    if(_socketTimeout!=null && !_socketTimeout.equals("")){
      socketTimeout = _socketTimeout;
    }

    if(configFile.getValue(dbName, "t_pfn field names")!=null){
      fileCatalog = true;
    }
    
    if(configFile.getValue(dbName, "jobDefinition field names")!=null){
      jobRepository = true;
    }
    
    boolean showDialog = true;
    // if global frame is set, this is a reload
    if(GridPilot.getClassMgr().getGlobalFrame()==null){
      showDialog = false;
    }
    
    gridAuth = false;
    if(user==null || user.equals("") ||
        database!=null && database.endsWith("/")){
      gridAuth = true;
      String subject = MyUtil.getGridSubject(GridPilot.CERT_FILE);
      
      if(gridAuth){
        try{
          GridPilot.getClassMgr().getSSL().activateSSL();
        }
        catch(Exception e){
          e.printStackTrace();
          Debug.debug("ERROR: "+e.getMessage(), 1);
          return;
        }
      }
      
      if(user==null || user.equals("")){
        user = GridPilot.getClassMgr().getSSL().getGridDatabaseUser();
        Debug.debug("Using user name from cksum of grid subject: "+user, 2);
      }
      
      if(database!=null && database.endsWith("/")){
        String dbName = subject.replaceAll(" ", "_");
        dbName = dbName.replaceAll("/", "|");
        dbName = dbName.replaceAll("\\.", "_");
        dbName = dbName.substring(1);
        database = database + dbName;
      }
      String useCachingStr = configFile.getValue(dbName, "cache search results");
      if(useCachingStr==null || useCachingStr.equalsIgnoreCase("")){
        useCaching = false;
      }
      else{
        useCaching = ((useCachingStr.equalsIgnoreCase("yes")||
            useCachingStr.equalsIgnoreCase("true"))?true:false);
      }
    }
    
    String [] up = null;
    for(int rep=0; rep<3; ++rep){
      if(showDialog ||
          user==null || (passwd==null && !gridAuth) || database==null){
        up = GridPilot.userPwd("DB login to "+dbName, new String [] {"User", "Password", "Database"},
            new String [] {user, passwd, database});
        if(up==null){
          return;
        }
        else{
          user = up[0];
          passwd = up[1];
          database = up[2];
        }
      }
      try{
        connect();
        break;
      }
      catch(Exception e){
        e.printStackTrace();
        passwd = null;
        continue;
      }
    }
    setUpTables();
    
  }
  
  public void requestStop(){
    stop = true;
  }
  
  public void clearRequestStop(){
    stop = false;
  }

  public boolean getStop(){
    return stop;
  }

  public void setUpTables(){
    try{
      setFieldNames();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    if(datasetFields==null){
      makeTable("dataset");
    }
    if(jobDefFields==null){
      makeTable("jobDefinition");
    }
    if(executableFields==null){
      makeTable("executable");
    }
    if(runtimeEnvironmentFields==null){
      makeTable("runtimeEnvironment");
    }
    if(t_lfnFields==null){
      try{
        makeTable("t_lfn");
      }
      catch(Exception e){
      }
    }
    if(t_pfnFields==null){
      try{
        makeTable("t_pfn");
      }
      catch(Exception e){
      }
    }
    if(t_metaFields==null){
      try{
        makeTable("t_meta");
      }
      catch(Exception e){
      }
    }
    try{
      setFieldNames();
    }
    catch(Exception e){
      error = "ERROR: could not set field names. "+e.getMessage();
      Debug.debug(error, 1);
      e.printStackTrace();
    }
  }
  
  public boolean isFileCatalog(){
    return fileCatalog;
  }
  
  public boolean isJobRepository(){
    return jobRepository;
  }
  
  public String connect() throws SQLException, ClassNotFoundException,
     ProxoolException {
    GridPilot.getClassMgr().establishJDBCConnection(dbName, driver, database, user, passwd, gridAuth,
        connectTimeout, socketTimeout, MAX_CONNECTIONS);
    return "";
  }
  
  /**
   * Check if the given table has field names defined in the config file.
   * @param table
   * @return
   */
  private boolean checkTable(String table){
    String [] fields = null;
    //String [] fieldTypes = null;
    try{
      fields = MyUtil.split(configFile.getValue(dbName, table+" field names"), ",");
      //fieldTypes = Util.split(tablesConfig.getValue(dbName, table+" field types"), ",");
    }
    catch(Exception e){
      //e.printStackTrace();
    }
    if(fields==null /*|| fieldTypes==null*/){
      return false;
    }
    else{
      return true;
    }
  }
  
  private void setFieldNames() throws SQLException {
    datasetFields = getFieldNames("dataset");
    jobDefFields = getFieldNames("jobDefinition");
    executableFields = getFieldNames("executable");
    // only used for checking
    runtimeEnvironmentFields = getFieldNames("runtimeEnvironment");
    // only needed if we need an explicit file catalog where more than one
    // pfn per lfn can be registered.
    try{
      t_lfnFields = getFieldNames("t_lfn");
      t_pfnFields = getFieldNames("t_pfn");
      t_metaFields = getFieldNames("t_meta");
    }
    catch(SQLException e){
    }
  }

  private boolean makeTable(String table){
    
    if(!checkTable(table)){
      GridPilot.getClassMgr().getLogFile().addMessage("ERROR: Fields for table "+table+" not defined in configuration file.");
      return false;
    }
    
    String [] fields = MyUtil.split(configFile.getValue(dbName, table+" field names"), ",");
    String [] fieldTypes = MyUtil.split(configFile.getValue(dbName, table+" field types"), ",");
    if(fields==null || fieldTypes==null){
      return false;
    }
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
    sql = sql.replaceAll(";", ",");
    boolean execok = true;
    try{
      Debug.debug("Creating table. "+sql, 1);
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      e.printStackTrace();
      error = e.getMessage();
    }
    return execok;
  }

  public void clearCaches(){
    clearCache();
  }

  public synchronized boolean cleanRunInfo(String jobDefID){
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    String sql = "UPDATE jobDefinition SET jobID = ''," +
        "outTmp = '', errTmp = '', validationResult = '' " +
        "WHERE "+idField+" = '"+jobDefID+"'";
    boolean ok = true;
    try{
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      error = e.getMessage();
      ok = false;
    }
    return ok;
  }

  public void disconnect(){
    try{
      ProxoolFacade.killAllConnections(dbName, "User forced reconnect", false);
    }
    catch(ProxoolException e){
      Debug.debug("Closing connections failed. "+
          e.getCause().toString()+"\n"+e.getMessage(), 1);
    }
  }

  public synchronized String [] getFieldNames(String table)
     throws SQLException {
    Debug.debug("getFieldNames for table "+table, 3);
    String [] ret = null;
    if(tableFieldNames.containsKey(table)){
      if(tableFieldNames.get(table)==null){
        return null;
      }
      ret = new String[tableFieldNames.get(table).length];
      for(int i=0; i<ret.length; ++i){
        ret[i] = tableFieldNames.get(table)[i];
      }
      Debug.debug("returning fields: "+MyUtil.arrayToString(ret), 3);
      return ret;
    }
    if(table.equalsIgnoreCase("file")){
      String nameField = MyUtil.getNameField(dbName, "dataset");
      String [] refFields = MyUtil.getJobDefDatasetReference(dbName);
      if(!isFileCatalog()){
        ret = new String [] {refFields[1], nameField, "url"};
       }
      else{
        ret = MyUtil.split(configFile.getValue(dbName, "file field names"), ", ");
      }
      tableFieldNames.put(table, ret);
      return ret;
    }
    else if(!checkTable(table)){
      Debug.debug("Notice: no fields defined for table "+table+". Using all.", 2);
      tableFieldNames.put(table, null);
      return null;
    }
    Connection conn = getDBConnection(dbName);
    Statement stmt = conn.createStatement();
    // TODO: Do we need to execute a query to get the metadata?
    ResultSet rset = stmt.executeQuery("SELECT * FROM "+table+" LIMIT 1");
    ResultSetMetaData md = rset.getMetaData();
    ret = new String[md.getColumnCount()];
    for(int i=1; i<=md.getColumnCount(); ++i){
      ret[i-1] = md.getColumnName(i);
    }
    conn.close();
    Debug.debug("caching and returning fields for "+dbName+
        "."+table+": "+MyUtil.arrayToString(ret), 3);
    tableFieldNames.put(table, ret);
    return ret;
  }

  public synchronized String getExecutableID(String exeName, String exeVersion){
    String idField = MyUtil.getIdentifierField(dbName, "executable");
    String nameField = MyUtil.getNameField(dbName, "executable");
    String versionField = MyUtil.getVersionField(dbName, "executable");
    String req = "SELECT "+idField+" from executable where "+nameField+" = '"+exeName + "'"+
    " AND "+versionField+" = '"+exeVersion+"'";
    String id = null;
    Vector<String> vec = new Vector<String>();
    try{
      DBResult rset = executeQuery(dbName, req);
      while(rset.moveCursor()){
        id = rset.getString(idField);
        if(id!=null){
          Debug.debug("Adding id "+id, 3);
          vec.add(id);
        }
        else{
          Debug.debug("WARNING: identifier null for name "+
              exeName, 1);
        }
      }
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
      error = e.getMessage();
      return "-1";
    }
    if(vec.size()>1){
      Debug.debug("WARNING: More than one ("+vec.size()+
          ") executable found with name:version "+exeName+":"+exeVersion, 1);
    }
    if(vec.size()==0){
      return "-1";
    }
    else{
      return vec.get(0);
    }
  }

  public synchronized String [] getRuntimeEnvironmentIDs(String name, String cs){
    String nameField = MyUtil.getNameField(dbName, "runtimeEnvironment");
    String idField = MyUtil.getIdentifierField(dbName, "runtimeEnvironment");
    String req = "SELECT "+idField+" from runtimeEnvironment where "+nameField+" = '"+name + "'"+
    " AND computingSystem = '"+cs+"'";
    String id = null;
    Vector<String> vec = new Vector<String>();
    try{
      DBResult rset = executeQuery(dbName, req);
      while(rset.moveCursor()){
        id = rset.getString(idField);
        if(id!=null){
          //Debug.debug("Adding id "+id, 3);
          vec.add(id);
        }
        else{
          Debug.debug("WARNING: identifier null for name "+
              name, 1);
        }
      }
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
      error = e.getMessage();
      return null;
    }
    if(vec.size()>1){
      Debug.debug("WARNING: More than one ("+vec.size()+
          ") runtimeEnvironment found with name:cs "+name+":"+cs, 1);
    }
    if(vec.size()==0){
      return null;
    }
    else{
      return vec.toArray(new String [vec.size()]);
    }
  }

  public String [] getExecutableJobParameters(String executableID) throws InterruptedException{
    String res = (String) getExecutable(executableID).getValue("arguments"); 
    return (res!=null?MyUtil.split(res):new String [] {});
  }

  public String [] getOutputFiles(String jobDefID) throws Exception{
    String executableID = getJobDefExecutableID(jobDefID);
    String outputs = (String) getExecutable(executableID).getValue("outputFiles");
    return (outputs!=null?MyUtil.splitUrls(outputs):new String [] {});
  }

  public String [] getJobDefInputFiles(String jobDefID) throws Exception{
    String inputs = (String) getJobDefinition(jobDefID).getValue("inputFileURLs");
    return (inputs!=null?MyUtil.splitUrls(inputs):new String [] {});
  }

  public String [] getJobDefExecutableParameters(String jobDefID) throws Exception{
    String args = (String) getJobDefinition(jobDefID).getValue("executableParameters");
    return (args!=null?MyUtil.splitUrls(args):new String [] {});
  }

  public String getJobDefOutLocalName(String jobDefID, String par) throws Exception{
    String exeID = getJobDefExecutableID(jobDefID);
    String [] fouts = MyUtil.splitUrls((String) getExecutable(exeID).getValue("outputFiles"));
    String maps = (String) getJobDefinition(jobDefID).getValue("outFileMapping");
    String maps1 = maps.replaceAll("(\\S+) +(\\w+:\\S+)", "file:$1 $2");
    String[] map = null;
    try{
      map = MyUtil.splitUrls(maps1);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not split URLs "+maps, 1);
      map = MyUtil.split(maps);
    }
    String name = "";
    for(int i=0; i<fouts.length; i++){
      if(par.equals(fouts[i])){
        name = map[i*2];
      }
    }
    return MyUtil.clearFile(name);
  }

  public String getJobDefOutRemoteName(String jobDefID, String par) throws Exception{
    String exeID = getJobDefExecutableID(jobDefID);
    // NOTICE: output file names must NOT have spaces.
    String [] fouts = MyUtil.split((String) getExecutable(exeID).getValue("outputFiles"));
    String maps = (String) getJobDefinition(jobDefID).getValue("outFileMapping");
    // maps is of the form out1.txt file:/some/dir/my file1.txt out2.txt file:/dome/dir/my file2.txt ...
    // Prepend file: to out1.txt
    String maps1 = maps.replaceAll("(\\S+) +(\\w+:\\S+)", "file:$1 $2");
    String[] map = MyUtil.splitUrls(maps1);
    String name = "";
    for(int i=0; i<fouts.length; i++){
      if(par.equals(fouts[i])){
        name = map[i*2+1];
        break;
      }
    }
    return name;
  }

  public String getStdOutFinalDest(String jobDefID) throws InterruptedException{
    return (String) getJobDefinition(jobDefID).getValue("stdoutDest");
  }

  public String getStdErrFinalDest(String jobDefID) throws InterruptedException{
    return (String) getJobDefinition(jobDefID).getValue("stderrDest");
  }

  public String getExecutableFile(String jobDefID) throws InterruptedException{
    String executableID = getJobDefExecutableID(jobDefID);
    String script = (String) getExecutable(executableID).getValue("executableFile");
    return script;
  }

  public String [] getRuntimeEnvironments(String jobDefID) throws InterruptedException{
    String executableID = getJobDefExecutableID(jobDefID);
    String rts = (String) getExecutable(executableID).getValue("runtimeEnvironmentName");
    return MyUtil.split(rts);
  }

  public String [] getExecutableArguments(String jobDefID) throws InterruptedException{
    String executableID = getJobDefExecutableID(jobDefID);
    String args = (String) getExecutable(executableID).getValue("arguments");
    return MyUtil.split(args);
  }

  public String getExecutableRuntimeEnvironment(String executableID) throws InterruptedException{
    return (String) getExecutable(executableID).getValue("runtimeEnvironmentName");
  }

  public String getJobDefUserInfo(String jobDefinitionID) throws InterruptedException{
    Object userInfo = getJobDefinition(jobDefinitionID).getValue("userInfo");
    if(userInfo==null){
      return "";
    }
    else{
      return (String) userInfo;
    }
  }

  public String getJobDefStatus(String jobDefinitionID) throws InterruptedException{
    return (String) getJobDefinition(jobDefinitionID).getValue("status");
  }

  public String getJobDefName(String jobDefinitionID) throws InterruptedException{
    String nameField = MyUtil.getNameField(dbName, "jobDefinition");
    return (String) getJobDefinition(jobDefinitionID).getValue(nameField);
  }

  public String getJobDefDatasetID(String jobDefinitionID) throws InterruptedException{
    String datasetName = (String) getJobDefinition(jobDefinitionID).getValue("datasetName");
    String datasetID = getDatasetID(datasetName);
    String idField = MyUtil.getIdentifierField(dbName, "dataset");
    return (String) getDataset(datasetID).getValue(idField);
  }

  public String getRuntimeInitText(String runTimeEnvironmentName, String csName) throws InterruptedException{
    String [] rtes = getRuntimeEnvironmentIDs(runTimeEnvironmentName, csName);
    if(rtes==null||rtes.length==0){
      return null;
    }
    String id = rtes[0];
    String initTxt = (String) getRuntimeEnvironment(id).getValue("initLines");
    return initTxt;
  }

  public synchronized String getJobDefExecutableID(String jobDefinitionID) throws InterruptedException{
    DBRecord dataset = getDataset(getJobDefDatasetID(jobDefinitionID));
    String executable = (String) dataset.getValue("executableName");
    String version = (String) dataset.getValue("executableVersion");
    String exeID = null;
    String idField = MyUtil.getIdentifierField(dbName, "executable");
    String nameField = MyUtil.getNameField(dbName, "executable");
    String req = "SELECT "+idField+" FROM "+
       "executable WHERE "+nameField+" = '"+executable+"' AND version = '"+version+"'";
    Vector<String> vec = new Vector<String>();
    Debug.debug(req, 2);
    try{
      DBResult rset = executeQuery(dbName, req);
      while(rset.moveCursor()){
        if(exeID!=null){
          Debug.debug("WARNING: more than one executable for name, version :" +
              executable+", "+version, 1);
          break;
        }
        exeID = rset.getString(idField);
        if(exeID!=null){
          Debug.debug("Adding version "+exeID, 3);
          vec.add(version);
        }
        else{
          Debug.debug("WARNING: identifier null", 1);
        }
      }
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
    }
    return exeID;
  }

  public boolean reserveJobDefinition(String jobDefID, String userInfo, String cs){
    boolean ret = updateJobDefinition(
        jobDefID,
        new String [] {/*"status", */"userInfo", "computingSystem"},
        new String [] {/*DBPluginMgr.getStatusName(DBPluginMgr.SUBMITTED), */userInfo, cs}
        );
    return ret;
  }
  
  public void executeUpdate(String sql) throws SQLException {
    executeUpdate(dbName, sql);
  }

  // TODO: clean up this mess.
  public synchronized DBResult select(String selectRequest, String idField,
      boolean findAll){
    
    String req = selectRequest;
    boolean withStar = false;
    boolean fileTable = false;
    Pattern patt;
    Matcher matcher;

    Debug.debug(">>> sql string was: "+req, 3);

    // Make sure we have identifier.
    // *, row1, row2 -> *
    if(selectRequest.matches("SELECT \\* FROM.*")){
      withStar = true;
    }
    else if(selectRequest.matches("SELECT \\*\\,.*")){
      withStar = true;
      Debug.debug("Correcting non-valid select pattern", 3);
      patt = Pattern.compile("SELECT \\*\\, (.+) FROM", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceFirst("SELECT * FROM");
    }
    else{
      patt = Pattern.compile(", "+idField+" ", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll(" ");
      patt = Pattern.compile("SELECT "+idField+" FROM", Pattern.CASE_INSENSITIVE);
      if(!patt.matcher(req).find()){
        patt = Pattern.compile(" FROM (\\w+)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst(", "+idField+" FROM "+"$1");
      }
    }
    
    if(isFileCatalog()){
      // The "file" table is a pseudo table constructed from the tables
      // t_pfn, t_lfn and t_meta
      if(req.matches("SELECT (.+) FROM file WHERE (.+)")){
        patt = Pattern.compile("SELECT (.+) FROM file WHERE (.+)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("SELECT $1 FROM t_pfn JOIN t_lfn USING (guid) JOIN t_meta USING (guid) WHERE $2");
        patt = Pattern.compile("WHERE(\\W+)guid(\\s*=.*)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("WHERE$1t_lfn.guid$2");
      }
      if(req.matches("SELECT (.+) FROM file\\b(.*)")){
        patt = Pattern.compile("SELECT (.+) FROM file", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("SELECT $1 FROM t_pfn JOIN t_lfn USING (guid) JOIN t_meta USING (guid)");
      }
      patt = Pattern.compile("SELECT (.*) guid FROM", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceFirst("SELECT $1 t_lfn.guid FROM");
    }
    else{
      // The "file" table is a pseudo table constructed from "jobDefinitions".
      // We replace "url" with "outFileMapping" and parse the values of
      // outFileMapping later.
      // We replace "bytes" "outputFileBytes" and "checksum" with "outputFileChecksum".
      String sizeField = MyUtil.getFileSizeField(dbName);
      String checksumField = MyUtil.getChecksumField(dbName);
      if(req.matches("SELECT (.+) url\\, (.+) FROM file\\b(.*)")){
        patt = Pattern.compile("SELECT (.+) url\\, (.+) FROM file\\b(.*)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("SELECT $1 outFileMapping, $2 FROM file $3");
      }
      else if(req.matches("SELECT (.+) url FROM file\\b(.*)")){
        patt = Pattern.compile("SELECT (.+) url FROM file\\b(.*)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("SELECT $1 outFileMapping FROM file $2");
      }
      if(req.matches("SELECT (.+) "+sizeField+"\\, (.+) FROM file\\b(.*)")){
        patt = Pattern.compile("SELECT (.+) url\\, (.+) FROM file\\b(.*)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("SELECT $1 outputFileBytes, $2 FROM file $3");
      }
      else if(req.matches("SELECT (.+) "+sizeField+" FROM file\\b(.*)")){
        patt = Pattern.compile("SELECT (.+) url FROM file\\b(.*)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("SELECT $1 outputFileBytes FROM file $2");
      }
      if(req.matches("SELECT (.+) "+checksumField+"\\, (.+) FROM file\\b(.*)")){
        patt = Pattern.compile("SELECT (.+) url\\, (.+) FROM file\\b(.*)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("SELECT $1 outputFileChecksum, $2 FROM file $3");
      }
      else if(req.matches("SELECT (.+) "+checksumField+" FROM file\\b(.*)")){
        patt = Pattern.compile("SELECT (.+) url FROM file\\b(.*)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("SELECT $1 outputFileChecksum FROM file $2");
      }
      if(req.matches("SELECT (.+) FROM file\\b(.*)")){
        fileTable = true;
        patt = Pattern.compile("SELECT (.+) FROM file\\b(.*)", Pattern.CASE_INSENSITIVE);
        matcher = patt.matcher(req);
        req = matcher.replaceFirst("SELECT $1 FROM jobDefinition $2");
      }
    }

    patt = Pattern.compile("CONTAINS ([^\\s']+)", Pattern.CASE_INSENSITIVE);
    matcher = patt.matcher(req);
    req = matcher.replaceAll("LIKE '%$1%'");
    patt = Pattern.compile("([<>=]) ([^\\s()']+)", Pattern.CASE_INSENSITIVE);
    matcher = patt.matcher(req);
    req = matcher.replaceAll("$1 '$2'");
    
    Debug.debug(">>> sql string is: "+req, 2);
    
    try{
      return doSelect(req, withStar, idField, fileTable);
    }
    catch(SQLException e){
      e.printStackTrace();
      Debug.debug(e.getMessage(),1);
      return new DBResult(0, 0);
    }
  }
  
  private DBResult doSelect(String req, boolean withStar, String idField, boolean fileTable) throws SQLException {
    int identifierColumn = -1;
    int urlColumn = -1;
    int nameColumn = -1;
    String [] tables = MyUtil.getTableNames(req);
    String [] fields = null;
    String [] tmpFields = null;
    Vector<String> fieldsSet = new Vector<String>();
    if(withStar){
      for(int i=0; i<tables.length; ++i){
        tmpFields = getFieldNames(tables[i]);
        for(int j=0; j<tmpFields.length; ++j){
          if(!fieldsSet.contains(tmpFields[j])){
            fieldsSet.add(tmpFields[j]);
          }
        }
      }
    }
    else{
      tmpFields = MyUtil.getColumnNames(req);
      for(int j=0; j<tmpFields.length; ++j){
        if(!fieldsSet.contains(tmpFields[j])){
          fieldsSet.add(tmpFields[j]);
        }
      }
    }
    fields = new String [fieldsSet.size()];
    int count = 0;
    for(Iterator<String> it=fieldsSet.iterator(); it.hasNext();){
      fields[count] = it.next();
      ++count;
    }
    Debug.debug("found fields: "+MyUtil.arrayToString(fields), 2);
    for(int j=0; j<fields.length; ++j){
      // Make sure that the identifier
      // column is at the end as it should be
      if(withStar && fields[j].equalsIgnoreCase(idField) && 
          j!=fields.length-1){
        identifierColumn = j;
      }
      // Find the outFileMapping column number
      if(fileTable && fields[j].equalsIgnoreCase("outFileMapping") && 
          j!=fields.length-1){
        urlColumn = j;
        // replace "outFileMapping" with "url" in the Table.
        fields[j] = "url";
      }
      // Find the name column number
      if(fileTable && fields[j].equalsIgnoreCase(MyUtil.getNameField(dbName, "jobDefinition")) && 
          j!=fields.length-1){
        nameColumn = j;
        // replace "name" with the what's defined in the config file
        fields[j] = MyUtil.getNameField(dbName, "file");
      }
    }
    if(withStar && identifierColumn>-1){
      fields[identifierColumn] = fields[fields.length-1];
      fields[fields.length-1] = idField;
    }
    DBResult rset = executeQuery(dbName, req);
    String [][] values = new String[rset.values.length][fields.length];
    int i=0;
    if(rset.fields.length!=fields.length){
      Debug.debug("ERROR: inconsistent number of fields "+rset.fields.length+"!="+fields.length+
          " : "+MyUtil.arrayToString(rset.fields)+" : "+MyUtil.arrayToString(fields), 1);
      return new DBResult(0, 0); 
    }
    Debug.debug("Fields: "+MyUtil.arrayToString(fields)+":"+MyUtil.arrayToString(rset.fields), 2);
    while(rset.moveCursor()){
      for(int j=0; j<fields.length; ++j){
        for(int k=0; k<rset.fields.length; ++k){
          if(fields[j].equalsIgnoreCase(rset.fields[k])){
            values[i][j] = selectValue(i, k, withStar, identifierColumn, rset, fileTable, urlColumn);
            break;
          }
        }
      }
      // Add extension to name
      if(nameColumn>-1 && urlColumn>-1 && values[i][urlColumn].indexOf("/")>0){
        int lastSlash = values[i][urlColumn].lastIndexOf("/");
        String fileName = null;
        if(lastSlash>-1){
          fileName = values[i][urlColumn].substring(lastSlash + 1);
        }
        if(fileName.startsWith(values[i][nameColumn])){
          values[i][nameColumn] = fileName;
        }
      }
      i++;
    }
    return new DBResult(fields, values);
  }

  private String selectValue(int i, int j, boolean withStar, int identifierColumn, DBResult rset,
      boolean fileTable, int urlColumn) {
    String foo;
    if(withStar && identifierColumn>-1){
      /*if(j==identifierColumn){
        // identifier column is not at the end, so we swap
        // identifier column and the last column
        foo = (String) rset.getElement(rset.fields.length);
        Debug.debug("values "+i+" "+foo, 2);
      }
      else if(j==rset.fields.length-1){
        foo = (String) rset.getElement(identifierColumn+1);
        Debug.debug("values "+i+" "+foo, 2);
      }
      else{*/
        foo = (String) rset.getElement(j+1);
        Debug.debug("values "+i+" "+foo, 2);
      //}
    }
    else if(fileTable && urlColumn>-1 && j==urlColumn){
      // The first output file specified in outFileMapping
      // is by convention *the* output file.
      String [] foos = MyUtil.split((String) rset.getElement(j+1));
      foo = "";
      if(foos.length>1){
        foo = foos[1];
      }
      else{
        Debug.debug("WARNING: no output file found!", 1);
      }
      Debug.debug("values "+i+" "+foo, 2);
    }
    else{
      foo = (String) rset.getElement(j+1);
      Debug.debug("values "+i+" "+foo, 2);
    }
    return foo;
  }

  public synchronized DBRecord getDataset(String datasetID){
    
    String idField = MyUtil.getIdentifierField(dbName, "dataset");
   
    DBRecord dataset = null;
    String req = "SELECT "+datasetFields[0];
    if(datasetFields.length>1){
      for(int i=1; i<datasetFields.length; ++i){
        req += ", "+datasetFields[i];
      }
    }
    req += " FROM dataset";
    req += " WHERE "+idField+" = '"+ datasetID+"'";
    try{
      Debug.debug(">> "+req, 3);
      DBResult rset = executeQuery(dbName, req);
      Vector<DBRecord> datasetVector = new Vector<DBRecord>();
      while(rset.moveCursor()){
        String values[] = new String[datasetFields.length];
        for(int i=0; i<datasetFields.length;i++){
          values[i] = rset.getString(datasetFields[i]);
          //Debug.debug(datasetFields[i]+"-->"+values[i], 2);
        }
        DBRecord jobd = new DBRecord(datasetFields, values);
        datasetVector.add(jobd);
      }
      if(datasetVector.size()==0){
        Debug.debug("ERROR: No dataset with id "+datasetID, 1);
      }
      else{
        dataset = datasetVector.get(0);
      }
      if(datasetVector.size()>1){
        Debug.debug("WARNING: More than one ("+rset.values.length+") dataset found with id "+datasetID, 1);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No dataset found with id "+datasetID+". "+e.getMessage(), 1);
      return dataset;
    }
     return dataset;
  }
  
  public String getDatasetExecutableName(String datasetID){
    return (String) getDataset(datasetID).getValue("executableName");
  }
  
  public String getDatasetExecutableVersion(String datasetID){
    return (String) getDataset(datasetID).getValue("executableVersion");
  }
  
  public String getDatasetName(String datasetID){
    String nameField = MyUtil.getNameField(dbName, "dataset");
    return (String) getDataset(datasetID).getValue(nameField);
  }

  public synchronized String getDatasetID(String datasetName){
    String idField = MyUtil.getIdentifierField(dbName, "dataset");
    String nameField = MyUtil.getNameField(dbName, "dataset");
    String req = "SELECT "+idField+" from dataset where "+nameField+" = '"+datasetName + "'";
    String id = null;
    Vector<String> vec = new Vector<String>();
    try{
      Debug.debug(">>> sql string was: "+req, 3);
      DBResult rset = executeQuery(dbName, req);
      while(rset.moveCursor()){
        id = rset.getString(idField);
        if(id!=null){
          Debug.debug("Adding id "+id, 3);
          vec.add(id);
        }
        else{
          Debug.debug("WARNING: identifier null for name "+
              datasetName, 1);
        }
      }
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
      return vec.get(0);
    }
  }

  public String getRunNumber(String datasetID){
    return (String) getDataset(datasetID).getValue("runNumber");
  }

  public synchronized DBRecord getRuntimeEnvironment(String runtimeEnvironmentID){
    
    String idField = MyUtil.getIdentifierField(dbName, "runtimeEnvironment");

    DBRecord pack = null;
    String req = "SELECT "+runtimeEnvironmentFields[0];
    if(runtimeEnvironmentFields.length>1){
      for(int i=1; i<runtimeEnvironmentFields.length; ++i){
        req += ", "+runtimeEnvironmentFields[i];
      }
    }
    req += " FROM runtimeEnvironment";
    req += " WHERE "+idField+" = '"+ runtimeEnvironmentID+"'";
    try{
      Debug.debug(">> "+req, 3);
      DBResult rset = executeQuery(dbName, req);
      Vector<DBRecord> runtimeEnvironmentVector = new Vector<DBRecord>();
      String [] jt = new String[runtimeEnvironmentFields.length];
      int i = 0;
      while(rset.moveCursor()){
        jt = new String[runtimeEnvironmentFields.length];
        for(int j=0; j<runtimeEnvironmentFields.length; ++j){
          try{
            jt[j] = (String) rset.getElement(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+(String) rset.getElement(j+1)+" in "+
                runtimeEnvironmentFields[j]+". "+e.getMessage(),1);
          }
        }
        //Debug.debug("Adding value "+jt[0], 3);
        runtimeEnvironmentVector.add(new DBRecord(runtimeEnvironmentFields, jt));
        //Debug.debug("Added value "+runtimeEnvironmentVector.get(i).getAt(0), 3);
        ++i;
      }
      if(i==0){
        Debug.debug("ERROR: No runtime environment found with id "+runtimeEnvironmentID, 1);
      }
      else{
        pack = runtimeEnvironmentVector.get(0);
      }
      if(i>1){
        Debug.debug("WARNING: More than one ("+rset.values.length+") runtime environment found with id "+runtimeEnvironmentID, 1);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No runtime environment with id "+runtimeEnvironmentID+". "+e.getMessage(), 1);
    }
     return pack;
  }
  
  public synchronized DBRecord getExecutable(String executableID){
    
    String idField = MyUtil.getIdentifierField(dbName, "executable");

    DBRecord executable = null;
    String req = "SELECT "+executableFields[0];
    if(executableFields.length>1){
      for(int i=1; i<executableFields.length; ++i){
        req += ", "+executableFields[i];
      }
    }
    req += " FROM executable";
    req += " WHERE "+idField+" = '"+ executableID+"'";
    try{
      Debug.debug(">> "+req, 3);
      DBResult rset = executeQuery(dbName, req);
      Vector<DBRecord> executableVector = new Vector<DBRecord>();
      String [] jt = new String[executableFields.length];
      int i = 0;
      while(rset.moveCursor()){
        jt = new String[executableFields.length];
        for(int j=0; j<executableFields.length; ++j){
          try{
            jt[j] = (String) rset.getElement(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+(String) rset.getElement(j+1)+" in "+
                executableFields[j]+". "+e.getMessage(),1);
          }
        }
        //Debug.debug("Adding value "+jt[0], 3);
        executableVector.add(new DBRecord(executableFields, jt));
        //Debug.debug("Added value "+executableVector.get(i).getAt(0), 3);
        ++i;
      }
      if(i==0){
        Debug.debug("ERROR: No executable found with id "+executableID, 1);
      }
      else{
        executable = executableVector.get(0);
      }
      if(i>1){
        Debug.debug("WARNING: More than one ("+rset.values.length+") executable found with id "+executableID, 1);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No executable with id "+executableID+". "+e.getMessage(), 1);
    }
     return executable;
  }
  
  public String getRunInfo(String jobDefID, String key){
    DBRecord jobDef = getJobDefinition(jobDefID);
    return (String) jobDef.getValue(key);
  }

  /*
   * Find runtimeEnvironment records
   */
  private synchronized DBRecord [] getRuntimeEnvironmentRecords(){
    
    DBResult rset = null;
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
      rset = executeQuery(dbName, req);
      Vector<DBRecord> runtimeEnvironmentVector = new Vector<DBRecord>();
      String [] jt = new String[runtimeEnvironmentFields.length];
      int i = 0;
      while(rset.moveCursor()){
        jt = new String[runtimeEnvironmentFields.length];
        for(int j=0; j<runtimeEnvironmentFields.length; ++j){
          try{
            jt[j] = (String) rset.getElement(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+(String) rset.getElement(j+1)+" in "+
                runtimeEnvironmentFields[j]+". "+e.getMessage(),1);
          }
        }
        //Debug.debug("Adding value "+jt[0], 3);
        runtimeEnvironmentVector.add(new DBRecord(runtimeEnvironmentFields, jt));
        //Debug.debug("Added value "+runtimeEnvironmentVector.get(i).getAt(0), 3);
        ++i;
      }
      allRuntimeEnvironmentRecords = new DBRecord[i];
      for(int j=0; j<i; ++j){
        allRuntimeEnvironmentRecords[j] = runtimeEnvironmentVector.get(j);
        Debug.debug("Added value "+allRuntimeEnvironmentRecords[j].values[0], 3);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No runtime environments found. "+e.getMessage(), 1);
    }
    return allRuntimeEnvironmentRecords;
  }

  /*
   * Find executable records
   */
  private synchronized DBRecord [] getExecutableRecords(){
    
    DBResult rset = null;
    String req = "";
    DBRecord [] allExecutableRecords = null;
    try{      
      req = "SELECT "+executableFields[0];
      if(executableFields.length>1){
        for(int i=1; i<executableFields.length; ++i){
          req += ", "+executableFields[i];
        }
      }
      req += " FROM executable";
      Debug.debug(req, 3);
      rset = executeQuery(dbName, req);
      Vector<DBRecord> executableVector = new Vector<DBRecord>();
      String [] jt = new String[executableFields.length];
      int i = 0;
      while(rset.moveCursor()){
        jt = new String[executableFields.length];
        for(int j=0; j<executableFields.length; ++j){
          try{
            jt[j] = (String) rset.getElement(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+(String) rset.getElement(j+1)+" in "+
                executableFields[j]+". "+e.getMessage(),1);
          }
        }
        //Debug.debug("Adding value "+jt[0], 3);
        executableVector.add(new DBRecord(executableFields, jt));
        //Debug.debug("Added value "+executableVector.get(i).getAt(0), 3);
        ++i;
      }
      allExecutableRecords = new DBRecord[i];
      for(int j=0; j<i; ++j){
        allExecutableRecords[j] = executableVector.get(j);
        Debug.debug("Added value "+allExecutableRecords[j].values[0], 3);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No executables found. "+e.getMessage(), 1);
    }
    return allExecutableRecords;
  }

  // Selects only the fields listed in fieldNames. Other fields are set to "".
  public synchronized DBRecord getJobDefinition(String jobDefinitionID){
    
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    
    String req = "SELECT *";
    req += " FROM jobDefinition where "+idField+" = '"+
    jobDefinitionID + "'";
    Vector<DBRecord> jobdefv = new Vector<DBRecord>();
    Debug.debug(req, 2);
    try{
      DBResult rset = executeQuery(dbName, req);
      while(rset.moveCursor()){
        String values[] = new String[jobDefFields.length];
        for(int i=0; i<jobDefFields.length;i++){
          String fieldname = jobDefFields[i];
          String val = "";
          for(int j=0; j<jobDefFields.length; ++j){
            if(fieldname.equalsIgnoreCase(jobDefFields[j])){
              if(fieldname.equalsIgnoreCase("metaData")){
                // TODO: dbDecode?                
                val = rset.getString(fieldname);
              }
              else{
                val = rset.getString(fieldname);
              }
              break;
            }
            val = "";
          }
          values[i] = val;
          Debug.debug(fieldname+"-->"+val, 2);
        }
        DBRecord jobd = new DBRecord(jobDefFields, values);
        jobdefv.add(jobd);
      }
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
    DBRecord def = jobdefv.get(0);
    jobdefv.removeAllElements();
    return def;
  }

  // Selects only the fields listed in fieldNames. Other fields are set to "".
  private synchronized DBRecord [] selectJobDefinitions(String datasetID, String [] fieldNames,
      String [] statusList, String [] csStatusList){
    
    String req = "SELECT";
    for(int i=0; i<fieldNames.length; ++i){
      if(i>0){
        req += ",";
      }
      req += " "+fieldNames[i];
    }
    req += " FROM jobDefinition";
    if(!datasetID.equals("-1")){
      req += " WHERE datasetName = '"+getDatasetName(datasetID) + "'";
    }
    if(statusList!=null && statusList.length>0){
      if(!datasetID.equals("-1")){
        req += " AND (";
      }
      else{
        req += " WHERE (";
      }
      for(int i=0; i<statusList.length; ++i){
        if(i>0){
          req += " OR ";
        }
        req += " status = '"+statusList[i]+"'";
      }
      req += ")";
    }
    if(csStatusList!=null && csStatusList.length>0){
      if(!datasetID.equals("-1") || statusList!=null && statusList.length>0){
        req += " AND (";
      }
      else{
        req += " WHERE (";
      }
      for(int i=0; i<csStatusList.length; ++i){
        if(i>0){
          req += " OR ";
        }
        req += " csStatus LIKE '"+csStatusList[i]+"%'";
      }
      req += ")";
    }
    Vector<DBRecord> jobdefv = new Vector<DBRecord>();
    Debug.debug(req, 2);
    try{
      DBResult rset = executeQuery(dbName, req);
      while(rset.moveCursor()){
        String values[] = new String[jobDefFields.length];
        for(int i=0; i<jobDefFields.length;i++){
          String fieldname = jobDefFields[i];
          String val = "";
          for(int j=0; j<fieldNames.length; ++j){
            if(fieldname.equalsIgnoreCase(fieldNames[j])){
              val = rset.getString(fieldname);
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
  
  public DBResult getExecutables(){
    DBRecord jt [] = getExecutableRecords();
    DBResult res = new DBResult(executableFields.length, jt.length);
    res.fields = executableFields;
    for(int i=0; i<jt.length; ++i){
      for(int j=0; j<executableFields.length; ++j){
        res.values[i][j] = jt[i].values[j];
      }
    }
    return res;
  }
  
  public DBResult getJobDefinitions(String datasetID, String [] fieldNames,
      String [] statusList, String [] csStatusList){
    
    DBRecord jt [] = selectJobDefinitions(datasetID, fieldNames, statusList, csStatusList);
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
                res.values[i][j] = (String) jt[i].getValue(fieldNames[j]);
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
  
  public synchronized boolean createJobDefinition(String [] _values){
    
    String [] values = (String []) _values.clone();
    
    if(jobDefFields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          jobDefFields.length+"!="+values.length, 1);
      return false;
    }

    String sql = "INSERT INTO jobDefinition (";
    for(int i=1; i<jobDefFields.length; ++i){
      sql += jobDefFields[i];
      if(jobDefFields.length>2 && i<jobDefFields.length-1){
        sql += ",";
      }
    }
    sql += ") VALUES (";
    for(int i=1; i<jobDefFields.length; ++i){     
      if(jobDefFields[i].equalsIgnoreCase("created")){
        try{
          values[i] = makeDate(values[i]);
        }
        catch(Exception e){
          values[i] = makeDate("");
        }
      }
      else if(jobDefFields[i].equalsIgnoreCase("lastModified")){
        values[i] = makeDate("");
      }
      else if((jobDefFields[i].equalsIgnoreCase("outputFileBytes") ||
          jobDefFields[i].equalsIgnoreCase("cpuSeconds")) &&
          (values[i]==null || values[i].equals(""))){
        values[i] = "'0'";
      }
      else if(jobDefFields[i].equalsIgnoreCase("metaData") || jobDefFields[i].equalsIgnoreCase("validationResult")){
        values[i] = "'"+dbEncode(values[i])+"'";
      }
      else{
        values[i] = "'"+values[i]+"'";
      }     
      sql += values[i];
      if(jobDefFields.length>1 && i<jobDefFields.length-1){
        sql += ",";
      }
    }
    sql += ")";
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      executeUpdate(dbName, sql);
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
    trparsstr = MyUtil.arrayToString(trpars);
    for(int i=0 ; i<ofmap.length ; i++){  
      ofmapstr += (i==0?"":" ") + ofmap[i] [0] + " " + ofmap[i] [1];
    }
    // Update DB with "request" and return success/failure
    // Fetch current date and time
    String dateString = MyUtil.makeDateString(null, GridPilot.DATE_FORMAT_STRING);
    // NOTICE: there must be a field jobDefinition.status
    String arg = "INSERT INTO jobDefinition (datasetName, status, ";
    for(int i=0; i<cstAttrNames.length; ++i){
      arg += cstAttrNames[i]+", ";
    }
    arg += "executableParameters, outFileMapping, stdoutDest," +
            "stderrDest, created, lastModified";
    arg += ") values ('"+datasetName+"', 'Defined', ";
    String val;
    for(int i=0; i<resCstAttr.length; ++i){
      val = resCstAttr[i];
      if((val==null || val.equals("")) &&
          isNumField(cstAttrNames[i])){
        val = "0";
      }
      arg += "'"+val+"', ";
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
      boolean execok = true;
      Debug.debug("Updating >>> "+arg, 2);
      try{
        executeUpdate(dbName, arg);
      }
      catch(Exception e){
        execok = false;
        Debug.debug(e.getMessage(), 2);
        error = e.getMessage();
        e.printStackTrace();
      }
      return execok;
    }
    else{
      Debug.debug("ERROR: Could not get dataset of job definition", 1);
      return false;
    }
  }

  private boolean isNumField(String field){
    return field.equalsIgnoreCase("outputFileBytes") ||
    field.equalsIgnoreCase("cpuSeconds") || 
    field.equalsIgnoreCase("number") ||
    field.equalsIgnoreCase("eventMin") ||
    field.equalsIgnoreCase("eventMax") ||
    field.equalsIgnoreCase("nEvents");
  }

  public synchronized boolean createDataset(String table,
      String [] fields, Object [] _values){
    String idField = MyUtil.getIdentifierField(dbName, "dataset");
    Object [] values = new Object [_values.length];
    String nonMatchedStr = "";
    Vector<String> matchedFields = new Vector<String>();
    boolean match = false;
    for(int i=0; i<fields.length; ++i){
      match = false;
      for(int j=0; j<datasetFields.length; ++j){
        if(fields[i].equalsIgnoreCase(datasetFields[j])){
          matchedFields.add(fields[i]);
          match = true;
        }
      }
      if(!match){
        if(i>0){
          nonMatchedStr += "\n";
        }
        nonMatchedStr += fields[i]+": "+_values[i];
      }
      else{
        values[i] = _values[i];
      }
    }
    String sql = "INSERT INTO "+table+" (";
    for(int i=0; i<fields.length; ++i){
      if((isFileCatalog() && fields[i].equalsIgnoreCase(idField) ||
          !((values[i]==null || values[i].toString().equals("''") || values[i].toString().equals("")))) &&
          matchedFields.contains(fields[i])){
        sql += fields[i];
        if(fields.length>0 && i<fields.length-1){
          sql += ",";
        }
      }
    }
    sql += ") VALUES (";
    for(int i=0; i<fields.length; ++i){
      if((isFileCatalog() && fields[i].equalsIgnoreCase(idField) ||
          !((values[i]==null || values[i].toString().equals("''") || values[i].toString().equals("")))) &&
          matchedFields.contains(fields[i])){
        if(!nonMatchedStr.equals("") &&
            // TODO: make metaData field configurable like identifier and name field
            (fields[i].equalsIgnoreCase("comment") ||
                fields[i].equalsIgnoreCase("metaData"))){
          values[i] = values[i]+"\n"+nonMatchedStr;
        }
        if(fields[i].equalsIgnoreCase("created")){
          try{
            values[i] = makeDate((String) values[i]);
          }
          catch(Exception e){
            values[i] = makeDate("");
          }
        }
        else if(fields[i].equalsIgnoreCase("lastModified")){
          values[i] = makeDate("");
        }
        else if(isFileCatalog() && fields[i].equalsIgnoreCase(idField)){
          // Generate uuid if this is a file catalogue and the
          // passed id is not a uuid.
          boolean isNum = false;
          try{
            int num = Integer.parseInt((String) values[i]);
            isNum = (num>-1);
          }
          catch(Exception e){
          }
          if(isNum || values[i]==null || values[i].equals("") ||
              values[i].equals("''")){
            values[i] = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
            String message = "Generated new UUID "+values[i]+" for dataset";
            //GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar().setLabel(message);
            //GridPilot.getClassMgr().getStatusBar().setLabel(message);
            GridPilot.getClassMgr().getLogFile().addInfo(message);
          }
           values[i] = "'"+values[i]+"'";
        }
        else if(values[i]!=null){
          values[i] = values[i].toString().replaceAll("\n","\\\\n");
          values[i] = "'"+values[i]+"'";
        }
        else if(values[i]==null){
          values[i] = "''";
        }
        sql += values[i];
        if(fields.length>0 && i<fields.length-1){
          sql += ",";
        }
      }
    }
    sql += ")";
    // When pasting to non-matching schema, there will be these
    // if there are multiple non-matching fields
    sql = sql.replaceFirst(",\\) VALUES ", ") VALUES ");
    sql = sql.replaceFirst(",\\)$", ")");
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      execok = false;
      e.printStackTrace();
      error = "ERROR: could not create dataset. "+e.getMessage();
      Debug.debug(error, 2);
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
    }
    return execok;
  }

  public synchronized boolean createExecutable(Object [] _values){
    
    Object [] values = (Object []) _values.clone();

    String sql = "INSERT INTO executable (";
    for(int i=1; i<executableFields.length; ++i){
      sql += executableFields[i];
      if(executableFields.length>2 && i<executableFields.length - 1){
        sql += ",";
      }
    }
    sql += ") VALUES (";
    for(int i=1; i<executableFields.length; ++i){
      if(executableFields[i].equalsIgnoreCase("created")){
        try{
          values[i] = makeDate((String) values[i]);
        }
        catch(Exception e){
          values[i] = makeDate("");
        }
      }
      else if(executableFields[i].equalsIgnoreCase("lastModified")){
        values[i] = makeDate("");
      }
      else{
        values[i] = "'"+values[i]+"'";
      }

      sql += values[i];
      if(executableFields.length>1 && i<executableFields.length - 1){
        sql += ",";
      }
    }
    sql += ")";
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    return execok;
  }
  
  public synchronized boolean createRuntimeEnvironment(Object [] _values){
    
    Object [] values = (Object []) _values.clone();

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
          values[i] = makeDate((String) values[i]);
        }
        catch(Exception e){
          values[i] = makeDate("");
        }
      }
      else if(runtimeEnvironmentFields[i].equalsIgnoreCase("lastModified")){
        values[i] = makeDate("");
      }
      else if(values[i]==null){
        values[i] = "''";
      }
      else{
        values[i] = "'"+values[i]+"'";
      }
  
      sql += values[i];
      if(runtimeEnvironmentFields.length>1 && i<runtimeEnvironmentFields.length - 1){
        sql += ",";
      }
    }
    sql += ")";
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      executeUpdate(dbName, sql);
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
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    String sql = "UPDATE jobDefinition SET ";
    sql += field+"='"+value+"' WHERE ";
    // Not very elegant, but we need to use Identifier instead of
    // identifier, because identifier will only have been set if
    // a JobDefinition object has already been made, which may not
    // be the case.
    for(int i=0; i<identifiers.length; ++i){
      sql += idField+"="+identifiers[i];
      if(identifiers.length>1 && i<identifiers.length-1){
        sql += " OR ";
      }
    }
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    Debug.debug("update exec: "+execok, 2);
    return execok;
  }
  
  public boolean updateJobDefinition(String jobDefID, String [] values){
    String nameField = MyUtil.getNameField(dbName, "dataset");
    return updateJobDefinition(
        jobDefID,
        new String [] {"userInfo", "jobID", nameField, "outTmp", "errTmp"},
        values
    );
  }
  
  public synchronized boolean updateJobDefinition(String jobDefID, String [] fields,
      String [] values){
    
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    
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
      if(!jobDefFields[i].equalsIgnoreCase(idField)){
        for(int j=0; j<fields.length; ++j){
          // only add if present in jobDefFields
          if(jobDefFields[i].equalsIgnoreCase(fields[j])){
            if(jobDefFields[i].equalsIgnoreCase("created")){
              try{
                values[j] = makeDate((String) values[j]);
              }
              catch(Exception e){
                values[j] = makeDate("");
              }
            }
            else if(jobDefFields[i].equalsIgnoreCase("lastModified")){
              values[j] = makeDate("");
            }
            else if((jobDefFields[i].equalsIgnoreCase("outputFileBytes") ||
                jobDefFields[i].equalsIgnoreCase("cpuSeconds")) &&
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
    sql += " WHERE "+idField+"="+jobDefID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    Debug.debug("update exec: "+execok, 2);
    return execok;
  }
  
  public synchronized boolean updateDataset(String datasetID, /*not used*/String datasetName, String [] fields,
      String [] values){
    
    String idField = MyUtil.getIdentifierField(dbName, "dataset");

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

    Debug.debug("Updating: "+MyUtil.arrayToString(fields)+" --> "+MyUtil.arrayToString(values), 2);
    
    String sql = "UPDATE dataset SET ";
    int addedFields = 0;
    for(int i=0; i<fields.length; ++i){
      Debug.debug("Checking field: "+fields[i], 2);
      Debug.debug("Value: "+values[i], 2);
      if(!((values[i]==null || values[i].toString().equals("''") || values[i].toString().equals(""))) &&
          !fields[i].equalsIgnoreCase(idField)){
        Debug.debug("Checking dataset fields: "+MyUtil.arrayToString(datasetFields), 2);
        for(int j=0; j<datasetFields.length; ++j){
          // only add if present in datasetFields
          if(fields[i].equalsIgnoreCase(datasetFields[j])){
            if(fields[i].equalsIgnoreCase("created")){
              try{
                values[i] = makeDate(values[i].toString());
              }
              catch(Exception e){
                values[i] = makeDate("");
              }
            }
            else if(fields[i].equalsIgnoreCase("lastModified")){
              values[i] = makeDate("");
            }
            else{
              values[i] = "'"+values[i]+"'";
            }
            if(addedFields>0){
              sql += ",";
            }
            sql += fields[i];
            sql += "=";
            sql += values[i];
            ++addedFields;
            break;
          }
        }
      }
    }
    sql += " WHERE "+idField+"='"+datasetID+"'";
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    Debug.debug("update exec: "+execok, 2);
    return execok;
  }

  public synchronized boolean updateExecutable(String executableID, String [] fields,
      String [] values){
    
    String idField = MyUtil.getIdentifierField(dbName, "executable");
    
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      error = "The number of fields and values do not agree, "+
         fields.length+"!="+values.length;
      return false;
    }
    if(fields.length>executableFields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+executableFields.length, 1);
      error = "The number of fields is too large, "+
         fields.length+">"+executableFields.length;
    }

    String sql = "UPDATE executable SET ";
    int addedFields = 0;
    for(int i=0; i<executableFields.length; ++i){
      if(!executableFields[i].equalsIgnoreCase(idField)){
        for(int j=0; j<fields.length; ++j){
          // only add if present in executableFields
          if(executableFields[i].equalsIgnoreCase(fields[j])){
            if(executableFields[i].equalsIgnoreCase("created")){
              try{
                values[j] = makeDate((String) values[j]);
              }
              catch(Exception e){
                values[j] = makeDate("");
              }
            }
            else if(executableFields[i].equalsIgnoreCase("lastModified")){
              values[j] = makeDate("");
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
    sql += " WHERE "+idField+"="+executableID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      executeUpdate(dbName, sql);
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
    
    String idField = MyUtil.getIdentifierField(dbName, "runtimeEnvironment");
    
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
      if(!runtimeEnvironmentFields[i].equalsIgnoreCase(idField)){
        for(int j=0; j<fields.length; ++j){
          // only add if present in runtimeEnvironmentFields
          if(runtimeEnvironmentFields[i].equalsIgnoreCase(fields[j])){
            if(runtimeEnvironmentFields[i].equalsIgnoreCase("created")){
              try{
                values[j] = makeDate((String) values[j]);
              }
              catch(Exception e){
                values[j] = makeDate("");
              }
            }
            else if(runtimeEnvironmentFields[i].equalsIgnoreCase("lastModified")){
              values[j] = makeDate("");
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
    sql += " WHERE "+idField+"="+runtimeEnvironmentID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try{
      executeUpdate(dbName, sql);
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
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    boolean ok = true;
    try{
      String sql = "DELETE FROM jobDefinition WHERE "+idField+" = '"+
      jobDefId+"'";
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
      ok = false;
    }
    return ok;
  }
  
  public boolean deleteDataset(String datasetID, boolean cleanup) throws NotImplemented {
    throw new NotImplemented();
  }
  
  public synchronized boolean deleteDataset(String datasetID){
    String idField = MyUtil.getIdentifierField(dbName, "dataset");
    boolean ok = true;
    try{
      String sql = "DELETE FROM dataset WHERE "+idField+" = '"+
      datasetID+"'";
      Debug.debug(">>> sql string was: "+sql, 3);
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 2);
      e.printStackTrace();
      error = e.getMessage();
      ok = false;
    }
    return ok;
  }

  public synchronized boolean deleteJobDefsFromDataset(String datasetID){
    String [] refFields = MyUtil.getJobDefDatasetReference(dbName);
    boolean ok = true;
    try{
      String sql = "DELETE FROM jobDefinition WHERE "+refFields[1]+" = '"+
        getDatasetName(datasetID)+"'";
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
      ok = false;
    }
    return ok;
  }
    
  public synchronized boolean deleteExecutable(String executableID){
    String idField = MyUtil.getIdentifierField(dbName, "executable");
    boolean ok = true;
    try{
      String sql = "DELETE FROM executable WHERE "+idField+" = '"+
      executableID+"'";
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
      ok = false;
    }
    return ok;
  }
    
  public synchronized boolean deleteRuntimeEnvironment(String runtimeEnvironmentID){
    String idField = MyUtil.getIdentifierField(dbName, "runtimeEnvironment");
    boolean ok = true;
    try{
      String sql = "DELETE FROM runtimeEnvironment WHERE "+idField+" = '"+
      runtimeEnvironmentID+"'";
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
      ok = false;
    }
    return ok;
  }

  public synchronized String [] getVersions(String executable){   
    String idField = MyUtil.getIdentifierField(dbName, "executable");
    String nameField = MyUtil.getNameField(dbName, "executable");
    String versionField = MyUtil.getVersionField(dbName, "executable");
    String req = "SELECT "+idField+", "+versionField+" FROM "+
    "executable WHERE "+nameField+" = '"+executable+"'";
    Vector<String> vec = new Vector<String>();
    Debug.debug(req, 2);
    String version;
    try{
      DBResult rset = executeQuery(dbName, req);
      while(rset.moveCursor()){
        version = rset.getString("version");
        if(version!=null){
          Debug.debug("Adding version "+version, 3);
          vec.add(version);
        }
        else{
          Debug.debug("WARNING: version null for identifier "+
              rset.getString(idField), 1);
        }
      }
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 1);
    }
    Vector<String> vec1 = new Vector<String>();
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
      ret[i] = vec1.get(i);
    }
    return ret;
  }

  public String [] getExecutableOutputs(String executableID){    
    String outputs = (String) getExecutable(executableID).getValue("outputFiles");
    return MyUtil.split(outputs);
  }

  public String [] getExecutableInputs(String executableID){    
    String inputs = (String) getExecutable(executableID).getValue("inputFiles");
    String [] ret = null;
    try{
      ret = MyUtil.splitUrls(inputs);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return ret;
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

  private String makeDate(String dateInput){
    try{
      SimpleDateFormat df = new SimpleDateFormat(GridPilot.DATE_FORMAT_STRING);
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
  
  public DBRecord getFile(String datasetName, String fileID, int findAllPFNs){
    String [] fields = null;
    Connection conn = null;
    try{
      fields = getFieldNames("file");
      conn = getDBConnection(dbName);
    }
    catch(Exception e){
      e.printStackTrace();
      return null;
    }
    String [] values = new String[fields.length];
    DBRecord file = new DBRecord(fields, values);
    // If the file catalog tables (t_pfn, t_lfn, t_meta) are present,
    // we use them.
    if(isFileCatalog()){
      Vector<String> fieldsVector = new Vector<String>();
      Debug.debug("file fields: "+MyUtil.arrayToString(fields), 3);
      // first some special fields; we lump all pfname's into the same pfname field
      for(int i=0; i<fields.length; ++i){
        try{
          if(fields[i].equalsIgnoreCase("dsname")){
            file.setValue(fields[i], datasetName);
          }
          else if(fields[i].equalsIgnoreCase("lfname")){
            // TODO: we're assuming a on-to-one lfn/guid mapping. Improve.
            file.setValue(fields[i],
                MyUtil.getValues(dbName,
                    "t_lfn", "guid", fileID, new String [] {"lfname"})[0][0]);
          }
          else if(fields[i].equalsIgnoreCase("pfname")){
            String [] pfns = new String [0];
            if(findAllPFNs!=Database.LOOKUP_PFNS_NONE){
              String [][] res = MyUtil.getValues(dbName, "t_pfn", "guid", fileID, new String [] {"pfname"});
              pfns = new String [res.length];
              for(int j=0; j<res.length; ++j){
                pfns[j] = res[j][0];
                if(findAllPFNs==Database.LOOKUP_PFNS_ONE){
                  break;
                }
              }
              if(findAllPFNs==Database.LOOKUP_PFNS_ONE && pfns.length>0){
                pfns = new String [] {pfns[0]};
              }
            }
            file.setValue(fields[i], MyUtil.arrayToString(pfns));
          }
          else if(fields[i].equalsIgnoreCase("guid")){
            file.setValue(fields[i], fileID);
          }
          else{
            fieldsVector.add(fields[i]);
          }
        }
        catch(Exception e){
          Debug.debug("WARNING: could not set field "+fields[i]+". "+e.getMessage(), 2);
        }
      }
      // get the rest of the values
      DBResult res = select("SELECT " +
          MyUtil.arrayToString(fieldsVector.toArray(), ", ") +
            " FROM file WHERE guid = "+fileID, "guid", true);
      for(int i=0; i<fieldsVector.size(); ++i){
        try{
          file.setValue(fieldsVector.get(i),
              (String) res.getValue(0, fieldsVector.get(i)));
        }
        catch(Exception e){
          Debug.debug("WARNING: could not set field "+fieldsVector.get(i)+". "+e.getMessage(), 2);
          e.printStackTrace();
        }
      }
    }
    // If there are no file catalog tables, we construct a virtual file table
    // from the jobDefinition table.
    else if(isJobRepository()){
      // "datasetName", "name", "url"
      DBRecord jobDef = getJobDefinition(fileID);
      for(int i=0; i<fields.length; ++i){
        try{
          file.setValue(fields[i], (String) jobDef.getValue(fields[i]));
        }
        catch(Exception e){
          Debug.debug("WARNING: could not set field "+fields[i]+". "+e.getMessage(), 2);
        }
      }
      for(int i=0; i<jobDef.fields.length; ++i){
        if(jobDef.fields[i].equalsIgnoreCase("outFileMapping")){
          String [] map = MyUtil.split((String) jobDef.getValue(jobDef.fields[i]));
          try{
            file.setValue("url", map[1]);
          }
          catch(Exception e){
            Debug.debug("WARNING: could not set URL. "+e.getMessage(), 2);
          }
        }
      }
    }
    try{
      conn.close();
    }
    catch(SQLException e){
      e.printStackTrace();
    }
    return file;
  }

  // Take different actions depending on whether or not
  // t_lfn, etc. are present
  public String [][] getFileURLs(String datasetName, String fileID, boolean findAll){
    if(isFileCatalog()){
      String ret = null;
      try{
        DBRecord file = getFile(datasetName, fileID, findAll?Database.LOOKUP_PFNS_ALL:Database.LOOKUP_PFNS_ONE);
        ret = (String) file.getValue("pfname");
      }
      catch(Exception e){
        Debug.debug("WARNING: could not get URLs. "+e.getMessage(), 1);
      }
      String [][] urls = new String [2][];
      try{
        urls[1] = MyUtil.splitUrls(ret);
        urls[0] = new String[urls[1].length];
        Arrays.fill(urls[0], "");
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      Debug.debug("Returning URLs "+MyUtil.arrayToString(urls), 2);
      return urls;
    }
    else{
      String ret = null;
      try{
        DBRecord file = getFile(datasetName, fileID, findAll?Database.LOOKUP_PFNS_ALL:Database.LOOKUP_PFNS_ONE);
        ret = (String) file.getValue("url");
      }
      catch(Exception e){
        Debug.debug("WARNING: could not get URLs. "+e.getMessage(), 1);
      }
      return new String [][] {{""}, {ret}};
    }
  }

  /**
   * Returns the files registered for a given dataset id.
   */
  public DBResult getFiles(String datasetID){
    String datasetName = getDatasetName(datasetID);
    String idField = MyUtil.getIdentifierField(dbName, "file");
    String[] dsRefFields = MyUtil.getFileDatasetReference(dbName);
    DBResult res = select("SELECT * FROM file WHERE "+dsRefFields[1]+" = "+datasetName, idField, false);
    return res;
  }

  public void registerFileLocation(String datasetID, String datasetName,
      String fileID, String lfn, String url, String size, String checksum,
      boolean datasetComplete) throws Exception {
    
    Debug.debug("Registering URL "+url+" for file "+
        datasetID+":"+datasetName+":"+fileID+":"+lfn, 2);
    
    // if this is not a file catalog we don't have to do anything
    if(!isFileCatalog()){
      final String msg = "This is a virtual file catalog - it cannot be modified directly.";
      final String title = "Table cannot be modified";
      SwingUtilities.invokeLater(
          new ResThread(){
            public void run(){
              MyUtil.showLongMessage(msg, title);
            }
          }
      );
      return;
    }
    
    boolean datasetExists = false;
    // Check if dataset already exists and has the same id
    try{
      String existingID = null;
      try{
        existingID = getDatasetID(datasetName);
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
      if(existingID!=null && !existingID.equals("-1") && !existingID.equals("")){
        if(!existingID.equalsIgnoreCase(datasetID)){
          error = "WARNING: dataset "+datasetName+" already registered with id "+
            existingID+"!="+datasetID;
          //GridPilot.getClassMgr().getLogFile().addInfo(error);
          Debug.debug(error, 3);
          datasetID = existingID;
        }
        datasetExists = true;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      datasetExists = false;
    }
    
    // If the dataset does not exist, create it
    if(!datasetExists){
      Debug.debug("Creating dataset "+datasetName, 2);
      // TODO: map source name field to target name field and
      // source identifier field to target identifier field.
      try{
        String nameField = MyUtil.getNameField(dbName, "dataset");
        String idField = MyUtil.getIdentifierField(dbName, "dataset");
        GridPilot.getClassMgr().getStatusBar().setLabel("Creating new dataset "+datasetName);
        if(!createDataset("dataset",
            new String [] {nameField, idField}, new Object [] {datasetName, datasetID})){
          throw new SQLException("createDataset failed");
        }
        //datasetID = getDatasetID(datasetName);
        GridPilot.getClassMgr().getLogFile().addInfo("Created new dataset "+datasetName+
            ". Please add some metadata if needed.");
        datasetExists = true;
      }
      catch(Exception e){
        error = "WARNING: could not create dataset "+datasetName+
        ". Aborting . The file "+lfn+" would be an orphan. Please correct this by hand.";
        GridPilot.getClassMgr().getLogFile().addMessage(error, e);
        datasetExists =false;
        return;
      }
    }

    boolean fileExists = false;
    // Check if file already exists and has the same id
    try{
      String existingID = null;
      try{
        existingID = getFileID(lfn);
      }
      catch(Exception ee){
        //ee.printStackTrace();
      }
      if(existingID!=null && !existingID.equals("")){
        if(!existingID.equalsIgnoreCase(fileID)){
          error = "WARNING: file "+lfn+" already registered with id "+
             existingID+"!="+fileID;
          GridPilot.getClassMgr().getLogFile().addMessage(error);
          //fileID = existingID;
        }
        else{
          // If the file is registered with another dataset, generate a new ID to register with this dataset
          String alreadyAssignedDatasetName = null;
          try{
            alreadyAssignedDatasetName = getFileDataset(existingID);
          }
          catch(Exception ee){
            //ee.printStackTrace();
          }
          if(alreadyAssignedDatasetName!=null && !alreadyAssignedDatasetName.equals(datasetName)){
            fileID  = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
            String message = "Generated new UUID "+fileID+" for file";
            GridPilot.getClassMgr().getLogFile().addInfo(message);
          }
          else{
            fileExists = true;
          }
        }
      }
    }
    catch(Exception e){
      fileExists =false;
    }    
    // If the file does not exist, create it - with the url.
    if(!fileExists){
      Debug.debug("Creating file "+lfn, 2);
      try{
        GridPilot.getClassMgr().getStatusBar().setLabel("Creating new file "+lfn);
        if(!createFile(datasetName, fileID, lfn, url, size, checksum)){
          throw new SQLException("create file failed");
        }
        GridPilot.getClassMgr().getLogFile().addInfo("Created new file "+lfn+
            ". Please add some metadata if needed.");
        fileExists = true;
      }
      catch(Exception e){
        error = "WARNING: could not create file "+lfn;
        GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      }
    }
    // Otherwise, just add the url.
    else{
      Debug.debug("Registering URL "+url, 2);
      // If the url is already registered, skip
      String [][] urls = getFileURLs(datasetName, fileID, true);
      for(int i=0; i<urls.length; ++i){
        if(urls[1][i].equals(url)){
          error = "WARNING: URL "+url+" already registered for file "+lfn+". Skipping.";
          GridPilot.getClassMgr().getLogFile().addMessage(error);
          return;
        }
      }
      addUrlToFile(fileID, url);
    }
  }

  // This is only to be used if this is a file catalog.
  private synchronized void addUrlToFile(String fileID, String url)
     throws Exception {
    String sql = "INSERT INTO t_pfn (pfname, guid) VALUES ('"+
    url + "', '" + fileID +"')";
    Debug.debug(sql, 2);
    executeUpdate(dbName, sql);
  }
  
  // This is only to be used if this is a file catalog.
  private synchronized boolean createFile(String datasetName, String fileID,
      String lfn, String url, String size, String checksum){
    if(size==null){
      size = "";
    }
    String chksum = "";
    if(checksum!=null){
      chksum = checksum;
      // Other checksum types than md5 we still use, but keep the "<type>:" tag.
      chksum = chksum.replaceFirst("^md5:", "");
    }
    String sql = "INSERT INTO t_pfn (pfname, guid) VALUES ('"+
       dbEncode(url) + "', '" + fileID + "'); ";
    Debug.debug(sql, 2);
    boolean execok1 = true;
    try{
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      execok1 = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    sql = "INSERT INTO t_lfn (lfname, guid) VALUES ('"+
       dbEncode(lfn) + "', '" + fileID +
    "'); ";
    Debug.debug("Creating file in DB "+dbName+" >> "+sql, 2);
    boolean execok2 = true;
    try{
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      execok2 = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    sql = "INSERT INTO t_meta (guid, dsname, fsize, md5sum) VALUES ('" +
       fileID + "', '" + dbEncode(datasetName) + "', '" + size+ "', '" + chksum +
    "')";
    Debug.debug(sql, 2);
    boolean execok3 = true;
    try{
      executeUpdate(dbName, sql);
    }
    catch(Exception e){
      execok3 = false;
      e.printStackTrace();
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    return execok1 && execok2 && execok3;
  }

  public boolean deleteFiles(String datasetID, String [] fileIDs, boolean cleanup) throws NotImplemented {
    throw new NotImplemented();
  }
  
  public boolean deleteFiles(String datasetID, String [] fileIDs) {
    if(!isFileCatalog()){
      // do nothing
      return true;
    }
    MyLogFile logFile = GridPilot.getClassMgr().getLogFile();
    // If no file IDs are given, delete all from this dataset
    if(fileIDs==null){
      String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
      DBResult filesRes = getFiles(datasetID);
      fileIDs = new String[filesRes.size()];
      for(int i=0; i<fileIDs.length; ++i){
        fileIDs[i] = (String) filesRes.getValue(i, idField);
      }
    }
    boolean ok = true;
    for(int i=0; i<fileIDs.length; ++i){
      try{
        String req = "DELETE FROM t_lfn WHERE guid = '"+fileIDs[i]+"'";
        Debug.debug(">> "+req, 3);
        int rowsAffected = executeUpdate(dbName, req);
        if(rowsAffected==0){
          error = "WARNING: could not delete guid "+fileIDs[i]+" from t_lfn";
          logFile.addMessage(error);
        }
        req = "DELETE FROM t_pfn WHERE guid = '"+fileIDs[i]+"'";
        Debug.debug(">> "+req, 3);
        rowsAffected = executeUpdate(dbName, req);
        if(rowsAffected==0){
          error = "WARNING: could not delete guid "+fileIDs[i]+" from t_pfn";
          logFile.addMessage(error);
        }
        req = "DELETE FROM t_meta WHERE guid = '"+fileIDs[i]+"'";
        Debug.debug(">> "+req, 3);
        rowsAffected = executeUpdate(dbName, req);
        if(rowsAffected==0){
          error = "WARNING: could not delete guid "+fileIDs[i]+" from t_meta";
          logFile.addMessage(error);
        }
      }
      catch(Exception e){
        e.printStackTrace();
        ok = false;
      }
    }
    return ok;
  }

  private synchronized String getFileID(String lfn){
    String ret = null;
    if(isFileCatalog()){
      try{
        ret = MyUtil.getValues(dbName, "t_lfn", "lfname", lfn, new String [] {"guid"})[0][0];
      }
      catch(Exception e){
        e.printStackTrace();
      }
      return ret;
    }
    else if(isJobRepository()){
      try{
        // an autoincremented integer is of no use... Except for when pasting:
        // then we need it to get the pfns.
        String nameField = MyUtil.getNameField(dbName, "jobDefinition");
        String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
        ret = MyUtil.getValues(dbName, "jobDefinition", nameField, lfn,
            new String [] {idField})[0][0];
      }
      catch(Exception e){
        e.printStackTrace();
      }
      return ret;
    }
    else{
      return null;
    }
  }
  
  private synchronized String getFileDataset(String guid){
    String ret = null;
    if(isFileCatalog()){
      ret = MyUtil.getValues(dbName, "t_meta", "guid", guid, new String [] {"dsname"})[0][0];
    }
    else if(isJobRepository()){
      // an auto-incremented integer is of no use... Except for when pasting:
      // then we need it to get the pfns.
      String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
      String datasetField = MyUtil.getJobDefDatasetReference(dbName)[1];
      ret = MyUtil.getValues(dbName, "jobDefinition", idField, guid,
          new String [] {datasetField})[0][0];
    }
    Debug.debug("Got dataset name "+ret+" for file "+guid, 2);
    return ret;
  }
  
  private static String dbEncode(String str){
    if(str==null || str.length()==0){
      return str;
    }
    String retStr = str;
    retStr = retStr.replaceAll("\\$", "\\\\\\$");
    retStr = str.replace('\n',' ');
    retStr = str.replace('\r',' ');
    retStr = retStr.replaceAll("\n","\\\\n");
    retStr = retStr.replaceAll("\'","\\\\\\'");
    Debug.debug("Encoded: "+str+"->"+retStr, 2);
    return retStr;
  }
  
  public String getFileID(String datasetName, String name){
    return getFileID(name);
  }

  public void requestStopLookup() {
  }

  public void clearRequestStopLookup() {
  }

}
