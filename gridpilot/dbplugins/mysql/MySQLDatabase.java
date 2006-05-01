package gridpilot.dbplugins.mysql;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.sql.ResultSet;
import java.sql.Statement;

import gridpilot.ConfigFile;
import gridpilot.Database;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.JobInfo;
import gridpilot.Util;
//import gridpilot.Database.DBRecord;
//import gridpilot.Database.DBResult;

public class MySQLDatabase implements Database{
  
  private String driver = "";
  private String database = "";
  private String user = "";
  private String passwd = "";
  private Connection conn = null;
  private String error = "";
 
  private String [] transformationFields = null;
  private String [] jobDefFields = null;
  private String [] datasetFields = null;
  private String [] runInfoFields = null;
  private String [] packageFields = null;

  public MySQLDatabase(
      String _driver, String _database,
      String _user, String _passwd){
    driver = _driver;
    database = _database;
    user = _user;
    passwd = _passwd;
        
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
    }
    setFieldNames();
    if(datasetFields==null || datasetFields.length<1){
      makeTable("dataset");
    }
    if(jobDefFields==null || jobDefFields.length<1){
      makeTable("jobDefinition");
    }
    if(transformationFields==null || transformationFields.length<1){
      makeTable("transformation");
    }
    if(packageFields==null || packageFields.length<1){
      makeTable("package");
    }
    if(runInfoFields==null || runInfoFields.length<1){
      makeTable("runInfo");
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
    try {
      conn = DriverManager.getConnection(database+
          "?user="+user+"&password="+passwd);
    }
    catch(Exception e){
      Debug.debug("Could not connect to db "+database+
          ", "+user+", "+passwd+" : "+e, 3);
      return null;
    }  
    try {
      conn.setAutoCommit(true);
    }
    catch(Exception e){
      Debug.debug("failed setting auto commit to true: "+e.getMessage(), 2);
    }
    return "";
  }
  
  private void setFieldNames(){
    datasetFields = getFieldNames("dataset");
    jobDefFields = getFieldNames("jobDefinition");
    transformationFields = getFieldNames("transformation");
    // only used for checking
    runInfoFields = getFieldNames("runInfo");
    packageFields = getFieldNames("package");
  }

  private boolean makeTable(String table){
    ConfigFile tablesConfig = new ConfigFile("gridpilot/dbplugins/mysql/tables.conf");
    String [] fields = Util.split(tablesConfig.getValue("tables", table+" field names"), ",");
    String [] fieldTypes = Util.split(tablesConfig.getValue("tables", table+" field types"), ",");
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
    return execok;
  }

  public String getPanelUtilClass(){
    return "MySQLPanelUtil";
  }

  public synchronized void clearCaches(){
    // nothing for now
  }

  public synchronized boolean dereserveJobDefinition(int partID){
    // nothing for now
    return false;
  }

  public void disconnect(){
    try{
      conn.close();
    }
    catch(SQLException e){
      Debug.debug("Closing connection failed. "+
          e.getCause().toString()+"\n"+e.getMessage(),1);
    }
  }

  public synchronized DBResult getAllPartJobInfo(int partID){
    // nothing for now
    return new DBResult();
  }

  public synchronized String [] getDefVals(int datasetID, String user){
    // nothing for now
    return new String [] {""};
  }
 
  public synchronized String [] getFieldNames(String table){
    try{
      Debug.debug("getFieldNames for table "+table, 3);
      Statement stmt = conn.createStatement();
      // TODO: Do we need to execute a query to get the metadata?
      ResultSet rset = stmt.executeQuery("SELECT * FROM "+table+" LIMIT 1");
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

  public synchronized String [] getTransJobParameters(int transformationID){
    // nothing for now
    return new String [] {""};
  }

  public synchronized String [] getOutputs(int jobDefID){
    String transformationID = "";
    transformationID = getTransformationID(jobDefID);
    // TODO: finish
    getTransformation(Integer.parseInt(transformationID)).getValue("outputs");
    // nothing for now
    return new String [] {""};
  }

  public synchronized String [] getInputs(int transformationID){
    // nothing for now
    return new String [] {""};
  }

  public synchronized String [] getJobDefTransPars(int transformationID){
    // nothing for now
    return new String [] {""};
  }

  public synchronized String getJobDefOutLocalName(int jobDefinitionID, String par){
    // nothing for now
    return "";
  }

  public synchronized String getJobDefInRemoteName(int jobDefinitionID, String par){
    // nothing for now
    return "";
  }

  public synchronized String getJobDefInLocalName(int jobDefinitionID, String par){
    // nothing for now
    return "";
  }

  public synchronized String getJobDefOutRemoteName(int jobDefinitionID, String par){
    // nothing for now
    return "";
  }

  public synchronized String getStdOutFinalDest(int jobDefinitionID){
    // nothing for now
    return "";
  }

  public synchronized String getStdErrFinalDest(int jobDefinitionID){
    // nothing for now
    return "";
  }

  public synchronized String getExtractScript(int jobDefinitionID){
    // nothing for now
    return "";
  }

  public synchronized String getValidationScript(int jobDefinitionID){
    // nothing for now
    return "";
  }

  public synchronized String getTransformationScript(int jobDefinitionID){
    // nothing for now
    return "";
  }

  public synchronized String [] getTransformationPackages(int jobDefID){
    String transformationID = getTransformationID(jobDefID);
    getTransformation(Integer.parseInt(transformationID)).getValue("uses");
    // nothing for now
    return new String [] {""};
  }

  public synchronized String [] getTransformationSignature(int jobDefID){
    String transformationID = "";
    transformationID = getTransformationID(jobDefID);
    // TODO: finish
    getTransformation(Integer.parseInt(transformationID)).getValue("uses");
    // nothing for now
    return new String [] {""};
  }

  public synchronized String getJobDefUser(int jobDefinitionID){
    return getJobDefinition(jobDefinitionID).getValue("userInfo").toString();
  }

  public synchronized String getJobStatus(int jobDefinitionID){
    return getJobDefinition(jobDefinitionID).getValue("status").toString();
  }

  public synchronized String getJobDefName(int jobDefinitionID){
    return getJobDefinition(jobDefinitionID).getValue("name").toString();
  }

  public synchronized int getJobDefDatasetID(int jobDefinitionID){
    int datasetID = Integer.parseInt(
        getJobDefinition(jobDefinitionID).getValue("dataset").toString());
    return Integer.parseInt(getDataset(datasetID).getValue("identifier").toString());
  }

  public synchronized String getJobRunUser(int jobDefinitionID){
    // nothing for now
    return "";
  }

 public synchronized String getPackInitText(String pack, String cluster){
    // nothing for now
    return "";
  }

  public synchronized String getTransformationID(int jobDefinitionID){
    DBRecord dataset = getDataset(getJobDefDatasetID(jobDefinitionID));
    String transformation = dataset.getValue("transformation").toString();
    String version = dataset.getValue("transVersion").toString();
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

  public synchronized String getUserLabel(){
    // nothing for now
    return "";
  }

  // panel creation methods
  
  public synchronized String [] getTransformationVersions(int datasetIdentifier){
    // nothing for now
    return new String [] {""};
  }

  public synchronized boolean reserveJobDefinition(int jobDefinitionID, String userName){
    // nothing for now
    return false;
  }

  public synchronized boolean saveDefVals(int datasetID, String[] defvals, String user){
    // nothing for now
    return false;
  }

  public synchronized DBResult select(String selectRequest, String identifier){
    
    String req = selectRequest;
    boolean withStar = false;
    int identifierColumn = -1;
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
    
    patt = Pattern.compile("CONTAINS (\\w+)", Pattern.CASE_INSENSITIVE);
    matcher = patt.matcher(req);
    req = matcher.replaceAll("LIKE '%$1%'");
    
    patt = Pattern.compile("([<>=]) (\\w+)", Pattern.CASE_INSENSITIVE);
    matcher = patt.matcher(req);
    req = matcher.replaceAll("$1 '$2'");
    
    Debug.debug(">>> sql string is: "+req, 3);
    
    try{
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      ResultSetMetaData md = rset.getMetaData();
      String[] fields = new String[md.getColumnCount()];
      //find out how many rows..
      int i=0;
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
          else{
            String foo =  rset.getString(j+1);
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

////////////////////////////////////////////////////////////
  
  public synchronized DBRecord getDataset(int datasetID){
    
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
            values[i] = Integer.toString(rset.getInt(datasetFields[i]));
          }
          else{
            values[i] = rset.getString(datasetFields[i]);
          }
          Debug.debug(datasetFields[i]+"-->"+values[i], 2);
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
  
  public synchronized String getDatasetName(int datasetID){
    return getDataset(datasetID).getValue("name").toString();
  }

  public synchronized String getRunNumber(int datasetID){
    return getDataset(datasetID).getValue("runNumber").toString();
  }

  public synchronized DBRecord getPackage(int packageID){
    
    DBRecord pack = null;
    String req = "SELECT "+packageFields[0];
    if(packageFields.length>1){
      for(int i=1; i<packageFields.length; ++i){
        req += ", "+packageFields[i];
      }
    }
    req += " FROM package";
    req += " WHERE identifier = '"+ packageID+"'";
    try{
      Debug.debug(">> "+req, 3);
      ResultSet rset = conn.createStatement().executeQuery(req);
      Vector packageVector = new Vector();
      String [] jt = new String[packageFields.length];
      int i = 0;
      while(rset.next()){
        jt = new String[packageFields.length];
        for(int j=0; j<packageFields.length; ++j){
          try{
            jt[j] = rset.getString(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+rset.getString(j+1)+" in "+
                packageFields[j]+". "+e.getMessage(),1);
          }
        }
        Debug.debug("Adding value "+jt[0], 3);
        packageVector.add(new DBRecord(packageFields, jt));
        Debug.debug("Added value "+((DBRecord) packageVector.get(i)).getAt(0), 3);
        ++i;
      }
      if(i==0){
        Debug.debug("ERROR: No package found with id "+packageID, 1);
      }
      else{
        pack = ((DBRecord) packageVector.get(0));
      }
      if(i>1){
        Debug.debug("WARNING: More than one ("+rset.getRow()+") package found with id "+packageID, 1);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No package with id "+packageID+". "+e.getMessage(), 1);
    }
     return pack;
  }
  
  public synchronized DBRecord getTransformation(int transformationID){
    
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
  
  public synchronized DBRecord getRunInfo(int jobDefID){
    // TODO: implement
    return new DBRecord();
  }

  /*
   * Find package records
   */
  private synchronized DBRecord [] getPackageRecords(){
    
    ResultSet rset = null;
    String req = "";
    DBRecord [] allPackageRecords = null;   
    try{      
      req = "SELECT "+packageFields[0];
      if(packageFields.length>1){
        for(int i=1; i<packageFields.length; ++i){
          req += ", "+packageFields[i];
        }
      }
      req += " FROM package";
      Debug.debug(req, 3);
      rset = conn.createStatement().executeQuery(req);
      Vector packageVector = new Vector();
      String [] jt = new String[packageFields.length];
      int i = 0;
      while(rset.next()){
        jt = new String[packageFields.length];
        for(int j=0; j<packageFields.length; ++j){
          try{
            jt[j] = rset.getString(j+1);
          }
          catch(Exception e){
            Debug.debug("Could not set value "+rset.getString(j+1)+" in "+
                packageFields[j]+". "+e.getMessage(),1);
          }
        }
        Debug.debug("Adding value "+jt[0], 3);
        packageVector.add(new DBRecord(packageFields, jt));
        Debug.debug("Added value "+((DBRecord) packageVector.get(i)).getAt(0), 3);
        ++i;
      }
      allPackageRecords = new DBRecord[i];
      for(int j=0; j<i; ++j){
        allPackageRecords[j] = ((DBRecord) packageVector.get(j));
        Debug.debug("Added value "+allPackageRecords[j].getAt(0), 3);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No packages found. "+e.getMessage(), 1);
    }
    return allPackageRecords;
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
        Debug.debug("Adding value "+jt[0], 3);
        transformationVector.add(new DBRecord(transformationFields, jt));
        Debug.debug("Added value "+((DBRecord) transformationVector.get(i)).getAt(0), 3);
        ++i;
      }
      allTransformationRecords = new DBRecord[i];
      for(int j=0; j<i; ++j){
        allTransformationRecords[j] = ((DBRecord) transformationVector.get(j));
        Debug.debug("Added value "+allTransformationRecords[j].getAt(0), 3);
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No transformations found. "+e.getMessage(), 1);
    }
    return allTransformationRecords;
  }

  // Selects only the fields listed in fieldNames. Other fields are set to "".
  public synchronized DBRecord getJobDefinition(int jobDefinitionID){
    
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
              if(fieldname.endsWith("FK") || fieldname.endsWith("ID")){
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
          Debug.debug(fieldname+"-->"+val, 2);
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

  // Selects only the fields listed in fieldNames. Other fields are set to "".
  public synchronized DBRecord [] selectJobDefinitions(int datasetID, String [] fieldNames){
    
    String req = "SELECT";
    for(int i=0; i<fieldNames.length; ++i){
      if(i>0){
        req += ",";
      }
      req += " "+fieldNames[i];
    }
    req += " FROM jobDefinition where datasetFK = '"+
    datasetID + "'";
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
              if(fieldname.endsWith("FK") || fieldname.endsWith("ID")){
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
    
  public synchronized DBResult getPackages(){
    DBRecord jt [] = getPackageRecords();
    DBResult res = new DBResult(packageFields.length, jt.length);
    res.fields = packageFields;
    for(int i=0; i<jt.length; ++i){
      for(int j=0; j<jt.length; ++j){
        res.values[i][j] = jt[i].values[j];
      }
    }
    return res;
  }
  
  public synchronized DBResult getTransformations(){
    DBRecord jt [] = getTransformationRecords();
    DBResult res = new DBResult(transformationFields.length, jt.length);
    res.fields = transformationFields;
    for(int i=0; i<jt.length; ++i){
      for(int j=0; j<jt.length; ++j){
        res.values[i][j] = jt[i].values[j];
      }
    }
    return res;
  }
  
  public synchronized DBResult getJobDefinitions(int datasetID, String [] fieldNames){
    
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
    
    String [] fields = getFieldNames("jobDefintion");
    
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      return false;
    }

    String sql = "INSERT INTO jobDefinition (";
    for(int i = 1; i < fields.length; ++i){
      sql += fields[i];
      if(fields.length > 2 && i < fields.length - 1){
        sql += ",";
      }
    }
    //sql += ",lastAttempt";
    sql += ") VALUES (";
    for(int i = 1; i < fields.length; ++i){
      
      if(fields[i].equalsIgnoreCase("creationTime") ||
          fields[i].equalsIgnoreCase("modificationTime")){
        try{
          SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          java.util.Date date = df.parse(values[i]);
          String dateString = df.format(date);
          values[i] = "TO_DATE('"+dateString+"', 'YYYY-MM-DD HH24:MI:SS')";
        }
        catch(Throwable e){
          Debug.debug("Could not set date. "+e.getMessage(), 1);
          e.printStackTrace();
        }
      }
      else{
        values[i] = "'"+values[i]+"'";
      }
      
      sql += values[i];
      if(fields.length > 1 && i < fields.length - 1){
        sql += ",";
      }
    }
    sql += ")";
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
    return execok;
  }
  
  public synchronized boolean createRunInfo(JobInfo jobInfo){
    // TODO: implement
    return true;
  }
  
  public synchronized boolean createDataset(String table,
      String [] fields, Object [] values){ 
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
    for(int i=1; i<datasetFields.length; ++i){
      if(!nonMatchedFields.contains(new Integer(i))){
        if(!nonMatchedStr.equals("") &&
            datasetFields[i].equalsIgnoreCase("comment")){
          values[i] = nonMatchedStr;
        }
        if(datasetFields[i].equalsIgnoreCase("creationTime") ||
            datasetFields[i].equalsIgnoreCase("modificationTime")){
          try{
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date date = df.parse(values[i].toString());
            String dateString = df.format(date);
            values[i] = "TO_DATE('"+dateString+"', 'YYYY-MM-DD HH24:MI:SS')";
          }
          catch(Throwable e){
            Debug.debug("Could not set date. "+e.getMessage(), 1);
            e.printStackTrace();
          }
        }
        else{
          values[i] = values[i].toString().replaceAll("\n","\\\\n");
          values[i] = "'"+values[i]+"'";
        }
        sql += values[i].toString();
        if(datasetFields.length>0 && i<datasetFields.length-1){
          sql += ",";
        }
      }
    }
    sql += ")";
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
      
      if(transformationFields[i].equalsIgnoreCase("creationTime") ||
          transformationFields[i].equalsIgnoreCase("modificationTime")){
        try{
          SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          java.util.Date date = df.parse(values[i].toString());
          String dateString = df.format(date);
          values[i] = "TO_DATE('"+dateString+"', 'YYYY-MM-DD HH24:MI:SS')";
        }
        catch(Throwable e){
          Debug.debug("Could not set date. "+e.getMessage(), 1);
          e.printStackTrace();
        }
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
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    return execok;
  }
  
  public synchronized boolean createPackage(Object [] values){
    
      String sql = "INSERT INTO package (";
      for(int i=1; i<packageFields.length; ++i){
        sql += packageFields[i];
        if(packageFields.length>2 && i<packageFields.length - 1){
          sql += ",";
        }
      }
      sql += ") VALUES (";
      for(int i=1; i<packageFields.length; ++i){
        
        if(packageFields[i].equalsIgnoreCase("creationTime") ||
            packageFields[i].equalsIgnoreCase("modificationTime")){
          try{
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date date = df.parse(values[i].toString());
            String dateString = df.format(date);
            values[i] = "TO_DATE('"+dateString+"', 'YYYY-MM-DD HH24:MI:SS')";
          }
          catch(Throwable e){
            Debug.debug("Could not set date. "+e.getMessage(), 1);
            e.printStackTrace();
          }
        }
        else{
          values[i] = "'"+values[i].toString()+"'";
        }
    
        sql += values[i].toString();
        if(packageFields.length>1 && i<packageFields.length - 1){
          sql += ",";
        }
      }
      sql += ")";
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
      return execok;
    }
    
  public synchronized boolean setJobDefsField(int [] identifiers,
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
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
      error = e.getMessage();
    }
    Debug.debug("update exec: "+execok, 2);
    return execok;
  }
  
  public synchronized boolean updateJobDefStatus(int jobDefID,
      String status){
    return updateJobDefinition(
        jobDefID,
        new String [] {"status"},
        new String [] {status}
        );
  }
  
  public synchronized boolean updateJobDefinition(int jobDefID,
      String [] values){
    return updateJobDefinition(
        jobDefID,
        new String [] {"jobDefID", "jobName"/*, "stdOut", "stdErr"*/},
        new String [] {values[0], values[1]}
        );
  }
  
  public synchronized boolean updateJobDefinition(int jobDefID, String [] fields,
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
      if(!jobDefFields[i].equals("identifier")){
        for(int j=0; j<fields.length; ++j){
          // only add if present in transformationFields
          if(jobDefFields[i].equalsIgnoreCase(fields[j])){

            if(jobDefFields[i].equalsIgnoreCase("creationTime") ||
                jobDefFields[i].equalsIgnoreCase("modificationTime")){
              try{
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String dateString = null;
                if(jobDefFields[i].equalsIgnoreCase("modificationTime")){
                  dateString = df.format(Calendar.getInstance().getTime());
                }
                if(jobDefFields[i].equalsIgnoreCase("creationTime")){
                  java.util.Date date = df.parse(values[j]);
                  dateString = df.format(date);
                }
                values[j] = "TO_DATE('"+dateString+"', 'YYYY-MM-DD HH24:MI:SS')";
              }
              catch(Throwable e){
                Debug.debug("Could not set date. "+e.getMessage(), 1);
                e.printStackTrace();
              }
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
    sql += " WHERE identifier="+jobDefID;
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
  
  public synchronized boolean updateRunInfo(JobInfo jobInfo){
    // TODO: implement
    return true;
  }
  
  public synchronized boolean updateDataset(int datasetID, String [] fields,
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
    for(int i = 0; i < datasetFields.length; ++i){
      if(!datasetFields[i].equals("identifier")){
        for(int j=0; j<fields.length; ++j){
          // only add if present in datasetFields
          if(datasetFields[i].equalsIgnoreCase(fields[j])){
            
            if(datasetFields[i].equalsIgnoreCase("creationTime") ||
                datasetFields[i].equalsIgnoreCase("modificationTime")){
              try{
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                java.util.Date date = df.parse(values[j]);
                String dateString = df.format(date);
                values[j] = "TO_DATE('"+dateString+"', 'YYYY-MM-DD HH24:MI:SS')";
              }
              catch(Throwable e){
                Debug.debug("Could not set date. "+e.getMessage(), 1);
                e.printStackTrace();
              }
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

  public synchronized boolean updateTransformation(int transformationID, String [] fields,
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
    for(int i = 0; i<transformationFields.length; ++i){
      if(!transformationFields[i].equals("identifier")){
        for(int j=0; j<fields.length; ++j){
          // only add if present in transformationFields
          if(transformationFields[i].equalsIgnoreCase(fields[j])){
            
            if(transformationFields[i].equalsIgnoreCase("creationTime") ||
                transformationFields[i].equalsIgnoreCase("modificationTime")){
              try{
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                java.util.Date date = df.parse(values[j]);
                String dateString = df.format(date);
                values[j] = "TO_DATE('"+dateString+"', 'YYYY-MM-DD HH24:MI:SS')";
              }
              catch(Throwable e){
                Debug.debug("Could not set date. "+e.getMessage(), 1);
                e.printStackTrace();
              }
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
  
  public synchronized boolean updatePackage(int packageID, String [] fields,
      String [] values){
    
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      error = "The number of fields and values do not agree, "+
         fields.length+"!="+values.length;
      return false;
    }
    if(fields.length>packageFields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+packageFields.length, 1);
      error = "The number of fields is too large, "+
         fields.length+">"+packageFields.length;
    }
  
    String sql = "UPDATE package SET ";
    int addedFields = 0;
    for(int i = 0; i<packageFields.length; ++i){
      if(!packageFields[i].equals("identifier")){
        for(int j=0; j<fields.length; ++j){
          // only add if present in packageFields
          if(packageFields[i].equalsIgnoreCase(fields[j])){
            
            if(packageFields[i].equalsIgnoreCase("creationTime") ||
                packageFields[i].equalsIgnoreCase("modificationTime")){
              try{
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                java.util.Date date = df.parse(values[j]);
                String dateString = df.format(date);
                values[j] = "TO_DATE('"+dateString+"', 'YYYY-MM-DD HH24:MI:SS')";
              }
              catch(Throwable e){
                Debug.debug("Could not set date. "+e.getMessage(), 1);
                e.printStackTrace();
              }
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
    sql += " WHERE identifier="+packageID;
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

  public synchronized boolean deleteJobDefinition(int jobDefId){
    boolean ok = true;
    try{
      //String idstr = jobDef.jobDefinitionID;
      //Integer jobid = Integer.valueOf(idstr);
      String sql = "DELETE FROM jobDefinition WHERE identifier = '"+
      jobDefId+"'";
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
  
    public synchronized boolean deleteDataset(int datasetID, boolean cleanup){
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

    public synchronized boolean deleteJobDefsFromDataset(int datasetID){
      boolean ok = true;
      try{
        String sql = "DELETE FROM jobDefinition WHERE dataset = '"+
        getDataset(datasetID).getValue("name")+"'";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
      }
      catch(Exception e){
        Debug.debug(e.getMessage(), 1);
        ok = false;
      }
      return ok;
    }
      
    public synchronized boolean deleteTransformation(int transformationID){
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
      
    public synchronized boolean deletePackage(int packageID){
      boolean ok = true;
      try{
        String sql = "DELETE FROM package WHERE identifier = '"+
        packageID+"'";
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

    public synchronized String getError(){
      return error;
    }

}
