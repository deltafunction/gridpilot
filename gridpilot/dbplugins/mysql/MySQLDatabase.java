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

import gridpilot.Database;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.JobInfo;

public class MySQLDatabase implements Database{
  
  private String dbName = null;
  private String driver = "";
  private String host = "";
  private String database = "";
  private String user = "";
  private String passwd = "";
  private String taskTable = null;
  private String taskIdentifier = null;
  private String jobDefTable = null;
  private String jobDefIdentifier = null;
  private String jobTransTable = null;
  private String jobTransIdentifier = null;
  private Connection conn = null;
  
  public MySQLDatabase(/*String _project, String _level, String _site, String _transDB,*/
      String _dbName,
      String _driver, String _host, String _database, String _user,
      String _passwd, String _dbprefix){
  	driver = _driver;
    host = _host;
    database = _database;
  	user = _user;
  	passwd = _passwd;
    dbName = _dbName;
    
    taskTable = GridPilot.getClassMgr().getConfigFile().getValue(dbName,
       "task table name");
    taskIdentifier = GridPilot.getClassMgr().getConfigFile().getValue(dbName,
       "task table identifier");
    jobDefTable = GridPilot.getClassMgr().getConfigFile().getValue(dbName,
       "job definition table name");
    jobDefIdentifier = GridPilot.getClassMgr().getConfigFile().getValue(dbName,
       "job definition table identifier");
    jobTransTable = GridPilot.getClassMgr().getConfigFile().getValue(dbName,
       "transformation table name");
    jobTransIdentifier = GridPilot.getClassMgr().getConfigFile().getValue(dbName,
       "transformation table identifier");
    
    Debug.debug("task table name: "+database+":"+taskTable, 2);
    
    String [] up = null;
    
    for(int rep=0; rep<3; ++rep){
      up = GridPilot.userPwd(user, passwd, database);
      if(up == null){
        GridPilot.exit(0);
        return;
      }
      else{
        user = up[0];
        passwd = up[1];
        database = up[2];
      }
      if(connect()!=null){
        return;
      }
    }
  }
  
  public String connect(){
    try{
      Class.forName(driver).newInstance();
    }
    catch(Exception e){
  		Debug.debug("Could not load the driver "+driver, 3);
  		return null;
  	}
  	try {
      conn = DriverManager.getConnection("jdbc:mysql://"+host+"/"+database+
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
      Debug.debug("talking to the db failed: "+e.getMessage(), 2);
  		//return null;
      return "";
  	}
    return "";
  }
  
  public synchronized void clearCaches(){
    // nothing for now
  }

  public synchronized int createPart(int datasetID, String lfn, String partNr,
      String evMin, String evMax,
      String transID, String [] trpars,
      String [] [] ofmap, String odest, String edest){
    // nothing for now
    return -1;
  }
  
  public synchronized boolean deletePart(int partID){
    // nothing for now
    return false;
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

  public synchronized String [] getDefVals(int taskId, String user){
    // nothing for now
    return new String [] {""};
  }
 
  public synchronized String [] getFieldNames(String table){
    try{
      Debug.debug("getFieldNames for table "+table, 3);
      Statement stmt = conn.createStatement();
      // TODO: Do we need to execute a query to get the metadata?
      ResultSet rset = stmt.executeQuery("describe " + table);
      ResultSetMetaData md = rset.getMetaData();
      String [] res = new String[md.getColumnCount()];
      for(int i=1; i<=md.getColumnCount(); ++i){
        res[i-1] = md.getColumnName(i);
      }
      return res;
    }
    catch(Exception e){
      Debug.debug(e.getMessage(),1);
      return new String[] {"taskName", "status",
          "taskTransFK", "actualPars", "taskID"};
    }
  }

  public synchronized String [] getTransJobParameters(int transformationID){
    // nothing for now
    return new String [] {""};
  }

  public synchronized String [] getOutputs(int jobDefID){
    String jobTransID = "";
    jobTransID = getTransformationID(jobDefID);
    // TODO: finish - go into XML
    getTransformation(Integer.parseInt(jobTransID)).getValue("outputs");
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

  public synchronized String getTransformationScript(int jobDefinitionID){
    // nothing for now
    return "";
  }

  public synchronized String [] getTransformationPackages(int jobDefID){
    String jobTransID = "";
    jobTransID = getTransformationID(jobDefID);
    // TODO: finish - go into XML
    getTransformation(Integer.parseInt(jobTransID)).getValue("uses");
    // nothing for now
    return new String [] {""};
  }

  public synchronized String [] getTransformationSignature(int jobDefID){
    String jobTransID = "";
    jobTransID = getTransformationID(jobDefID);
    // TODO: finish - go into XML
    getTransformation(Integer.parseInt(jobTransID)).getValue("uses");
    // nothing for now
    return new String [] {""};
  }

  public synchronized String getJobDefUser(int jobDefinitionID){
    int taskID = getJobDefTaskId(jobDefinitionID);
    return this.getTask(taskID).getValue("userName").toString();
  }

  public synchronized String getJobStatus(int jobDefinitionID){
    return getJobDefinition(jobDefinitionID).getValue("currentStatus").toString();
  }

  public synchronized String getJobDefName(int jobDefinitionID){
    return getJobDefinition(jobDefinitionID).getValue("jobName").toString();
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
    String jobTransID = "-1";    
    jobTransID = getJobDefinition(jobDefinitionID).getValue("JobTransFK").toString();
    return jobTransID;
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

  public synchronized boolean saveDefVals(int taskId, String[] defvals, String user){
    // nothing for now
    return false;
  }

  public synchronized DBResult select(String selectRequest, String identifier){
    
    String req = selectRequest;
    boolean withStar = false;
    int identifierColumn = -1;
    Pattern patt;
    Matcher matcher;

    // Oracle uses username.tablename.
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
      patt = Pattern.compile(" FROM (\\w+)", Pattern.CASE_INSENSITIVE);
      matcher = patt.matcher(req);
      req = matcher.replaceAll(", "+identifier+" FROM "+"$1");
    }
    
    patt = Pattern.compile("CONTAINS (\\w+)", Pattern.CASE_INSENSITIVE);
    matcher = patt.matcher(req);
    req = matcher.replaceAll("LIKE '%$1%'");
    
    patt = Pattern.compile("([<>=]) (\\w+)", Pattern.CASE_INSENSITIVE);
    matcher = patt.matcher(req);
    req = matcher.replaceAll("$1 '$2'");
    
    Debug.debug(">>> sql string was : "+req, 3);
    
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
  
  public void commit(){
    // nothing for now
    Debug.debug(">>> commiting ... done ", 2);
  }
  
  public synchronized DBRecord getTask(int taskID){
    
    String [] fields = getFieldNames(taskTable);
    
    DBRecord task = null;
    String req = "SELECT "+fields[0];
    if(fields.length>1){
      for(int i=1; i<fields.length; ++i){
        req += ", "+fields[i];
      }
    }
    req += " FROM "+taskTable;
    req += " WHERE "+taskIdentifier+" = '"+ taskID+"'";
    try{
      Debug.debug(">> "+req, 3);
      ResultSet rset = conn.createStatement().executeQuery(req);
      Vector taskVector = new Vector();
      String [] jt = new String[fields.length];
      while(rset.next()){
        String values[] = new String[fields.length];
        for(int i=0; i<fields.length;i++){
          if(fields[i].endsWith("FK") || fields[i].endsWith("ID") &&
              !fields[i].equalsIgnoreCase("grid") ||
              fields[i].endsWith("COUNT")){
            int tmp = rset.getInt(fields[i]);
            values[i] = Integer.toString(rset.getInt(fields[i]));
          }
          else{
            values[i] = rset.getString(fields[i]);
          }
          Debug.debug(fields[i]+"-->"+values[i], 2);
        }
        DBRecord jobd = new DBRecord(fields, values);
        taskVector.add(jobd);
      };
      rset.close();
      if(taskVector.size()==0){
        Debug.debug("ERROR: No task found for taskID "+taskID, 1);
      }
      else{
        task = ((DBRecord) taskVector.get(0));
      }
      if(taskVector.size()>1){
        Debug.debug("WARNING: More than one ("+rset.getRow()+") task found for taskID "+taskID, 1);
      }
    }catch(SQLException e){
      Debug.debug("WARNING: No jobTrans found for taskID "+taskID+". "+e.getMessage(), 1);
      return task;
    }
     return task;
  }
  

  public synchronized DBRecord getTransformation(int jobTransID){
    
    String [] fields = getFieldNames(jobTransTable);

    DBRecord jobTrans = null;
    String req = "SELECT "+fields[0];
    if(fields.length>1){
      for(int i=1; i<fields.length; ++i){
        req += ", "+fields[i];
      }
    }
    req += " FROM "+jobTransTable;
    req += " WHERE "+jobTransIdentifier+" = '"+ jobTransID+"'";
    try{
      Debug.debug(">> "+req, 3);
      ResultSet rset = conn.createStatement().executeQuery(req);
      Vector jobTransVector = new Vector();
      String [] jt = new String[fields.length];
      int i = 0;
      while(rset.next()){
        jt = new String[fields.length];
        for(int j=0; j<fields.length; ++j){
          try{
            jt[j] = rset.getString(j+1);
          }catch(Exception e){
            Debug.debug("Could not set value "+rset.getString(j+1)+" in "+
                fields[j]+". "+e.getMessage(),1);
          }
        }
        Debug.debug("Adding value "+jt[0], 3);
        jobTransVector.add(new DBRecord(fields, jt));
        Debug.debug("Added value "+((DBRecord) jobTransVector.get(i)).getAt(0), 3);
        ++i;
      }
      if(i==0){
        Debug.debug("ERROR: No task found for jobTransID "+jobTransID, 1);
      }
      else{
        jobTrans = ((DBRecord) jobTransVector.get(0));
      }
      if(i>1){
        Debug.debug("WARNING: More than one ("+rset.getRow()+") task found for jobTransID "+jobTransID, 1);
      }
    }catch(SQLException e){
      Debug.debug("WARNING: No jobTrans found for jobTransID "+jobTransID+". "+e.getMessage(), 1);
    }
     return jobTrans;
  }
  
  public synchronized DBRecord getRunInfo(int jobDefID){
    // TODO: implement
    return new DBRecord();
  }
  /*
   * Find JobTrans records belonging to the taskTrans of a given task
   */
  public synchronized DBRecord [] getJobTrans(int taskID){
    
    String [] fields = getFieldNames(jobTransTable);

    String taskTransFK = "-1";
    ResultSet rset;
    String req = "SELECT TASKTRANSFK FROM "+taskTable;
    if(taskID!=-1){
      req += " WHERE "+taskIdentifier+" = '"+ taskID+"'";
    }
    Debug.debug(req, 3);
    DBRecord [] allJobTrans = null;
    
    try{
      rset = conn.createStatement().executeQuery(req);
      
      while(rset.next()){
        taskTransFK=rset.getString("TASKTRANSFK");
      }
      if(taskTransFK == null ){
        Debug.debug("ERROR: No taskTransFK found for taskID "+taskID, 1);
        taskID = -1;
      }
      if(rset.getRow()>1){
        Debug.debug("WARNING: More than one ("+rset.getRow()+") taskTransFK found for taskID "+taskID, 1);
      }
      req = "SELECT "+fields[0];
      if(fields.length>1){
        for(int i=1; i<fields.length; ++i){
          req += ", "+fields[i];
        }
      }
      req += " FROM "+jobTransTable;
      // If no task was found, return all jobTrans records.
      if(taskID!=-1){
        req += " WHERE TASKTRANSFK = '"+taskTransFK+"'";
      }
      Debug.debug(req, 3);
      rset = conn.createStatement().executeQuery(req);
      //ResultSetMetaData md = rset.getMetaData();
      Vector jobTransVector = new Vector();
      String [] jt = new String[fields.length];
      int i = 0;
      while(rset.next()){
        jt = new String[fields.length];
        for(int j=0; j<fields.length; ++j){
          try{
            //((JobTrans) jobTransVector.get(i)).setValue(fields[j],rset.getString(j+1));
            jt[j] = rset.getString(j+1);
          }catch(Exception e){
            Debug.debug("Could not set value "+rset.getString(j+1)+" in "+
                fields[j]+". "+e.getMessage(),1);
          }
        }
        Debug.debug("Adding value "+jt[0], 3);
        //jobTransVector.add(new JobTrans(jt));
        jobTransVector.add(new DBRecord(fields, jt));
        Debug.debug("Added value "+((DBRecord) jobTransVector.get(i)).getAt(0), 3);
        ++i;
      }
      allJobTrans = new DBRecord[i];
      for(int j=0; j<i; ++j){
        allJobTrans[j] = ((DBRecord) jobTransVector.get(j));
        Debug.debug("Added value "+allJobTrans[j].getAt(0), 3);
      }
    }catch(SQLException e){
      Debug.debug("WARNING: No jobTrans found for taskID "+taskID+". "+e.getMessage(), 1);
    }
    return allJobTrans;
  }
  
  // Selects only the fields listed in fieldNames. Other fields are set to "".
  public synchronized DBRecord getJobDefinition(int jobDefinitionID){
    
    String [] fields = getFieldNames(jobDefTable);
    
    String req = "SELECT *";
    req += " FROM "+jobDefTable+" where "+jobDefIdentifier+" = '"+
    jobDefinitionID + "'";
    Vector jobdefv = new Vector();
    Debug.debug(req, 2);
    try{
    	Statement stmt = conn.createStatement();
    	ResultSet rset = stmt.executeQuery(req);
    	while(rset.next()){
    		String values[] = new String[fields.length];
    		for(int i=0; i<fields.length;i++){
    			String fieldname = fields[i];
    			String val = "";
          for(int j=0; j<fields.length; ++j){
            if(fieldname.equalsIgnoreCase(fields[j])){
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
    		DBRecord jobd = new DBRecord(fields, values);
			  jobdefv.add(jobd);
    	};
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
  public synchronized DBRecord [] selectJobDefinitions(int taskID, String [] fieldNames){
    
    String [] fields = getFieldNames(jobDefTable);

    String req = "SELECT";
    for(int i=0; i<fieldNames.length; ++i){
      if(i>0){
        req += ",";
      }
      req += " "+fieldNames[i];
    }
    req += " FROM "+jobDefTable+" where TASKFK = '"+
    taskID + "'";
    Vector jobdefv = new Vector();
    Debug.debug(req, 2);
    try {
    	Statement stmt = conn.createStatement();
    	ResultSet rset = stmt.executeQuery(req);
    	while(rset.next()){
    		String values[] = new String[fields.length];
    		for(int i=0; i<fields.length;i++){
    			String fieldname = fields[i];
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
    		DBRecord jobd = new DBRecord(fields, values);
  		  jobdefv.add(jobd);
  		
    	};
    	rset.close();
    
    }
    catch(Exception e){
      Debug.debug(e.getMessage(), 2);
    };
    DBRecord[] defs = new DBRecord[jobdefv.size()];
    for(int i=0; i<jobdefv.size(); i++) defs[i] = (DBRecord)jobdefv.get(i);
    jobdefv.removeAllElements();
    return defs;
  }
  
  //// FJOB PRODDB
  
  public int getJobDefTaskId(int jobDefID){
    String sql = "SELECT TASKFK FROM "+jobDefTable+" WHERE "+jobDefIdentifier+" = '"+
    jobDefID + "'";
    int taskid = 0;
    try {
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(sql);
      while(rset.next()){
        taskid = rset.getInt("TASKFK");
      }
      rset.close();
    }
    catch(Exception e){Debug.debug(e.getMessage(), 2);} 
    return taskid;
  }
  
  // In the case of proddb there is no unique name.
  // Only the distribution kit homePackage/implementation.
  // Still, we need a name to label a transformation the user can select,
  // so we add and extra field by hand.
  public synchronized DBResult getTransformations(int taskID){
    
    String [] fields = getFieldNames(jobTransTable);
    
    DBRecord jt [] = getJobTrans(taskID);
    DBResult res = new DBResult(fields.length+1, jt.length);

    System.arraycopy(fields, 0, res.fields, 0, fields.length);
            
    res.fields[fields.length] = "jobTransName";
    for(int i = 0; i<jt.length; ++i){
      Debug.debug("Copying over "+jt[i].values.length, 3);
      System.arraycopy(jt[i].values, 0, res.values[i], 0, fields.length);
      res.values[i][fields.length] = res.getValue(i, "homePackage")+":"+
         res.getValue(i, "implementation");
      Debug.debug("Setting jobTransName to "+res.values[i][fields.length], 3);
    }

    return res;
  }
  
  public synchronized DBResult getJobDefinitions(int taskID, String [] fieldNames){
    
    String [] fields = getFieldNames(jobDefTable);
    
    DBRecord jt [] = selectJobDefinitions(taskID, fieldNames);
    DBResult res = new DBResult(fieldNames.length, jt.length);
    
    System.arraycopy(
        fieldNames, 0,
        res.fields, 0, fieldNames.length);
            
    for(int i=0; i<jt.length; ++i){
      for(int j=0; j<fieldNames.length;j++){
        for(int k=0; k<fields.length;k++){
          if(fieldNames[j].equalsIgnoreCase(fields[k])){
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
    
    String [] fields = getFieldNames(jobDefTable);
    
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      return false;
    }

    String sql = "INSERT INTO "+jobDefTable+" (";
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
    //sql += ",'0'";
    sql += ")";
    Debug.debug(sql, 2);
    boolean execok = true;
    try {
    	Statement stmt = conn.createStatement();
    	stmt.executeUpdate(sql);
      //conn.commit();
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
    };
    return execok;
  };
  
  public synchronized boolean createRunInfo(JobInfo jobInfo){
    // TODO: implement
    return true;
  }
  
  public synchronized boolean createTask(String [] values){

    String [] fields = getFieldNames(taskTable);
    
    String sql = "INSERT INTO "+taskTable+" (";
    for(int i=1; i<fields.length; ++i){
      sql += fields[i];
      if(fields.length>0 && i<fields.length-1){
        sql += ",";
      }
    }
    sql += ") VALUES (";
    for(int i=1; i<fields.length; ++i){

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
      if(fields.length>0 && i<fields.length-1){
        sql += ",";
      }
    }
    sql += ")";
    Debug.debug(sql, 2);
    boolean execok = true;
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      //conn.commit();
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
    };
    return execok;
  };
  
  public synchronized boolean createTransformation(String [] values){

    String [] fields = getFieldNames(jobTransTable);

    String sql = "INSERT INTO "+jobTransTable+" (";
    for(int i = 1; i < fields.length; ++i){
      sql += fields[i];
      if(fields.length > 2 && i < fields.length - 1){
        sql += ",";
      }
    }
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
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      //conn.commit();
    }
    catch(Exception e){
      execok = false;
      Debug.debug(e.getMessage(), 2);
    };
    return execok;
  };
  
  public synchronized boolean setJobDefsField(int [] identifiers,
      String field, String value){
    String sql = "UPDATE "+jobDefTable+"  SET ";
    sql += field+"='"+value+"' WHERE ";
    // Not very elegant, but we need to use Identifier instead of
    // identifier, because identifier will only have been set if
    // a JobDefinition object has already been made, which may not
    // be the case.
    for(int i=0; i<identifiers.length; ++i){
      sql += jobDefIdentifier+"="+identifiers[i];
      if(identifiers.length > 1 && i < identifiers.length - 1){
        sql += " OR ";
      }
    }
    Debug.debug(sql, 2);
    boolean execok = true;
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      //conn.commit();
    }
    catch(Exception e){
      execok = false; Debug.debug(e.getMessage(), 2);
    };
    Debug.debug("update exec: "+execok, 2);
    return execok;
  };
  
  public synchronized boolean updateJobDefStatus(int jobDefID,
      String status){
    return updateJobDefinition(
        jobDefID,
        new String [] {"currentStatus"},
        new String [] {status}
        );
    // TODO: update in XML
  }
  
  public synchronized boolean updateJobDefinition(int jobDefID,
      String [] values){
    return updateJobDefinition(
        jobDefID,
        new String [] {"jobDefID", "jobName"/*, "stdOut", "stdErr"*/},
        new String [] {values[0], values[1]}
        );
    // TODO: update stdout and stderr in XML
  }
  
  public synchronized boolean updateJobDefinition(int jobDefID, String [] fields,
      String [] values){
    
    String [] defFields = getFieldNames(jobDefTable);
    
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      return false;
    }
    if(fields.length>defFields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+defFields.length, 1);
    }

    String sql = "UPDATE "+jobDefTable+"  SET ";
    int addedFields = 0;
    for(int i=0; i<defFields.length; ++i){
      if(!defFields[i].equals(jobDefIdentifier)){
        for(int j=0; j<fields.length; ++j){
          // only add if present in defFields
          if(defFields[i].equalsIgnoreCase(fields[j])){

            if(defFields[i].equalsIgnoreCase("creationTime") ||
                defFields[i].equalsIgnoreCase("modificationTime")){
              try{
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String dateString = null;
                if(defFields[i].equalsIgnoreCase("modificationTime")){
                  dateString = df.format(Calendar.getInstance().getTime());
                }
                if(defFields[i].equalsIgnoreCase("creationTime")){
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
    sql += " WHERE "+jobDefIdentifier+"="+jobDefID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try {
    	Statement stmt = conn.createStatement();
    	stmt.executeUpdate(sql);
    	//conn.commit();
    }
    catch(Exception e){
      execok = false; Debug.debug(e.getMessage(), 2);
    };
    Debug.debug("update exec: "+execok, 2);
    return execok;
  };
  
  public synchronized boolean updateRunInfo(JobInfo jobInfo){
    // TODO: implement
    return true;
  }
  
  public synchronized boolean updateTask(int taskID, String [] fields,
      String [] values){

    String [] defFields = getFieldNames(taskTable);

    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      return false;
    }
    if(fields.length>defFields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+defFields.length, 1);
    }

    String sql = "UPDATE "+taskTable+" SET ";
    int addedFields = 0;
    for(int i = 0; i < defFields.length; ++i){
      if(!defFields[i].equals(taskIdentifier)){
        for(int j=0; j<fields.length; ++j){
          // only add if present in defFields
          if(defFields[i].equalsIgnoreCase(fields[j])){
            
            if(defFields[i].equalsIgnoreCase("creationTime") ||
                defFields[i].equalsIgnoreCase("modificationTime")){
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
    sql += " WHERE "+taskIdentifier+"="+taskID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    }
    catch(Exception e){
      execok = false; Debug.debug(e.getMessage(), 2);
    };
    Debug.debug("update exec: "+execok, 2);
    return execok;
  };
  
  public synchronized boolean updateTransformation(int jobTransID, String [] fields,
      String [] values){
    
    String [] defFields = getFieldNames(jobTransTable);
    
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      return false;
    }
    if(fields.length>defFields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+defFields.length, 1);
    }

    String sql = "UPDATE "+jobTransTable+" SET ";
    int addedFields = 0;
    for(int i = 0; i<defFields.length; ++i){
      if(!defFields[i].equals(jobTransIdentifier)){
        for(int j=0; j<fields.length; ++j){
          // only add if present in defFields
          if(defFields[i].equalsIgnoreCase(fields[j])){
            
            if(defFields[i].equalsIgnoreCase("creationTime") ||
                  defFields[i].equalsIgnoreCase("modificationTime")){
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
    sql += " WHERE "+jobTransIdentifier+"="+jobTransID;
    Debug.debug(sql, 2);
    boolean execok = true;
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    }
    catch(Exception e){
      execok = false; Debug.debug(e.getMessage(), 2);
    };
    Debug.debug("update exec: "+execok, 2);
    return execok;
  };
  
  public synchronized boolean deleteJobDefinition(int jobDefId){
  	boolean ok = true;
  	try {
  		//String idstr = jobDef.jobDefinitionID;
  		//Integer jobid = Integer.valueOf(idstr);
  		String sql = "DELETE FROM "+jobDefTable+" WHERE "+jobDefIdentifier+" = '"+
      jobDefId+"'";
  		Statement stmt = conn.createStatement();
    	ResultSet rset = stmt.executeQuery(sql);
  	}
    catch(Exception e){ Debug.debug(e.getMessage(), 2); ok = false; }
    return ok;
    };
  
    public synchronized boolean deleteTask(int taskID){
      boolean ok = true;
      try {
        String sql = "DELETE FROM "+taskTable+" WHERE "+taskIdentifier+" = '"+
        taskID+"'";
        Statement stmt = conn.createStatement();
        ResultSet rset = stmt.executeQuery(sql);
      }
      catch(Exception e){ Debug.debug(e.getMessage(), 2); ok = false; }
      return ok;
      };
    
      public synchronized boolean deleteTransformation(int jobTransID){
        boolean ok = true;
        try {
          String sql = "DELETE FROM "+jobTransTable+" WHERE "+jobTransIdentifier+" = '"+
          jobTransID+"'";
          Statement stmt = conn.createStatement();
          ResultSet rset = stmt.executeQuery(sql);
        }
        catch(Exception e){ Debug.debug(e.getMessage(), 2); ok = false; }
        return ok;
        };
      
  public synchronized String [] getVersions(String homePackage){   
    String req = "SELECT "+jobTransIdentifier+", VERSION FROM "+
    jobTransTable+" WHERE HOMEPACKAGE = '"+homePackage+"'";
    Vector vec = new Vector();
    Debug.debug(req, 2);
    String version;
    try {
      Statement stmt = conn.createStatement();
      ResultSet rset = stmt.executeQuery(req);
      while(rset.next()){
        version = rset.getString("VERSION");
        if(version!=null){
          Debug.debug("Adding version "+version, 3);
          vec.add(version);
        }
        else{
          Debug.debug("WARNING: VERSION null for "+jobTransIdentifier+" "+
              rset.getInt(jobTransIdentifier), 1);
        }
      };
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
    
}
