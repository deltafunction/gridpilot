package gridpilot;

//import java.util.HashMap;

public interface Database{
  
  public class Task extends DBRecord{
    
    public String taskID ;
    public String taskName ;
    public String status ;
    public String taskTransFK ;
    public String actualPars ; // XML
    
    public static String [] Fields = new String [] {"taskID", "taskName", "status", "taskTransFK",
    "actualPars"};
    private String _identifier = "taskID";
    
    public Task(String _taskID, String _taskName, String _status, String _taskTransFK, String _actualPars) {
      taskID = _taskID;
      taskName = _taskName;
      status = _status;
      taskTransFK = _taskTransFK;
      actualPars = _actualPars;
      
      identifier = _identifier;
      fields = Fields;
      values = new String [] {taskID, taskName, status, taskTransFK,
          actualPars};

    }
    
    public Task(String [] _values) {
      
      taskID = _values[0] ;
      taskName = _values[1] ;
      status = _values[2] ;
      taskTransFK = _values[3] ;
      actualPars = _values[4] ;
      
      identifier = _identifier;
      fields = Fields;
      values = _values;
    }  
  }
  
  public class JobTrans extends DBRecord{
  
    public String jobTransID ;
    public String taskTransFK ;
    public String version ;
    public String uses ;
    public String implementation ;
    public String formalPars ; // XML
    public String homePackage ;
    //public static String [] fields;
    
    private static String _identifier = "jobTransID";

    public static String [] Fields =  new String [] {"jobTransID", "taskTransFK", "version",
       "uses", "implementation", "formalPars", "homePackage"};

    public JobTrans(String _jobTransID, String _taskTransFK, String _version,
        String _uses, String _implementation,
        String _formalPars, String _homePackage) {
      
      jobTransID = _jobTransID ;
      taskTransFK = _taskTransFK ;
      version = _version ;
      uses = _uses ;
      implementation = _implementation ;
      formalPars = _formalPars ;
      homePackage = _homePackage ;
      
      identifier = _identifier;
      fields = Fields;
      values = new String [] {jobTransID, taskTransFK, version, uses, implementation,
          formalPars,  homePackage};
      
    }
    
    public JobTrans(String [] _values) {
      
      jobTransID = _values[0] ;
      uses = _values[1] ;
      taskTransFK = _values[2] ;
      version = _values[3] ;
      implementation = _values[4] ;
      formalPars = _values[5] ;
      homePackage = _values[6] ;
      
      identifier = _identifier;
      fields = Fields;
      values = _values;
    }  
  }
  
  
  public class TaskTrans extends DBRecord{
  
    public String taskTransID ;
    public String taskTransName ;
    public String formalPars ; // XML
    public String documentation ;
    
    private static String _identifier = "jobTransID";

    public static String [] Fields =  new String [] {"taskTransID", "taskTransName", "formalPars",
    "documentation"};

    public TaskTrans(String _taskTransID, String _taskTransName,
        String _formalPars, String _documentation) {
      
      taskTransID = _taskTransID ;
      taskTransName = _taskTransName ;
      formalPars = _formalPars ;
      documentation = _documentation ;
      
      identifier = _identifier;
      fields = Fields;
      values = new String [] {_taskTransID, taskTransName, formalPars,
          documentation};
      
    }
    
    public TaskTrans(String [] _values) {
      
      taskTransID = _values[0] ;
      taskTransName = _values[1] ;
      formalPars = _values[2] ;
      documentation = _values[3] ;
      
      identifier = _identifier;
      fields = Fields;
      values = _values;
    }  
  }
  
  public class JobDefinition extends DBRecord{
    
    public String jobDefinitionID ;
    public String jobTransFK ;
    public String taskFK ;
    public String jobName ;
    public String jobXML ;
    
    //public static String [] fields;
    
    public static String Identifier = "jobDefinitionID";
    
    public static String [] Fields = new String []{
        "jobDefinitionID", "jobTransFK", "taskFK", "currentState", "maxAttempt",
        "jobName", "cpuCount", "cpuUnit", "ramCount", "ramUnit", "diskCount",
        "diskUnit", "ipConnectivity", "priority", "inputHint", "events", "jobXML"};
    
  
    public JobDefinition(String [] _values){
      
      jobDefinitionID = _values[0] ;
      jobTransFK = _values[1] ;
      taskFK = _values[2] ;
      jobName = _values[5] ;
      jobXML = _values[16] ;
      
      identifier = Identifier;      
      fields = Fields;
      values = _values;
    }

    //returns the values of fields in the order of fields
    public String[] getValues() {
      	return (String []) values;
      }
    
    public static boolean isNumericField(String s) {
    	if (s.endsWith("FK") || s.endsWith("ID") || s.equalsIgnoreCase("events")) return true;
    	if (s.equalsIgnoreCase("priority") || s.endsWith("Count") || s.startsWith("max") || s.startsWith("last")) return true;
    	return false;
    }
  }
  
  // TODO: shouldn't we make this more flexible?
  //public int createPart (int datasetID, String lfn, String partNr,
   //   String evMin, String evMax,
   //   String transID, String [] trpars,
    //  String [] [] ofmap, String odest, String edest);
  //public boolean deletePart(int partID);
  //public boolean dereservePart(int partID);
  //public boolean reservePart(int partID, String user /*user name recorded*/);
  //public boolean saveDefVals(int datasetId, String[] defvals);
  //public String [] getFieldNames(String table);
  //public DBResult select(String selectRequest, String identifier);
  //public String getDatasetTableName();
  //public String getPartitionTableName();
  //public DBResult getAllPartJobInfo(int datasetID);
  //public boolean updatePartition(int partID, HashMap attrVals);
  //public String getTransId(int datasetIdentifier, String version);
  //public String [] getTransformationVersions(int datasetIdentifier);
  //public String getPartTransValue (int partID, String key);
  //public String getPartValue (int partID, String key);
  //public String getPartOutLogicalName (int partID, String outpar);
  //public String getPartOutLocalName (int partID, String outpar);
  //public String getPackInitText (String pack, String cluster);
  //public String [] getOutputMapping(int transformationIdentifier, String version);
  //public String [] getJobParameters(int transformationIdentifier, String version);
  //public String [] getDefVals(int datasetIdentifier);
  //public String getDatasetName(int datasetID);
  // TODO: shouldn't we make this more flexible?
  //public boolean createRunRecord(int partID, String user, String cluster, String jobID,
  //    String jobName, String outTmp, String errTmp);
  
  public String connect();
  public void disconnect();
  public void clearCaches();
  // PRODDB ADDITIONS
  public DBResult getAllTaskTransRecords();
  public int getTaskTransId(int taskID);
  public DBRecord getTaskTransRecord(int taskID);
  public DBResult select(String selectRequest, String identifier);
  //

  public boolean createTask(String [] values);
  public boolean updateTask(int taskID, String [] fields, String [] values);
  public boolean deleteTask(int taskID);
  public DBRecord getTask(int taskID);

  public int getTaskId(int jobDefID);
  public DBResult getJobDefinitions(int taskID, String [] fieldNames);
  public DBRecord getJobDefinition(int jobDefinitionID);
  public boolean createJobDefinition(String [] values);
  public boolean updateJobDefinition(int jobDefID, String [] fields, String [] values);
  public DBRecord getRunInfo(int jobDefID);
  public boolean createRunInfo(JobInfo jobInfo);
  public boolean updateRunInfo(JobInfo jobInfo);
  public boolean setJobDefsField(int [] identifiers,
      String field, String value);
  public boolean deleteJobDefinition(int jobDefID);

  //public JobTrans [] getJobTrans(int taskID);
  public DBResult getJobTransRecords(int taskID);
  public DBRecord getJobTransRecord(int taskID);
  public boolean createJobTransRecord(String [] values);
  public boolean updateJobTransRecord(int jobTransID, String [] fields, String [] values);
  public boolean deleteJobTransRecord(int jobTransID);
  public String [] getVersions(String jobTransName);
  public String getUserLabel();
  public String getJobRunInfo(int jobDefID, String key);
  // Not yet used
  public boolean reserveJobDefinition(int jobDefinitionID, String UserName);
  public boolean dereserveJobDefinition(int jobDefinitionID);

  public boolean saveDefVals(int taskId, String[] defvals, String user);
  public String [] getDefVals(int taskId, String user);

  public String [] getJobParameters(int transformationID);
  public String getJobTransValue(int jobDefinitionID, String key);

  public String [] getOutputs(int transformationID);
  public String [] getInputs(int transformationID);
  /**
   * Get the value of the parameter par (from the signature of the transformation),
   * as set in the jobDefinition record.
   */
  public String getJobDefInRemoteName (int jobDefinitionID, String par);
  public String getJobDefInLocalName (int jobDefinitionID, String par);
  public String getJobDefOutRemoteName (int jobDefinitionID, String par);
  public String getJobDefOutLocalName (int jobDefinitionID, String par);

  public String getStdOutFinalDest(int jobDefinitionID);
  public String getStdErrFinalDest(int jobDefinitionID);
  
  public String [] getFieldNames(String table);
  public String getJobDefValue(int jobDefinitionID, String key);
  public String getPackInitText (String pack, String cluster);
  
  public class DBRecord {
    public String [] fields = null;
    public Object [] values = null;
    public static String identifier = null;
    public DBRecord(){
      fields = new String [] {""};
    }
    public DBRecord(String [] _fields, Object [] _values){
      fields = _fields;
      values = _values;
    }
    public Object getAt(int i){
      return values[i];  
    }
    public Object getValue(String col){
      for (int i = 0 ; i < fields.length ; i++) {
        if (col.equals(fields[i])) return values[i] ;
      }
      return "no such field "+col ;
    }
    public void setValue(String col, String val) throws Exception{
       for (int i = 0 ; i < fields.length ; i++) {
        if (col.equalsIgnoreCase(fields[i])){
          values[i] = val;
          //Debug.debug("Set field "+fields[i]+" to value "+values[i],3);
          // TODO: Should set field to value. Seems not to work
          //DBRecord.class.getField(col).set(this,val);
           return;
        }
      }
        throw new Exception("no such field "+col);
    }
  }
  
  
  public static class DBResult {
  
    public String[]    fields ;
    public String[][]  values ;
  
    public DBResult(int nrFields, int nrValues) {
      fields = new String [nrFields];
      values = new String [nrValues][nrFields];
    }
  
    public DBResult(String[] _fields, String[][] _values) {
      fields = _fields ;
      values = _values ;
    }
  
    public DBResult() {
      String [] f = {};
      String [] [] v = {};
      fields = f;
      values = v ;
    }
  
    public String getAt(int row, int column){
      if (row > values.length-1) return "no such row";
      if (column > values[0].length-1) return "no such column";
      return values[row][column];
    }

   public String getValue(int row, String col) {
      if (row > values.length-1) return "no such row";
      for (int i = 0 ; i < fields.length ; i++) {
        if (col.equalsIgnoreCase(fields[i])) return values[row][i] ;
      }
      return "no such field" ;
    }
  }
  public static final int DEFINED = 1;
  public static final int SUBMITTED = 2;
  public static final int VALIDATED = 3;
  public static final int UNDECIDED = 4;
  public static final int FAILED = 5;
  public static final int ABORTED = 6;
  public static final int UNEXPECTED = 7;
}