package gridpilot;

import java.util.HashMap;

public interface Database {
  
  public class Task extends DBRecord{
    
    public String taskID ;
    public String taskName ;
    public String status ;
    public String taskTransFK ;
    public String actualPars ; // XML
    
    private String [] _fields = new String [] {"taskID", "taskName", "status", "taskTransFK",
    "actualPars"};
    private String _identifier = "taskID";
    
    public Task(String _taskID, String _taskName, String _status, String _taskTransFK, String _actualPars) {
      taskID = _taskID;
      taskName = _taskName;
      status = _status;
      taskTransFK = _taskTransFK;
      actualPars = _actualPars;
      
      identifier = _identifier;
      fields = _fields;
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
      fields = _fields;
      values = _values;
    }  
  }
  
  public class JobTrans extends DBRecord{
  
    public String jobTransID ;
    public String uses ;
    public String implementation ;
    public String formalPars ; // XML
    public String homePackage ;
    //public static String [] fields;
    
    private static String _identifier = "jobTransID";

    public static String [] Fields =  new String [] {"jobTransID", "uses", "implementation", "formalPars",
    "homePackage"};

    public JobTrans(String _jobTransID, String _uses, String _implementation,
        String _formalPars, String _homePackage) {
      
      jobTransID = _jobTransID ;
      uses = _uses ;
      implementation = _implementation ;
      formalPars = _formalPars ;
      homePackage = _homePackage ;
      
      identifier = _identifier;
      fields = Fields;
      values = new String [] {jobTransID, uses, implementation, formalPars,
          homePackage};
      
    }
    
    public JobTrans(String [] _values) {
      
      jobTransID = _values[0] ;
      uses = _values[1] ;
      implementation = _values[2] ;
      formalPars = _values[3] ;
      homePackage = _values[4] ;
      
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
    public String currentState ;
    public String maxAttempt ;
    public String jobName ;
    public String cpuCount ;
    public String cpuUnit;
    public String ramCount ;
    public String ramUnit ;
    public String diskCount ;
    public String diskUnit ;
    public String ipConnectivity ;
    public String priority ;
    public String inputHint ;
    public String events ;
    public String jobXML ;
    
    //public static String [] fields;
    
    private static String _identifier = "jobDefinitionID";
    
    public static String [] Fields =  new String [] {"jobDefinitionID", "jobTransFK", "taskFK", "currentState", "maxAttempt",
        "jobName", "cpuCount", "cpuUnit", "ramCount", "ramUnit", "diskCount",
        "diskUnit", "ipConnectivity", "priority", "inputHint", "events", "jobXML"};
    
  
    public JobDefinition(String _jobDefinitionID, String _jobTransFK, String _taskFK, 
              String _currentState, String _maxAttempt, String _jobName,
              String _cpuCount, String _cpuUnit, String _ramCount, String _ramUnit,
              String _diskCount, String _diskUnit, String _ipConnectivity,   
              String _priority, String _inputHint, String _events,
              String _jobXML) {
      
      jobDefinitionID = _jobDefinitionID;
      jobTransFK =_jobTransFK ;
      taskFK =_taskFK ;
      currentState =_currentState ;
      maxAttempt =_maxAttempt ;
      jobName =_jobName ;
      cpuCount =_cpuCount ;
      cpuUnit =_cpuUnit ;
      ramCount =_ramCount ;
      ramUnit =_ramUnit ;
      diskCount =_diskCount ;
      diskUnit =_diskUnit ;
      ipConnectivity =_ipConnectivity ;
      priority =_priority ;
      inputHint =_inputHint ;
      events =_events ;
      jobXML =_jobXML ;
            
      identifier = _identifier;
      fields = Fields;
      values = new String [] {jobDefinitionID, jobTransFK, taskFK, currentState, maxAttempt,
          jobName, cpuCount, cpuUnit, ramCount, ramUnit, diskCount,
          diskUnit, ipConnectivity, priority, inputHint, events, jobXML};      
    }
    public JobDefinition(String [] _values) {
      
      jobDefinitionID = _values[0] ;
      jobTransFK = _values[1] ;
      taskFK = _values[2] ;
      currentState = _values[3] ;
      maxAttempt = _values[4] ;
      jobName = _values[5] ;
      cpuCount = _values[6] ;
      cpuUnit = _values[7] ;
      ramCount = _values[8] ;
      ramUnit = _values[9] ;
      diskCount = _values[10] ;
      diskUnit = _values[11] ;
      ipConnectivity = _values[12] ;
      priority = _values[13] ;
      inputHint = _values[14] ;
      events = _values[15] ;
      jobXML = _values[16] ;
      
      identifier = _identifier;      
      fields = Fields;
      values = _values;
    }
  }

  public String connect();
  public void disconnect();
  public void clearCaches();
  // TODO: shouldn't we make this more flexible?
  public int createPart (int datasetID, String lfn, String partNr,
      String evMin, String evMax,
      String transID, String [] trpars,
      String [] [] ofmap, String odest, String edest);
  public boolean deletePart(int partID);
  public boolean dereservePart(int partID);
  public boolean reservePart(int partID, String user /*user name recorded*/);
  public boolean saveDefVals(int datasetId, String[] defvals);
  public String [] getFieldNames(String table);
  public DBResult select(String selectRequest);
  public String getDatasetTableName();
  public String getPartitionTableName();
  public DBResult getAllPartJobInfo(int datasetID);
  public boolean updatePartition(int partID, HashMap attrVals);
  public String getTransId(int datasetIdentifier, String version);
  public String [] getTransformationVersions(int datasetIdentifier);
  public String getPartTransValue (int partID, String key);
  public String getPartValue (int partID, String key);
  public String getPartOutLogicalName (int partID, String outpar);
  public String getPartOutLocalName (int partID, String outpar);
  public String getPackInitText (String pack, String cluster);
  public String [] getOutputMapping(int datasetIdentifier, String version);
  public String [] getJobParameters(int datasetIdentifier, String version);
  public String [] getDefVals(int datasetIdentifier);
  public String getDatasetName(int datasetID);
  // TODO: shouldn't we make this more flexible?
  public boolean createRunRecord(int partID, String user, String cluster, String jobID,
      String jobName, String outTmp, String errTmp);
  
  // PRODDB ADDITIONS
  public int getTaskTransId(int taskID);
  //public JobTrans [] getJobTrans(int taskID);
  public DBResult getAllJobTransRecords(int taskID);
  public DBResult getAllJobDefinitions(int taskID);
  public int createJobDefinition(JobDefinition jobDef);
  public boolean updateJobDefinition(JobDefinition jobDef);
  public boolean deleteJobDefinition(JobDefinition jobDef);
  public DBRecord getTaskTransRecord(int taskID);
  
  public class DBRecord {
    public String [] fields = null;
    public String [] values = null;
    public String identifier = null;
    public DBRecord(){
      fields = new String [] {""};
    }
    public DBRecord(String [] _fields, String [] _values){
      fields = _fields;
      values = _values;
    }
    public String getAt(int i){
      return values[i];  
    }
    public String getValue(String col){
      for (int i = 0 ; i < fields.length ; i++) {
        if (col.equals(fields[i])) return values[i] ;
      }
      return "no such field "+col ;
    }
    public void setValue(String col, String val) throws Exception{
       for (int i = 0 ; i < fields.length ; i++) {
        if (col.equals(fields[i])){
          values[i] = val;
          //Debug.debug("Set field "+fields[i]+" to value "+values[i],3);
          // TODO: Should set field to value. Seems not to work
          //DBRecord.class.getField(col).set(this,val);
           return;
        }
      }
        throw new Exception("no such field "+col) ;
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
        if (col.equals(fields[i])) return values[row][i] ;
      }
      return "no such field" ;
    }
  }
  public static final int ABORTED = 6;
  public static final int DEFINED = 1;
  public static final int FAILED = 5;
  public static final int SUBMITTED = 2;
  public static final int UNDECIDED = 4;
  public static final int VALIDATED = 3;
}