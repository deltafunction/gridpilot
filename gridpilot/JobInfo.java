package gridpilot;

import gridpilot.Database.DBRecord;

/**
 * Each object of this represents a submitted job.
 *
 * - String Name
 * 	 Usually the partition logical file name ; used for choosing working directories
 * (&lt;plugin working directory&gt;/name.&lt;random&gt;)
 * - int PartId
 * 		Partition.identifier of this job/partition in DB database (used for all request in DBPluginMgr)
 * - int ComputingSystem
 * 		 id of the computingSystem which this job is running on
 * - String StdOut (StdErr)
 * 		 Standard output (error) of this job ; created by AtCom, has to be respected by the plugin
 * 		If the system doesn't allow the choice outputs, plugin has to move outputs in these destiniation
 * 		after the job end.
 * 		If StdErr is null, all outputs have to be written in StdOut
 * - String ValidationStdOut (ValidationStdErr)
 * 		 Outputs of the validation script ran on this job (at the end)
 * - String  JobId
 * 		 Id of this job (only if this job has been submitted), specific to
 * his computing system (an integer on LSF, https://... on European Datagrid, gsiftp://... on NorduGrid, ...
 * - String JobStatus
 * 		 Status of this job on his system
 * - int  AtComStatus
 * 		 Status used by AtCom (defined in ComputingSystem :STATUS_WAIT, STATUS_RUNNING,
 * 		STATUS_DONE, STATUS_ERROR, STATUS_FAILED)
 * - int DBStatus
 * 		 Status of this partition (defined in DBPluginMgr :DEFINED, SUBMITTED, UNDECIDED, VALIDATED, FAILED, ABORTED)
 * - String Host
 * 		 Host where this job is running
 * - int TableRow
 * 		 Row in monitoring table where this job is shown
 * - boolean NeedToBeRefreshed
 * 		 True if this job status can still possibly change
 * - boolean SkipValidation
 *     When job status is being checked, skip job validation
 *
 */

public class JobInfo extends DBRecord{

  private String jobName="";
  private int jobDefID=-1;
  private String jobDefDB="";
  private String cs="";
  private String user="";
  private String db="";
  private String outTmp="";
  private String errTmp="";
  private String outVal="";
  private String errVal="";
 
  private String jobID="";
  private String jobStatus="";
  private String newStatus="";
  private int atComStatus;
  private int dbStatus = gridpilot.DBPluginMgr.DEFINED;
  private String host="";
  private boolean needUpdate;
  private int tableRow = -1;
  public static String [] Fields= new String [] {
      /*These are the fields of the runtime DB table*/
      "jobName", "jobDefID", "cs", "user", "db", "outTmp", "errTmp",
      "outVal", "errVal",
      /*----*/
      "jobId", "jobStatus", "newStatus", "atComStatus", "dbStatus",
      "host", "needUpdate"};
  
  public static String Identifier = "jobId";

  public JobInfo(int _jobDefID, String _jobName, String _cs, String _db){
    
    jobName = _jobName;
    jobDefID = _jobDefID;
    cs = _cs;
    db = _db;
    
    fields = Fields;
    
    setValues();
  }
  
  private void setValues(){
    values = new String [] {
        jobName, Integer.toString(jobDefID), cs, user, db, outTmp, errTmp,
        outVal, errVal,
        jobID, jobStatus, newStatus, Integer.toString(atComStatus), Integer.toString(dbStatus),
        host, Boolean.toString(needUpdate)};
  }

  public JobInfo(){}

  /**
   * Properties
   */

  public String getName() {return jobName;}
  public int getJobDefId(){return jobDefID;}
  public String getCSName(){return cs;}
  public String getDBName(){return db;}

  public String getStdOut() {return outTmp;}
  public String getStdErr() {return errTmp;}
  public String getValidationStdOut(){ return outVal;}
  public String getValidationStdErr(){ return errVal;}


  public String getJobId(){ return jobID;}
  public String getJobStatus(){ return jobStatus;}
  public String getHost(){ return host;}
  public String getUser(){ return user;}
  
  public boolean needToBeRefreshed() { return needUpdate;}

  public int getDBStatus() {return dbStatus;}

  public int getAtComStatus(){ return atComStatus;}

  /**
   * Operations
   */

  void setName(String _jobName){
    jobName = _jobName;
  }

  void setJobDefId(int _jobDefId){
    jobDefID = _jobDefId;
    setValues();
  }

  void setCSName(String _computingSystem){
    cs = _computingSystem;
    setValues();
  }

  void setDBName(String _database){
    db = _database;
    setValues();
  }

  public void setStdOut(String _stdOut){
    outTmp = _stdOut;
    setValues();
  }
  public void setStdErr(String _stdErr){
    errTmp = _stdErr;
    setValues();
  }

  public void setOutputs(String _stdOut, String _stdErr){
    errTmp = _stdOut;
    errTmp = _stdErr;
    setValues();
  }

  void setValidationStdOut(String _validationStdout){
    outVal = _validationStdout;
    setValues();
  }
  void setValidationStdErr(String _validationStdErr){
    errVal = _validationStdErr;
    setValues();
  }

  void setValidationOutputs(String _validationStdOut, String _validationStdErr){
    outVal = _validationStdOut;
    errVal = _validationStdErr;
    setValues();
  }

  public void setJobId( String _jobID){
    jobID =  _jobID;
      setValues();
  }
  
  public void setJobStatus(String _jobStatus){
    jobStatus = _jobStatus;
    setValues();
  }
  
  public void setHost(String _host){
    host = _host;
    setValues();
  }

  public void setUser(String _user){
    user = _user;
    setValues();
  }

  void setDBStatus(int _dbStatus){
    dbStatus = _dbStatus;
    setValues();
  }

  public void setAtComStatus(int _atComStatus){
    atComStatus = _atComStatus;
    setValues();
  }

  void setNeedToBeRefreshed(boolean _needUpdate){
    needUpdate = _needUpdate;
  }

  public String toString(){
    return "\nJob # " + getJobDefId()+ "\n" +
        "  Name \t: " + getName() + "\n" +
        "  cs \t: " + getCSName() + "\n" +
        "  JobId \t: " + getJobId() + "\n" +
        "  Host \t: " + getHost() + "\n" +
        "  Status DB \t: " + gridpilot.DBPluginMgr.getStatusName(getDBStatus()) + "\n" +
        "  Status \t: " + getJobStatus() + "\n" +
        "  Status AtCom \t: "+ getAtComStatus() + "\n" +
        "  StdOut \t: " + getStdOut() + "\n" +
        "  StdErr \t: " + getStdErr() + "\n" +
        "  Val sdtOut \t: " + getValidationStdOut() +"\n"+
        "  Val stdErr \t: " + getValidationStdErr() + "\n" ;
  }

  public int getTableRow(){ return tableRow;}

  void setTableRow(int _tableRow){
    tableRow = _tableRow;
  }
}


