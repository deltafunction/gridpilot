package gridpilot;

import gridpilot.Database.DBRecord;

// TODO: harmonize with prodDB conventions

/**
 * Each object of this class symbolises a job associated to a partition.
 * Contains this information :
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
 * 		STATUS_DONE, STATUS_ERROR or STATUS_FAILED)
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

  public String name="uninitialised";
  public int partId=-1;

  public String csName="";
  public String dbName="";

  public String stdOut="";
  public String stdErr="";

  public String validationStdOut="";
  public String validationStdErr="";

  public String jobId="";
  public String jobStatus=""; //native

  public String newStatus="uninitialised";
  public int atComStatus;
  public int dbStatus = gridpilot.DBPluginMgr.DEFINED;

  public String host="";

  public String stepDB="";

  //private int tableRow = -1;

  //private boolean needUpdate;

  //private boolean skipValidation;

  public JobInfo(String _name, int _partId, String _computingSystem){
    name = _name;
    partId = _partId;
    csName = _computingSystem;
    
    fields = new String [] {"name", "partId", "csName", "dbName", "stdOut", "stdErr", "validationStdOut", "validationStdErr",
        "jobId", "jobStatus", "newStatus", "atComStatus", "dbStatus", "host", "stepDB"};
    setValues();
  }
  
  private void setValues(){
    values = new String [] {name, Integer.toString(partId), csName, dbName, stdOut, stdErr, validationStdOut, validationStdErr,
        jobId, jobStatus, newStatus, Integer.toString(atComStatus), Integer.toString(dbStatus),
        host, stepDB};
  }


  //where is this used ? job without name ??
  //public JobInfo(int _partId, String _computingSystem){
  //  partId = _partId;
   // csName = _computingSystem;
  //}

  public JobInfo(){}

  /**
   * Properties
   */

  public String getName() {return name;}
  public int getPartId(){return partId;}
  public String getCSName(){return csName;}
  public String getDBName(){return dbName;}

  public String getStdOut() {return stdOut;}
  public String getStdErr() {return stdErr;}
  public String getValidationStdOut(){ return validationStdOut;}
  public String getValidationStdErr(){ return validationStdErr;}


  public String getJobId(){ return jobId;}
  public String getJobStatus(){ return jobStatus;}
  public String getHost(){ return host;}


  public int getDBStatus() {return dbStatus;}

  public int getAtComStatus(){ return atComStatus;}

  //public int getTableRow(){ return tableRow;}

  public boolean needToBeRefreshed() { return true;} //dummy version this should be changed !!

  public boolean skipValidation() { return true;} //dummy version, validation will be separated anyway

  /**
   * Operations
   */

  void setName(String _name){
    name = _name;
  }

  void setPartId(int _partId){
    partId = _partId;
    setValues();
  }

  void setCSName(String _computingSystem){
    csName = _computingSystem;
    setValues();
  }

  public void setStdOut(String _stdOut){
    stdOut = _stdOut;
    setValues();
  }
  public void setStdErr(String _stdErr){
    stdErr = _stdErr;
    setValues();
  }

  public void setOutputs(String _stdOut, String _stdErr){
    stdOut = _stdOut;
    stdErr = _stdErr;
    setValues();
  }

  void setValidationStdOut(String _validationStdout){
    validationStdOut = _validationStdout;
    setValues();
  }
  void setValidationStdErr(String _validationStdErr){
    validationStdErr = _validationStdErr;
    setValues();
  }

  void setValidationOutputs(String _validationStdOut, String _validationStdErr){
    validationStdOut = _validationStdOut;
    validationStdErr = _validationStdErr;
    setValues();
  }

  public void setJobId( String _jobId){
      jobId =  _jobId;
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

  void setDBStatus(int _dbStatus){
    dbStatus = _dbStatus;
    setValues();
  }

  public void setAtComStatus(int _atComStatus){
    atComStatus = _atComStatus;
    setValues();
  }

  public void print(){
    System.out.println(toString());
  }

  public String toString(){
    return "\nJob n0 " + getPartId()+ "\n" +
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
      //  "  Row \t: " + getTableRow();
  }
}


