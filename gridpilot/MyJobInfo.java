package gridpilot;

import gridfactory.common.JobInfo;

/**
 * Each instance of this class represents a submitted job.
 */
public class MyJobInfo extends JobInfo{

  private String cs = "";
  private String db = "";

  private String validationResult = "";
  /** List of files to be uploaded by the job itself. A nx2 array with src->dest mappings.
   * The use of this should be avoided as far as possible. */
  private String [][] uploadFiles = null;
 
  private String csStatus;
  private int tableRow = -1;
  private String [] depJobs = null;

  /**
   * These are the fields of the runtime DB table
   * The method getRunInfo of the db plugin must fill in values
   * for these fields as it best can, and return them as an array.
   
   public static String [] Fields= new String [] {
      "name", "identifier", "computingSystem",
      // worker node, filled in when checking
      "hostMachine",
      "userInfo", "db", "outTmp", "errTmp", "validationResult",
      // ----
      "jobId", "jobStatus", "newStatus", "internalStatus", "dbStatus",
      // gate keeper, filled in when submitting
      "host", "needUpdate"};
      
   */
  
  public static String Identifier = "jobId";
  
  /**
   * These are for fallback when loading jobs without CS information present.
   */
  public final static String CS_STATUS_WAIT = "wait";
  public final static String CS_STATUS_FAILED = "failed";
  public final static String CS_STATUS_DONE = "done";
  public final static String CS_STATUS_RUNNING = "running";
  
  public MyJobInfo(String _identifier, String _name, String _cs, String _db){
    
    super(_identifier, _name);
    
    cs = _cs;
    db = _db;

  }
  
  public MyJobInfo(String _identifier, String _name){
    super(_identifier, _name);
  }

  public String getCSName(){
    return cs;
  }
  public String getDBName(){
    return db;
  }

  public String [][] getUploadFiles(){
    return uploadFiles;
  }

  public String getValidationResult(){
    return validationResult;
  }

  public String getCSStatus(){
    return csStatus;
  }

  public void setCSName(String _computingSystem){
    cs = _computingSystem;
  }

  public void setDBName(String _database){
    db = _database;
  }

  public void setOutputs(String _stdOut, String _stdErr){
    setOutTmp(_stdOut);
    setErrTmp(_stdErr);
  }
  
  public void setUploadFiles(String [][] _files){
    uploadFiles = _files;
  }

  public void setValidationResult(String _result){
    validationResult = _result;
  }

  public void setCSStatus(String _csStatus){
    csStatus = _csStatus;
  }
  
  public String toString(){
    return "\nJob # " + this.getIdentifier()+ "\n" +
        "  Name \t: " + getName() + "\n" +
        "  cs \t: " + getCSName() + "\n" +
        "  JobId \t: " + getJobId() + "\n" +
        "  Host \t: " + getHost() + "\n" +
        "  Status DB \t: " + gridpilot.DBPluginMgr.getStatusName(getDBStatus()) + "\n" +
        "  Status \t: " + getStatus() + "\n" +
        "  Status internal \t: "+ getCSStatus() + "\n" +
        "  StdOut \t: " + getOutTmp() + "\n" +
        "  StdErr \t: " + getErrTmp() + "\n";
  }

  public int getTableRow(){
    return tableRow;
  }

  public void setTableRow(int _tableRow){
    tableRow = _tableRow;
  }
  
  /**
   * Return jobs on which this one depends.
   * @return a list of identifiers of jobs that this one depends on -
   *         i.e. that must have finished before this one can start
   */
  public String [] getDepJobs(){
    return depJobs;
  }

  public void setDepJobs(String [] _depJobs){
    depJobs = _depJobs;
  }

}


