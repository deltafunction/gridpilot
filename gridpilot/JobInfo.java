package gridpilot;

import gridpilot.DBRecord;

/**
 * Each instance of this class represents a submitted job.
 */
public class JobInfo extends DBRecord{

  private String jobName = "";
  private String jobDefID = "-1";
  private String cs = "";
  private String user = "";
  private String db = "";
  private String outTmp = "";
  private String errTmp = "";
  private String validationResult = "";
  // List of files to be downloaded by the job itself.
  // The use of this should be avoided as far as possible.
  private String [] downloadFiles = null;
  // List of files to be uploaded by the job itself. A nx2 array with src->dest mappings.
  // The use of this should be avoided as far as possible.
  private String [][] uploadFiles = null;
 
  private String jobID="";
  private String jobStatus="";
  //private String newStatus="";
  private int internalStatus;
  private int dbStatus = gridpilot.DBPluginMgr.DEFINED;
  private String host="";
  private boolean needUpdate;
  private int tableRow = -1;
  /**
   * These are the fields of the runtime DB table
   * The method getRunInfo of the db plugin must fill in values
   * for these fields as it best can, and return them as an array.
   */
  /*public static String [] Fields= new String [] {
      "name", "identifier", "computingSystem",
      // worker node, filled in when checking
      "hostMachine",
      "userInfo", "db", "outTmp", "errTmp", "validationResult",
      // ----
      "jobId", "jobStatus", "newStatus", "internalStatus", "dbStatus",
      // gate keeper, filled in when submitting
      "host", "needUpdate"};*/
  
  public static String Identifier = "jobId";

  public JobInfo(String _jobDefID, String _jobName, String _cs, String _db){
    
    jobName = _jobName;
    jobDefID = _jobDefID;
    cs = _cs;
    db = _db;
    
    //fields = Fields;
    
    //setValues();
  }
  
  /*private void setValues(){
    values = new String [] {
        jobName, Integer.toString(jobDefID), cs, host, user, db, outTmp, errTmp,
        validationResult,
        jobID, jobStatus, newStatus, Integer.toString(internalStatus), Integer.toString(dbStatus),
        host, Boolean.toString(needUpdate)};
  }*/

  public JobInfo(){}

  /**
   * Properties
   */

  public String getName(){
    return jobName;
  }
  public String getJobDefId(){
    return jobDefID;
  }
  public String getCSName(){
    return cs;
  }
  public String getDBName(){
    return db;
  }

  public String getStdOut(){
    return outTmp;
  }
  
  public String getStdErr(){
    return errTmp;
  }
  
  public String [] getDownloadFiles(){
    return downloadFiles;
  }

  public String [][] getUploadFiles(){
    return uploadFiles;
  }

  public String getValidationResult(){
    return validationResult;
  }

  public String getJobId(){
    return jobID;
  }
  
  public String getJobStatus(){
    return jobStatus;
  }
  
  public String getHost(){
    return host;
  }
  
  public String getUser(){
    return user;
  }
  
  public boolean needToBeRefreshed(){
    return needUpdate;
  }

  public int getDBStatus(){
    return dbStatus;
  }

  public int getInternalStatus(){
    return internalStatus;
  }

  /**
   * Operations
   */

  public void setName(String _jobName){
    jobName = _jobName;
  }

  public void setJobDefId(String _jobDefId){
    jobDefID = _jobDefId;
    //setValues();
  }

  public void setCSName(String _computingSystem){
    cs = _computingSystem;
    //setValues();
  }

  public void setDBName(String _database){
    db = _database;
    //setValues();
  }

  public void setStdOut(String _stdOut){
    outTmp = _stdOut;
    //setValues();
  }
  
  public void setStdErr(String _stdErr){
    errTmp = _stdErr;
    //setValues();
  }

  public void setOutputs(String _stdOut, String _stdErr){
    outTmp = _stdOut;
    errTmp = _stdErr;
    //setValues();
  }
  
  public void setDownloadFiles(String [] _files){
    downloadFiles = _files;
    //setValues();
  }

  public void setUploadFiles(String [][] _files){
    uploadFiles = _files;
    //setValues();
  }

  public void setValidationResult(String _result){
    validationResult = _result;
    //setValues();
  }

  public void setJobId( String _jobID){
    jobID = _jobID;
    //setValues();
  }
  
  public void setJobStatus(String _jobStatus){
    jobStatus = _jobStatus;
    //setValues();
  }
  
  public void setHost(String _host){
    host = _host;
    //setValues();
  }

  public void setUser(String _user){
    user = _user;
    //setValues();
  }

  public void setDBStatus(int _dbStatus){
    dbStatus = _dbStatus;
    //setValues();
  }

  public void setInternalStatus(int _internalStatus){
    internalStatus = _internalStatus;
    //setValues();
  }

  public void setNeedToBeRefreshed(boolean _needUpdate){
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
        "  Status internal \t: "+ getInternalStatus() + "\n" +
        "  StdOut \t: " + getStdOut() + "\n" +
        "  StdErr \t: " + getStdErr() + "\n";
  }

  public int getTableRow(){
    return tableRow;
  }

  public void setTableRow(int _tableRow){
    tableRow = _tableRow;
  }
}


