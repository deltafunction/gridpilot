package gridpilot;

import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;

import java.awt.Color;
import java.util.*;

import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JProgressBar;

/**
 * This class manages the jobs from a single database.
 */
public class JobMgr{

  private DBPluginMgr dbPluginMgr;
  private MyLogFile logFile;
  
  // These are shared between all jobMgrs
  private MyJTable statusTable;
  private StatusBar statusBar;
  private StatisticsPanel statisticsPanel;
  /** Contains all jobs or logicalFiles (partitions) managed currently. <br>
   * "monitored" jobs are not submitted. <br>
   * This vector contains three types of JobInfo : <ul>
   * <li>the jobs which have been submitted,
   * <li>the jobs which have been created, but not yet submitted
   *     (they are in {@link SubmissionControl#toSubmitJobs})
   * <li>the logicalFile which have been monitored (not submitted) </ul>
   */
  private Vector submittedJobs;

  /** Index of column of icon in statusTable*/
  public final static int FIELD_CONTROL = 0;
  /** Index of column of JobName in statusTable*/
  public final static int FIELD_JOBNAME = 1;
  /** Index of column of JobId in statusTable*/
  public final static int FIELD_JOBID = 2;
  /** Index of column of JobStatus in statusTable*/
  public final static int FIELD_STATUS = 3;
  /** Index of column of Computing System name in statusTable*/
  public final static int FIELD_CS = 4;
  /** Index of column of host in statusTable*/
  public final static int FIELD_HOST = 5;
  /** Index of column of db in statusTable*/
  public final static int FIELD_DB = 6;
  /** Index of column of DB status in statusTable*/
  public final static int FIELD_DBSTATUS = 7;
  /** Index of column of DB reservedBy in statusTable*/
  public final static int FIELD_USER = 8;

  /** 
   * counters of jobs ordered by local status
   */
  private static int[] jobsByStatus = new int[DBPluginMgr.getStatusNames().length];
  
  /** 
   * counters of jobs ordered by DB status
   */
  private static int[] jobsByDBStatus = new int[DBPluginMgr.getDBStatusNames().length];
  
  private boolean [] hasChanged;
  private boolean useChanges = true;
  
  /*
  Constants to specify which jobs to display
  */
  public final static int DEFINED = 0;
  public final static int SUBMITTED = 1;
  public final static int VALIDATED = 2;
  public final static int UNDEFINED = 3;
  public final static int FAILED = 4;
  public final static int ABORTED = 5;
  // Name used for DB reservation and load
  public String dbName;

  public JobMgr(String _dbName) throws Exception{
    dbName = _dbName;
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    logFile = GridPilot.getClassMgr().getLogFile();
    statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
    statusTable = GridPilot.getClassMgr().getJobStatusTable();
    Debug.debug("Status table fields: "+statusTable.getModel().getColumnCount(), 3);
    statisticsPanel = GridPilot.getClassMgr().getJobStatisticsPanel();
    submittedJobs = GridPilot.getClassMgr().getSubmittedJobs();
    if(useChanges){
      hasChanged = new boolean[0];
    }
  }

   public int [] getJobsByStatus(){
     return jobsByStatus;
   }

   public int [] getJobsByDBStatus(){
     return jobsByDBStatus;
   }

   public DBPluginMgr getDBPluginMgr(){
    if(dbPluginMgr==null){
      Debug.debug("dbPluginMgr null", 3);
      new Exception().printStackTrace();
    }
    return dbPluginMgr;
  }
  
  //********************************************
  // methods from JobControl
  //********************************************

  /**
   * Adds selected jobDefinitions to the statusTable without submitting them. <p>
   * (From AtCom1)
   */
  public synchronized void addJobs(String [] selectedJobDefs){
    statusBar.setLabel("Adding job definitions...");
    JProgressBar pb = new JProgressBar(0, selectedJobDefs.length);
    statusBar.setProgressBar(pb);

    //monitored are not added
    for(int i=0; i<selectedJobDefs.length; ++i){
      pb.setValue(i);

      if(!exists(selectedJobDefs[i])){
        String jobName = dbPluginMgr.getJobDefName(selectedJobDefs[i]);
        MyJobInfo job = new MyJobInfo(selectedJobDefs[i], jobName);

        int dbStatus = DBPluginMgr.getStatusId(dbPluginMgr.getJobDefStatus(job.getIdentifier()));
        Debug.debug("Setting job db status :"+dbStatus+":", 3);
        job.setDBStatus(dbStatus);
        Debug.debug("Setting job DB :"+dbName+":", 3);
        job.setDBName(dbName);
        String jobUser = dbPluginMgr.getJobDefUserInfo(job.getIdentifier());
        Debug.debug("Setting job user :"+jobUser+":", 3);
        job.setUserInfo(jobUser);
        try{
          String jobID = dbPluginMgr.getRunInfo(job.getIdentifier(), "jobID");
          Debug.debug("Setting job ID :"+jobID+":", 3);
          job.setJobId(jobID);
        }
        catch(Exception e){
          Debug.debug(e.getCause().toString(), 2);
        }
        try{
          String jobCS = dbPluginMgr.getRunInfo(job.getIdentifier(), "computingSystem");
          Debug.debug("Setting job CS :"+jobCS+":", 3);
          job.setCSName(jobCS);
        }
        catch(Exception e){
          Debug.debug(e.getCause().toString(), 2);
        }
        try{
          String jobHost = dbPluginMgr.getRunInfo(job.getIdentifier(), "hostMachine");
          Debug.debug("Setting job host :"+jobHost+":", 3);
          job.setHost(jobHost);
        }
        catch(Exception e){
          Debug.debug(e.getCause().toString(), 2);
        }
        job.setTableRow(submittedJobs.size());
        job.setNeedsUpdate(true);
        String stdOut = null;
        String stdErr = null;
        switch(dbStatus){
          case DBPluginMgr.DEFINED:
          case DBPluginMgr.ABORTED:
          case DBPluginMgr.VALIDATED:
            Debug.debug(job.getName()+" is validated",3);
            job.setNeedsUpdate(false);
            //job.setCSName("");
            job.setOutputs(dbPluginMgr.getStdOutFinalDest(job.getIdentifier()),
                           dbPluginMgr.getStdErrFinalDest(job.getIdentifier()));
            break;
          case DBPluginMgr.FAILED:
          case DBPluginMgr.UNDECIDED:
            Debug.debug(job.getName()+" exited with state undecided",3);
            stdOut = dbPluginMgr.getRunInfo(job.getIdentifier(), "outTmp");
            stdErr = dbPluginMgr.getRunInfo(job.getIdentifier(), "errTmp");
            if(stdErr==null || stdErr.trim().length()==0 ||
               stdErr.equalsIgnoreCase("null")){
              stdErr = null;
            }
            job.setOutputs(stdOut, stdErr);
            job.setNeedsUpdate(false);
            // ! no break
          case DBPluginMgr.UNEXPECTED:
            Debug.debug(job.getName()+" ran with unexpected errors",3);
            stdOut = dbPluginMgr.getRunInfo(job.getIdentifier(), "outTmp");
            stdErr = dbPluginMgr.getRunInfo(job.getIdentifier(), "errTmp");
            if(stdErr==null || stdErr.trim().length()==0 ||
               stdErr.equalsIgnoreCase("null")){
              stdErr = null;
            }
            job.setOutputs(stdOut, stdErr);
            job.setNeedsUpdate(false);
            // ! no break
          case DBPluginMgr.SUBMITTED:
            stdOut = dbPluginMgr.getRunInfo(job.getIdentifier(), "outTmp");
            stdErr = dbPluginMgr.getRunInfo(job.getIdentifier(), "errTmp");
            if(stdErr==null || stdErr.trim().length()==0 ||
               stdErr.equalsIgnoreCase("null")){
              stdErr = null;
            }
            job.setOutputs(stdOut, stdErr);
            String jobId = dbPluginMgr.getRunInfo(job.getIdentifier(), "jobId");
            job.setJobId(jobId);
            String csName = dbPluginMgr.getRunInfo(job.getIdentifier(), "computingSystem");
            if (csName==null || csName.equals("")){
              logFile.addMessage("this job (" + job.getIdentifier() + ") doesn't have a CS defined");
              job.setNeedsUpdate(false);
            }
            job.setCSName(csName);
            break;
          default:
            logFile.addMessage("This status (" + dbStatus +
                               ") doesn't exist. " +
                               "The record of the job definition " +
                               job.getIdentifier() +
                               " seems to be corrupted");
            job.setNeedsUpdate(false);
            break;
        }
        submittedJobs.add(job);
        
        Debug.debug(job.getName()+" getNeedsUpdate: "+job.getNeedsUpdate(),3);

        if (job.getDBStatus()==DBPluginMgr.SUBMITTED){
          job.setCSStatus(MyJobInfo.CS_STATUS_WAIT);
          if(job.getJobId()==null || job.getJobId().trim().length()==0){
            logFile.addMessage("This job is SUBMITTED but doesn't have any job id\n" +
                               "It is set to UNDECIDED", job);
            job.setNeedsUpdate(false);
            initChanges();
            // TODO: is this necessary?
            statusTable.createRows(submittedJobs.size());
            updateDBStatus(job, DBPluginMgr.UNDECIDED);
          }
        }
        else{
          if(job.getDBStatus()!=DBPluginMgr.DEFINED){
            job.setCSStatus(MyJobInfo.CS_STATUS_DONE);
          }
        }
        //job.print();
        Debug.debug(job.getName()+" getNeedsUpdate: "+job.getNeedsUpdate(),3);
      }
    }

    statusBar.removeProgressBar(pb);
    statusBar.setLabel("Adding job definitions done.");

    statusTable.createRows(submittedJobs.size());
    initChanges();

    updateDBCells(submittedJobs);
    updateJobCells(submittedJobs);
    
    //updateJobsByStatus();
  }

  public void initChanges(){
    boolean [] oldChanges = hasChanged;
    hasChanged = new boolean[submittedJobs.size()];
    if(oldChanges!=null){
      for(int i=0; i<hasChanged.length && i<oldChanges.length; ++i){
        hasChanged[i] = oldChanges[i];
      }
    }
  }

  /**
   * Updates cell in status table which contains DB status of this specified job.
   * @see #updateDBCells(DBVector)
   * (from AtCom)
   */
  public void updateDBCell(MyJobInfo job){
    // Label with status Name

    JLabel status = new JLabel(DBPluginMgr.getStatusName(job.getDBStatus()));
    /*if(hasChanged[job.getTableRow()])
        status.setFont(new Font("Dialog", Font.BOLD, 12));
    else
        status.setFont(new Font("Dialog", Font.PLAIN, 12));*/

    // TODO: check what this has to do with GridPilot.jobColorMapping
    // label coloration
    switch(job.getDBStatus()){
      case DBPluginMgr.DEFINED :
        status.setForeground(Color.black);
        break;
      case DBPluginMgr.SUBMITTED :
        status.setForeground(Color.blue);
        break;

      case DBPluginMgr.UNDECIDED :
        status.setForeground(Color.orange);
        break;

      case DBPluginMgr.ABORTED :
        status.setForeground(Color.red);
        break;

      case DBPluginMgr.FAILED :
        status.setForeground(Color.magenta);
        break;

      case DBPluginMgr.UNEXPECTED :
        status.setForeground(Color.darkGray);
        break;

      case DBPluginMgr.VALIDATED :
        status.setForeground(Color.green);
        break;
    }

    statusTable.setValueAt(status, job.getTableRow(), FIELD_DBSTATUS);
    statusTable.updateSelection();
  }

  /**
   * Updates DB cell for all specified jobs.
   * @see #updateDBCell(MyJobInfo)
   * (From AtCom)
   */
  public void updateDBCells(Vector jobs){
    // this works fine, except that the setTable causes the right-click menu to be lost...
    /*Object [][] values = new Object[jobs.size()][GridPilot.jobStatusFields.length];
    for(int i=0; i<jobs.size(); ++i){
      values[i][FIELD_JOBNAME] = ((JobInfo) jobs.get(i)).getName();
      values[i][FIELD_JOBID] = ((JobInfo) jobs.get(i)).getJobId();
      values[i][FIELD_CS] = ((JobInfo) jobs.get(i)).getCSName();
      values[i][FIELD_DB] = ((JobInfo) jobs.get(i)).getDBName();
      values[i][FIELD_USER] = ((JobInfo) jobs.get(i)).getUser();
      values[i][FIELD_STATUS] = ((JobInfo) jobs.get(i)).getJobStatus();
      values[i][FIELD_HOST] = ((JobInfo) jobs.get(i)).getHost();
      values[i][FIELD_DBSTATUS] = DBPluginMgr.getStatusName(((JobInfo) jobs.get(i)).getDBStatus());
      Debug.debug("Setting values "+
          Util.arrayToString(GridPilot.jobStatusFields)+":"+values[i].length+"-->"+Util.arrayToString(values[i]), 3);
    }
    statusTable.setTable(values, GridPilot.jobStatusFields);
    */

    Enumeration e = jobs.elements();
    while(e.hasMoreElements()){
      updateDBCell((MyJobInfo)e.nextElement());
    }
    //updateJobsByStatus();

  }

  /**
   * Updates cells for all specified jobs.
   * @see #updateJobCell(MyJobInfo)
   * (From AtCom)
   */
  public void updateJobCells(Vector jobs){
    Enumeration e = jobs.elements();
    while(e.hasMoreElements()){
      updateJobCell((MyJobInfo)e.nextElement());
    }
  }

  /**
   * Updates all fields (except DB) for this specified job.
   * (From AtCom)
   */
  private void updateJobCell(MyJobInfo job){
    int row = job.getTableRow();
    statusTable.setValueAt(job.getName(), row, FIELD_JOBNAME);
    statusTable.setValueAt(job.getJobId(), row, FIELD_JOBID);
    if(job.getCSName()!=null){
      statusTable.setValueAt(job.getCSName(), row, FIELD_CS);
    }
    statusTable.setValueAt(job.getDBName(), row, FIELD_DB);
    statusTable.setValueAt(job.getUserInfo(), row, FIELD_USER);
    statusTable.setValueAt(job.getCSStatus(), row, FIELD_STATUS);
    statusTable.setValueAt(job.getHost(), row, FIELD_HOST);
  }

  // (From AtCom)
  void updateJobsByStatus(){
    // jobsByDBStatus
    for(int i=0; i<jobsByDBStatus.length;++i){
          jobsByDBStatus[i] = 0;
    }
    // jobsByStatus
    for(int i=0; i<jobsByStatus.length;++i){
      jobsByStatus[i] = 0;
    }
    int waitIndex = 0;
    int runIndex = 1;
    int doneIndex = 2;

    for(int i=0; i<submittedJobs.size(); ++i){
      Debug.debug("adding job "+((MyJobInfo) submittedJobs.get(i)).getName()+
          " : "+((MyJobInfo) submittedJobs.get(i)).getDBStatus()+
          " : "+jobsByDBStatus[((MyJobInfo) submittedJobs.get(i)).getDBStatus()-1], 3);
      ++jobsByDBStatus[((MyJobInfo) submittedJobs.get(i)).getDBStatus()-1];

      switch(((MyJobInfo) submittedJobs.get(i)).getStatus()){
        case MyJobInfo.STATUS_READY:
          ++jobsByStatus[waitIndex];
          break;

        case MyJobInfo.STATUS_RUNNING:
          ++jobsByStatus[runIndex];
          break;

        case MyJobInfo.STATUS_DONE:
          ++jobsByStatus[doneIndex];
          break;

        case MyJobInfo.STATUS_FAILED:
          ++jobsByStatus[doneIndex];
          break;
      }
    }
    statisticsPanel.update();
  }

  public void resetChanges(){
    if(hasChanged==null){
      return;
    }
    for(int i=0; i<hasChanged.length ; ++i){
      hasChanged[i] = false;
    }
    updateDBCells(submittedJobs);
  }

  /**
   * Checks if there is already a job with the specified jobDefId in the monitored jobs. <p>
   * (From AtCom)
   */
  public boolean exists(String jobDefId){
    MyJobInfo job;
    Debug.debug("Checking jobs: "+submittedJobs.size(), 3);
    Iterator i = submittedJobs.iterator();
    while(i.hasNext()){
      job = ((MyJobInfo) i.next());
      if(job.getIdentifier()==jobDefId){
        return true;
      }
    }
    return false;
  }

  /**
   * Removes row according to job name.
   * (from AtCom1)
   */
  public void removeRow(String jobDefID){
    String lfn = dbPluginMgr.getJobDefinition(jobDefID).getValue("name").toString();
    // Remove jobs from status vectors
    for(int i=0; i<submittedJobs.size(); ++i){
      if(((MyJobInfo) submittedJobs.get(i)).getName().equals(lfn)){
        --jobsByDBStatus[((MyJobInfo) submittedJobs.get(i)).getDBStatus()-1];
        submittedJobs.remove(i);
      }
    }
    for(int i=0; i<statusTable.getRowCount(); ++i){
      // "Job Name" is the second column, match it against the found job name
      Debug.debug("Checking monitored row "+statusTable.getUnsortedValueAt(i, 1)+
          " : "+lfn, 3);
      if(statusTable.getUnsortedValueAt(i, 1).equals(lfn)){
        statusTable.removeRow(i);
      }
    }
    updateJobsByStatus();
    statusTable.updateUI();
  }

  /**
   * Returns a String which contains some information about the job at the specified row. <p>
   * (from AtCom1)
   */
  public static String getJobInformation(int row){
    MyJobInfo job = getJobAtRow(row);

    return "  Name \t: " + job.getName() + "\n" +
        "  Job definition ID \t: " + job.getIdentifier() + "\n" +
        "  CS \t: " + job.getCSName() + "\n" +
        "  Job ID \t: " + job.getJobId() + "\n" +
        "  Host \t: " + job.getHost() + "\n" +
        "  Status DB \t: " + DBPluginMgr.getStatusName(job.getDBStatus()) + "\n" +
        "  Status \t: " + job.getStatus() + "\n" +
        "  Status GridPilot \t: " + MyJobInfo.getStatusName(job.getStatus()) + "\n" +
        "  StdOut \t: " + job.getOutTmp() + "\n" +
        "  StdErr \t: " + job.getErrTmp() + "\n" +
        (job.getDownloadFiles()==null?"":"  Download files \t: " + MyUtil.arrayToString(job.getDownloadFiles()) + "\n") +
        (job.getUploadFiles()==null?"":"  Upload files \t: " + MyUtil.arrayToString(job.getUploadFiles()) + "\n") +
        "  Row \t: " + job.getTableRow() + "\n" +
        "  Updatable \t: " + job.getNeedsUpdate() ;
  }

  /**
   * Checks if the final destinations of the job at the specified job is known. <br>
   * If it isn't, download them from the DB.
   * (from AtCom1)
   */
  /*public void setFinalDest(int row){
    JobInfo job = getJobAtRow(row);
    if((job.getStdOut()==null || job.getStdOut().equals("")) &&
       (job.getStdErr()==null || job.getStdErr().equals(""))){
      job.setOutputs(dbPluginMgr.getStdOutFinalDest(job.getIdentifier()),
                     dbPluginMgr.getStdErrFinalDest(job.getIdentifier()));
    }
  }*/

  /**********************************************************
   * update, end of job, decision methods and functions
   * (from AtCom)
   **********************************************************/

  /**
   * Called when user has chosen DB status for some Undecided jobs.
   */
  /*public void undecidedChoices(DBVector jobs, int[] choices){
    for(int i = 0; i < jobs.size(); ++i){
      JobInfo job = (JobInfo) jobs.getDBRecord(i);
      if(job.getDBStatus()!=choices[i])
        updateDBStatus(job, choices[i]);
    }
  }*/

  /**
   * Forces revalidation of some jobs.
   * When this function returns, all job are in the validation queue, but are not
   * always already validated.
   */
  public static void revalidate(final int[] rows){
    JobValidation jobValidation = GridPilot.getClassMgr().getJobValidation();
    if(jobValidation==null){
      jobValidation = new JobValidation();
      GridPilot.getClassMgr().setJobValidation(jobValidation);
    }
    for(int i = 0; i<rows.length; ++i)
      GridPilot.getClassMgr().getJobValidation().validate(getJobAtRow(rows[i]));
  }

  /**
   * Called when a plugin indicates that this job failed.
   * In this case, validation is never done.
   */
  public void jobFailure(MyJobInfo job){
    updateDBStatus(job, DBPluginMgr.FAILED);
    if(job.getDBStatus()!=DBPluginMgr.FAILED){
      logFile.addMessage("Update DB status failed after job Failure ; " +
                         " this job is put back updatable", job);
      job.setNeedsUpdate(true);
    }
    else{
      // Resubmit job if value of resubmit spinner is larger than job.getResubmitCount().
      Integer resubmit = (Integer) GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.jobMonitor.sAutoResubmit.getValue();
      int resubmitNr = resubmit.intValue();
      Debug.debug("Checking if job should be resubmitted, "+job.getResubmitCount()+":"+resubmitNr, 2);
      if(job.getResubmitCount()>-1 && job.getResubmitCount()<resubmitNr){
        job.incrementResubmitCount();
        logFile.addInfo("Auto-resubmitting job "+job.getIdentifier()+" : "+job.getResubmitCount()+":"+resubmitNr);
        // TODO: consider doing this in a thread...
        SubmissionControl submissionControl = GridPilot.getClassMgr().getSubmissionControl();
        Vector jobVector = new Vector();
        jobVector.add(job);
        submissionControl.resubmit(jobVector);
      }
    }
  }

  /********************************************************
   * Requests about jobs
   ********************************************************/

  /**
   * Returns the submitted job with the specified jobDefinition.identifier.
   */
  public static MyJobInfo getJob(String jobDefID){
    Vector submJobs = GridPilot.getClassMgr().getSubmittedJobs();
    Enumeration e = submJobs.elements();
    MyJobInfo job = null;
    while(e.hasMoreElements()){
      job = (MyJobInfo) e.nextElement();
      if(job.getIdentifier().equalsIgnoreCase(jobDefID)){
        return job;
      }
    }
    Debug.debug("No submitted job found with ID "+jobDefID, 2);
    return null;
  }

  /**
   * Returns the job at the specified row in the statusTable
   * @see #getJobsAtRows(int[])
   */
  public static MyJobInfo getJobAtRow(int row){
    Vector submJobs = GridPilot.getClassMgr().getSubmittedJobs();
    //Debug.debug("Got jobs at row "+row+". "+submJobs.size(), 3);
    return (MyJobInfo) submJobs.get(row);
  }

  /**
   * Returns the jobs at the specified rows in statusTable
   * @see #getJobAtRow(int)
   */
  public static Vector getJobsAtRows(int[] row){
    Vector jobs = new Vector(row.length);
    for(int i=0; i<row.length; ++i){
      jobs.add(getJobAtRow(row[i]));
    }
    return jobs;
  }

  /**
   * Returns the number of job in statusTable;
   */
  //public int getJobCount(JobMgr jobMgr){
  //  return submittedJobs.size();
  //}

  /**
   * Checks if all jobs at these specified rows are killable. <p>
   * @see #isKillable(MyJobInfo)
   */
  public static boolean areKillable(int[] rows){
    for(int i = 0; i < rows.length; ++i){
      MyJobInfo job = getJobAtRow(rows[i]);
      if(!isKillable(job))
        return false;
    }
    return true;
  }

  /**
   * Checks if this specified job is killable.
   * A Killable job is a job which is Submitted, and have a job Id!=null.
   * Called by :
   * <li>{@link #areKillables(int[])}
   * <li>{@link #killJob(MyJobInfo)}
   * @see #areKillables(int[])
   */
  private static boolean isKillable(MyJobInfo job){
    return job.getDBStatus()==DBPluginMgr.SUBMITTED && job.getJobId()!=null;
  }

  /**
   * Checks if all jobs at these specified rows are killable. <p>
   * Called by {@link MonitoringPanel#selectionEvent(javax.swing.event.ListSelectionEvent)} <p>
   * @see #isKillable(MyJobInfo)
   */
  public static boolean areKillables(int[] rows) {
    for (int i = 0; i < rows.length; ++i) {
      MyJobInfo job = getJobAtRow(rows[i]);
      if (!isKillable(job))
        return false;
    }
    return true;
  }
  
  /**
   * Checks if all jobs at these specified rows are Undecided.
   */
  public static boolean areDecidables(int[] rows){
    for(int i = 0; i < rows.length; ++i){
      if(getJobAtRow(rows[i]).getDBStatus()!=DBPluginMgr.UNDECIDED)
        return false;
    }
    return true;
  }

  /**
   * Checks if all jobs at these specified rows are re-submitables.
   * A job is resubmitable is its DB status is Failed, and if it has a
   * attributed computing system.
   */
  public static boolean areResumbitables(int[] rows){
    for(int i = 0; i < rows.length; ++i){
      MyJobInfo job = getJobAtRow(rows[i]);
      if(job.getDBStatus()!=DBPluginMgr.FAILED || job.getCSName().equals(""))
        return false;
    }
    return true;
  }

  /**
   * Checks if all jobs at these specified rows are submitables.
   * A job is submitable iff its DB status is Defined.
   *
   */
  public static boolean areSubmitables(int[] rows){
    for(int i = 0; i < rows.length; ++i){
      if(getJobAtRow(rows[i]).getDBStatus()!=DBPluginMgr.DEFINED)
        return false;
    }
    return true;
  }

  /**
   * Checks if the job at the specified row is currently running.
   */
  public static boolean isRunning(int row){
    return isRunning(getJobAtRow(row));
  }

  /**
   * Checks if the specified job is running.
   * A job is running iff its DB status is Submitted.
   */
  public static boolean isRunning(MyJobInfo job){
    return job.getDBStatus()==DBPluginMgr.SUBMITTED && job.getJobId()!=null;
  }

  /********************************************************************
   * Status (DB, AtCom, ...) methods & functions
   * (from AtCom1)
   ********************************************************************/

  /**
   * Allows the user to force DB status. <p>
   */
  public void setDBStatus(int[] rows, int dbStatus){
    JProgressBar pb = new JProgressBar(0, rows.length);
    statusBar.setProgressBar(pb);
    statusBar.setLabel("Setting DB status (" + DBPluginMgr.getStatusName(dbStatus) +
                       ") ...");
    int choice = 3;
    boolean skipAll = false;
    boolean skip = false;
    boolean showThis = true;
   
    for (int i=0; i<rows.length; ++i){
      if(skipAll){
        break;
      }
      pb.setValue(i);
      MyJobInfo job = getJobAtRow(rows[i]);
      String authorization = isTransitionAllowed(job.getDBStatus(), dbStatus);
      
      if(authorization!=null){
        if(showThis){
          ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()); 
          try{
            String confirmString = "WARNING: The transition from " +
            DBPluginMgr.getStatusName(job.getDBStatus()) + " to " +
            DBPluginMgr.getStatusName(dbStatus) + " is not allowed.\n"+
            authorization+".\nContinue on your own risk.";
            if(rows.length-i>1){
              choice = confirmBox.getConfirm("Confirm change status",
                  confirmString,
                 new Object[] {"OK", "Skip", "OK for all", "Skip all"});
            }
            else{
              choice = confirmBox.getConfirm("Confirm change status",
                  confirmString,
               new Object[] {"OK",  "Skip"});        
            }
          }
          catch(java.lang.Exception e){
            Debug.debug("Could not get confirmation, "+e.getMessage(), 1);
          }
          switch(choice){
            case 0  : skip = false;  break;  // OK
            case 1  : skip = true;   break; // Skip
            case 2  : skip = false;  showThis = false ; break;   //OK for all
            case 3  : skip = true;   showThis = false ; skipAll = true; break;// Skip all
            default : skip = true;   skipAll = true; break;// other (closing the dialog). Same action as "Skip all"
          }   
        }
        if(!skip && skipAll!=true){
          // go ahead
        }
        else if(skip && skipAll!=true){
          continue;
        }
        else if(skipAll==true){
          break;
        }
      }
      updateDBStatus(job, dbStatus);
      if (isRunning(job) && job.getCSName()!=null && !job.getCSName().equals("")){
        job.setNeedsUpdate(true);
      }
      else{
        job.setNeedsUpdate(false);
      }
    }
    statusBar.removeProgressBar(pb);
    statusBar.setLabel("Setting DB status done");
  }

  /**
   * Tells if a transiton between DB status is allowed. <p>
   * Currently, this method is only called with toStatus in {Defined, Aborted, Failed}.
   * @return <code>null</code> if the transition from the DB status <code>fromStatus</code>
   * to <code>toStatus</code> is allowed, and a String which explains why if this
   * transition is not allowed. <p>
   *
   */
  private static String isTransitionAllowed(int fromStatus, int toStatus) {

    String sameStatus = "Transitions between the same status are not allowed";

    String fromDefined = "Defined logical files cannot be set manually into other status than Aborted";
    String fromValidated = "A Validated logical file can only be put in Defined or Aborted";
    String fromAborted = "An Aborted logical file has to be put in Defined before resubmission";
    String fromSubmitted = "Kill this job and wait until GridPilot detects its end, " +
        "otherwise ghost job will overwrite new attempt";

    String toValidated = "A logical file can never be put in Validated manualy";
    String toUndecided = "A logical file can never be put in Undecided manualy";

    String undToSub = "Set it Failed before, and resubmit it";
    String failToSub = "Resubmit it";
    String undToDef = "Set it Failed before";

    String allowedTransition [][] = {
        /*   from\to    DEFINED        SUBMITTED      VALIDATED      UNDECIDED      FAILED         ABORTED*/
        /*DEFINED*/    {sameStatus,    fromDefined,   fromDefined,   fromDefined,   fromDefined,   null},
        /*SUBMITTED*/  {fromSubmitted, sameStatus,    toValidated,   toUndecided,   fromSubmitted, fromSubmitted},
        /*VALIDATED*/  {null,          fromValidated, sameStatus,    fromValidated, fromValidated, null},
        /*UNDECIDED*/  {undToDef,      undToSub,      toValidated,   sameStatus,    null,          null},
        /*FAILED*/     {null,          failToSub,     toValidated,   toUndecided,   sameStatus,    null},
        /*ABORTED*/    {null,          fromAborted,   fromAborted,   fromAborted,   fromAborted,   sameStatus}};

    return allowedTransition[fromStatus -1][toStatus -1];
  }


  /**
   * Called each time the DB status of the specified job changes (except at submission). <br>
   * In all cases, DB is updated ; <br>
   * <ul>
   * <li>Defined : dereservation
   * <li>Submitted : needToBeRefreshed <- true
   * <li>Undecided : -
   * <li>Aborted : clear output mapping, dereservation
   * <li>Failed : clear output mapping
   * <li>Validated : post processing, update in Magda, dereservation
   * </ul>
   * <p>
   *
   * This function can be called by : <ul>
   * <li>Validation : <ul>
   *    <li>new status in {Validated, Undecided, Failed}
   *    <li>previous status = Submitted </ul>
   *
   * <li>Decide (button) : <ul>
   *    <li>new status in {Validated, Failed, Aborted}. (Not Undecided)
   *    <li>previous status = Undecided </ul>
   * <li>Job Failure : <ul>
   *    <li>new status = Failed
   *    <li>previous status = Submitted </ul>
   * <li>Set Status : <ul>
   *    <li>new status in {Aborted, Defined, Failed}
   *    <li>previous status in {Aborted, Failed, Undecided} </ul>
   * <li>Resubmit : <ul>
   *    <li>new status = Submitted
   *    <li>previous status = Failed </ul>
   * </ul>
   *
   * <p>
   * This, if the status is *, the caller is : <ul>
   * <li>Defined : Set status (from V,A,F)
   * <li>Submitted : Resubmit (from F)
   * <li>Undecided : Validation (from S)
   * <li>Aborted : Decide (from U), Set status (from D,V,S,F)
   * <li>Failed : Decided (from U), Job failure (from S), Set status (from U),
   *    Validation (from S)
   * <li>Validated : Validation (from S), Decide (from U)
   * </ul>
   *
   * (from AtCom)
   */
  public boolean updateDBStatus(MyJobInfo job, int dbStatusNr) {
    // dbPluginMgr.updateDBStatus should be done before dereservation (otherwise you
    // could have integrity problems), but should be done after post processing
    // in case of 'Validate' (in order to avoid double access to DB)
    
    boolean succes = true;
    String dbStatus = DBPluginMgr.getStatusName(dbStatusNr);

    if (job.getDBStatus()==dbStatusNr){
      Debug.debug("job.getDBStatus()==dbStatus", 3);
      return true;
    }
    initChanges();
    //if(hasChanged.length > job.getTableRow())
    Debug.debug("updating "+job.getTableRow()+":"+
        hasChanged.length, 3);
    hasChanged[job.getTableRow()] = true;

    int previousStatus = job.getDBStatus();

    switch(dbStatusNr){
      case DBPluginMgr.DEFINED:
        /** Previous : V, A, F*/

        if(dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "status",
            dbStatus)){
          job.setDBStatus(dbStatusNr);

          /** if job still reserved -> dereserve */
          if(previousStatus==DBPluginMgr.FAILED ||
              previousStatus==DBPluginMgr.UNEXPECTED  ||
              previousStatus==DBPluginMgr.ABORTED ||
              previousStatus==DBPluginMgr.UNDECIDED ||
              previousStatus==DBPluginMgr.VALIDATED){
            if(!dbPluginMgr.cleanRunInfo(job.getIdentifier())){
              logFile.addMessage("DB deReservePart(" + job.getIdentifier() +
              ") failed.\n" + dbPluginMgr.getError(), job);
            }
          }
          /** if job Validated -> clearOutputMapping
           * (remove logs in final destination) */
          if(previousStatus==DBPluginMgr.FAILED ||
              previousStatus==DBPluginMgr.UNEXPECTED ||
              previousStatus==DBPluginMgr.UNDECIDED){
            GridPilot.getClassMgr().getCSPluginMgr().cleanup(job);
          }
        }
        else{
          succes = false;
          logFile.addMessage("DB updateDBStatus(" + job.getIdentifier() + ", " +
              dbStatus + ") failed \n" + "  -> dereservation not done.\n" +
              dbPluginMgr.getError(), job);
        }
        
      break;
      
      case DBPluginMgr.SUBMITTED:
        /**
         * Not called after submission : reservation set DB status to submitted
         * -> called only by resubmission
         */
        if(dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "status",
            dbStatus)){
          job.setDBStatus(dbStatusNr);
          if(job.getCSName()!=""){
            job.setNeedsUpdate(true);
          }
          job.setStatusReady();
          updateJobCell(job);
          job.setCSStatus(MyJobInfo.CS_STATUS_WAIT);
        }
        else{
          succes = false;
          logFile.addMessage("DB updateDBStatus(" + job.getIdentifier() + ", " +
              dbStatus + ") failed.\n" +
              dbPluginMgr.getError(), job);
        }
        break;

      case DBPluginMgr.UNDECIDED:
        if(dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "status",
            dbStatus)){
          job.setDBStatus(dbStatusNr);
        }
        else{
          succes = false;
          logFile.addMessage("DB updateDBStatus(" + job.getIdentifier() + ", " +
              dbStatus + ") failed.\n" +
              dbPluginMgr.getError(), job);
        }

        break;

      case DBPluginMgr.ABORTED:
        /**
         * clearOutputMapping, dereservation
         */

        if(dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "status",
            dbStatus)){
          if(previousStatus==DBPluginMgr.UNDECIDED ||
              previousStatus==DBPluginMgr.UNEXPECTED ||
              previousStatus==DBPluginMgr.VALIDATED){
            /** if job still has outputs, clean completely */
            GridPilot.getClassMgr().getCSPluginMgr().cleanup(job);
          }
          else{
            /** just dereserve */
            dbPluginMgr.cleanRunInfo(job.getIdentifier());
          }
          job.setDBStatus(dbStatusNr);
        }
        else{
          succes = false;
          logFile.addMessage("DB updateDBStatus(" + job.getIdentifier() + ", " +
              dbStatus + ") failed\n" +
                             "clearOutputMapping and dereservation not done.\n"+
                             dbPluginMgr.getError(),
                             job);
        }
        break;

      case DBPluginMgr.FAILED:
        /**
         * clearOutputMapping - dropped for now
         * - we want to be able to check what went wrong.
         */

        if(dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "status",
            dbStatus)){
          job.setDBStatus(dbStatusNr);
        }
        else{
          succes = false;
          logFile.addMessage("DB updateDBStatus(" + job.getIdentifier() + ", " +
              dbStatus + ") failed. "+dbPluginMgr.getError(), job);
        }
        break;

      case DBPluginMgr.UNEXPECTED:
        if(dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "status",
            dbStatus)){
        }
        else{
          succes = false;
          logFile.addMessage("DB updateDBStatus(" + job.getIdentifier() + ", " +
              dbStatus + ") failed. "+dbPluginMgr.getError(), job);
        }
        break;

      case DBPluginMgr.VALIDATED:
        /** previous : S, U*/
        // Post processing
        if(!GridPilot.getClassMgr().getCSPluginMgr().postProcess(job)){
          logFile.addMessage("Post processing of job " + job.getName() +
                             " failed ; \n" +
                             GridPilot.getClassMgr().getCSPluginMgr().getError(job.getCSName()) +
                             "\n\tThis job is put back in Undecided status ; \n"
                             +"\tde-reservation won't be done",
                             job);
          if(job.getDBStatus()!=DBPluginMgr.UNDECIDED){
            updateDBStatus(job, DBPluginMgr.UNDECIDED);
          }
        }
        else{
          // Stdout/stderr has now been copied to
          // final destinations and the local run dir deleted by postProcessing.
          if(dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "status",
              dbStatus)){
            job.setDBStatus(dbStatusNr);
          }
          else{
            succes = false;
            logFile.addMessage("DB updateDBStatus(" + job.getIdentifier() + ", " +
                dbStatus + ") failed.\n"+dbPluginMgr.getError(),
                               job);
          }
          /*if(!dbPluginMgr.cleanRunInfo(job.getIdentifier())){
            logFile.addMessage("de-reservation of job " + job.getName() +
                               " failed", job);
          }*/
        }

        break;

      default:
        succes = false;
        logFile.addMessage("ERROR: wrong DB status of job " + job.getName() +
                           " : " + job.getDBStatus());
        break;
    }

    updateDBCell(job);
    updateJobsByStatus();
    //statusTable.updateUI();
    
    return succes;
  }

  /**
   * Kills all jobs at these specified rows.
   */
  public static void killJobs(final int [] rows){
    if(GridPilot.getClassMgr().getCSPluginMgr().killJobs(
        getJobsAtRows(rows))){
      // update db and monitoring panel
    }
  }

}
