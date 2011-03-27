package gridpilot;

import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.MyLinkedHashSet;
import gridfactory.common.StatusBar;

import java.awt.Color;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.*;

import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;
import javax.swing.Timer;

/**
 * This class manages the jobs from a single database.
 */
public class JobMgr{

  private DBPluginMgr dbPluginMgr;
  private MyLogFile logFile;
  
  private Timer timer;
  /** All jobs for which the post-processing is not done yet. */
  private Vector<MyJobInfo> toPostProcessJobs = new Vector<MyJobInfo>();
  /** All jobs for which the post-processing is in progress. */
  private Vector<MyJobInfo> postProcessingJobs = new Vector<MyJobInfo>();
 /** Maximum number of simultaneous threads for post-processing. */
  private int maxSimultaneousPostProcessing = 5;
  /** Delay in milliseconds between the begin of two postprocessing threads. */
  private int timeBetweenPostProcessing = 1000;
  
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
  private Set<MyJobInfo> monitoredjobs;

  /** Index of column of icon in statusTable .*/
  public final static int FIELD_CONTROL = 0;
  /** Index of column of JobName in statusTable .*/
  public final static int FIELD_JOBNAME = 1;
  /** Index of column of JobId in statusTable .*/
  public final static int FIELD_JOBID = 2;
  /** Index of column of JobStatus in statusTable .*/
  public final static int FIELD_STATUS = 3;
  /** Index of column of Computing System name in statusTable .*/
  public final static int FIELD_CS = 4;
  /** Index of column of host in statusTable .*/
  public final static int FIELD_HOST = 5;
  /** Index of column of db in statusTable .*/
  public final static int FIELD_DB = 6;
  /** Index of column of DB status in statusTable .*/
  public final static int FIELD_DBSTATUS = 7;
  /** Index of column of DB reservedBy in statusTable .*/
  public final static int FIELD_USER = 8;

  /** 
   * Counters of jobs ordered by local status.
   */
  private static int[] jobsByStatus = new int[DBPluginMgr.getStatusNames().length];
  
  /** 
   * Counters of jobs ordered by DB status.
   */
  private static int[] jobsByDBStatus = new int[DBPluginMgr.getDBStatusNames().length];
  
  /** 
   * Counters of running jobs ordered by computing system.
   */
  private int [] submittedJobsByCS;
  
  /** 
   * Counters of preprocessing jobs ordered by computing system.
   */
  private int [] preprocessedJobsByCS;
  
  private boolean [] hasChanged;
  private boolean useChanges = true;
  
  private ImageIcon iconWaiting;
  private ImageIcon iconProcessing;
  
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
    statusBar = GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar();
    statusTable = GridPilot.getClassMgr().getJobStatusTable();
    Debug.debug("Status table fields: "+statusTable.getModel().getColumnCount(), 3);
    statisticsPanel = GridPilot.getClassMgr().getJobStatisticsPanel();
    monitoredjobs = GridPilot.getClassMgr().getMonitoredJobs();
    try{
      URL imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "waiting.png");
      iconWaiting = new ImageIcon(imgURL);
    }
    catch(Exception e){
      logFile.addMessage("Could not find image "+ GridPilot.ICONS_PATH + "waiting.png");
      iconWaiting = new ImageIcon();
    }
    try{
      URL imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "processing.png");
      iconProcessing = new ImageIcon(imgURL);
    }
    catch(Exception e){
      logFile.addMessage("Could not find image "+ GridPilot.ICONS_PATH + "processing.png");
      iconProcessing = new ImageIcon();
    }
    if(useChanges){
      hasChanged = new boolean[0];
    }
    String tmp = GridPilot.getClassMgr().getConfigFile().getValue("Computing systems", "max simultaneous post-processing");
    if(tmp!=null){
      try{
        maxSimultaneousPostProcessing = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"max simultaneous post-processing\" "+
                                    "is not an integer in configuration file", nfe);
      }
    }
    timer = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent e){
        trigPostProcess();
      }
    });
    timer.setDelay(timeBetweenPostProcessing);
  }

   public int [] getJobsByStatus(){
     return jobsByStatus;
   }

   public int [] getJobsByDBStatus(){
     return jobsByDBStatus;
   }

   public int [] getSubmittedJobsByCS(){
     return submittedJobsByCS;
   }

   public int [] getPreprocessedJobsByCS(){
     return preprocessedJobsByCS;
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
    JProgressBar pb = statusBar.createJProgressBar(0, selectedJobDefs.length);
    statusBar.setProgressBar(pb);

    //monitored are not added
    for(int i=0; i<selectedJobDefs.length; ++i){
      pb.setValue(i);

      if(!exists(selectedJobDefs[i])){
        String jobName = dbPluginMgr.getJobDefName(selectedJobDefs[i]);
        MyJobInfo job = new MyJobInfo(selectedJobDefs[i], jobName);

        int dbStatus = DBPluginMgr.getStatusId(dbPluginMgr.getJobDefStatus(job.getIdentifier()));
        Debug.debug("Setting job db status :"+dbStatus+":", 2);
        job.setDBStatus(dbStatus);
        Debug.debug("Setting job DB :"+dbName+":", 3);
        job.setDBName(dbName);
        String jobCS = null;
        try{
          jobCS = dbPluginMgr.getRunInfo(job.getIdentifier(), "computingSystem");
          Debug.debug("Setting job CS :"+jobCS+":", 3);
          job.setCSName(jobCS);
        }
        catch(Exception e){
          Debug.debug(e.getCause().toString(), 2);
        }
        try{
          String [] rtes = dbPluginMgr.getRuntimeEnvironments(job.getIdentifier());
          Debug.debug("Setting job RTEs :"+MyUtil.arrayToString(rtes)+":", 3);
          job.setRTEs(rtes);
        }
        catch(Exception e){
          Debug.debug(e.getCause().toString(), 2);
        }
        try{
          String jobID = dbPluginMgr.getRunInfo(job.getIdentifier(), "jobID");
          Debug.debug("Setting job ID :"+jobID+":", 3);
          job.setJobId(jobID);
        }
        catch(Exception e){
          Debug.debug(e.getCause().toString(), 2);
        }
        try{
          String jobHost = dbPluginMgr.getRunInfo(job.getIdentifier(), "host");
          Debug.debug("Setting job host :"+jobHost+":", 3);
          job.setHost(jobHost);
        }
        catch(Exception e){
          Debug.debug(e.getCause().toString(), 2);
        }
        GridPilot.getClassMgr().getCSPluginMgr().setCSUserInfo(job);
        job.setTableRow(monitoredjobs.size());
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
            Debug.debug(job.getName()+" is failed",3);
            job.setNeedsUpdate(false);
            job.setOutputs(dbPluginMgr.getStdOutFinalDest(job.getIdentifier()),
                dbPluginMgr.getStdErrFinalDest(job.getIdentifier()));
            break;
          case DBPluginMgr.UNDECIDED:
            Debug.debug(job.getName()+" exited with state undecided",3);
            stdOut = dbPluginMgr.getRunInfo(job.getIdentifier(), "outTmp");
            stdErr = dbPluginMgr.getRunInfo(job.getIdentifier(), "errTmp");
            if(stdErr==null || stdErr.trim().length()==0 || stdErr.equalsIgnoreCase("null")){
              stdErr = null;
            }
            job.setOutputs(stdOut, stdErr);
            //setUpdateNeeded(job);
            job.setNeedsUpdate(false);
            break;
          case DBPluginMgr.UNEXPECTED:
            Debug.debug(job.getName()+" ran with unexpected errors",3);
            stdOut = dbPluginMgr.getRunInfo(job.getIdentifier(), "outTmp");
            stdErr = dbPluginMgr.getRunInfo(job.getIdentifier(), "errTmp");
            if(stdErr==null || stdErr.trim().length()==0 ||
               stdErr.equalsIgnoreCase("null")){
              stdErr = null;
            }
            job.setOutputs(stdOut, stdErr);
            setUpdateNeeded(job);
            break;
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
            setUpdateNeeded(job);
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
        monitoredjobs.add(job);
        
        Debug.debug(job.getName()+" getNeedsUpdate: "+job.getNeedsUpdate(),3);

        if(job.getDBStatus()==DBPluginMgr.SUBMITTED){
          job.setCSStatus(MyJobInfo.CS_STATUS_WAIT);
          if(job.getJobId()==null || job.getJobId().trim().length()==0){
            logFile.addMessage("This job is SUBMITTED but is not known to the computing backend.\n" +
                               "It is set to UNDECIDED", job);
            job.setNeedsUpdate(false);
            initChanges();
            // TODO: is this necessary?
            statusTable.createRows(monitoredjobs.size());
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

    statusTable.createRows(monitoredjobs.size());
    initChanges();

    updateDBCells(monitoredjobs);
    updateJobCells(monitoredjobs);
    
    //updateJobsByStatus();
  }

  private void setUpdateNeeded(MyJobInfo job) {
    String csName = dbPluginMgr.getRunInfo(job.getIdentifier(), "computingSystem");
    if(csName==null || csName.equals("")){
      logFile.addMessage("this job (" + job.getIdentifier() + ") doesn't have a CS defined");
      job.setNeedsUpdate(false);
    }
    job.setCSName(csName);
  }

  public void initChanges(){
    boolean [] oldChanges = hasChanged;
    hasChanged = new boolean[monitoredjobs.size()];
    if(oldChanges!=null){
      for(int i=0; i<hasChanged.length && i<oldChanges.length; ++i){
        hasChanged[i] = oldChanges[i];
      }
    }
  }

  /**
   * Updates cell in status table which contains DB status of this specified job.
   * (from AtCom)
   */
  public void updateDBCell(final MyJobInfo job){
    if(SwingUtilities.isEventDispatchThread()){
      doUpdateDBCell(job);
    }
    else{
      SwingUtilities.invokeLater(
        new Runnable(){
          public void run(){
            try{
              doUpdateDBCell(job);
            }
            catch(Exception ex){
              Debug.debug("Could not update DB cell", 1);
              ex.printStackTrace();
            }
          }
        }
      );
    }
  }

  private void doUpdateDBCell(MyJobInfo job) {
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
    
    if(job.getTableRow()>-1){
      statusTable.setValueAt(status, job.getTableRow(), FIELD_DBSTATUS);
      statusTable.updateSelection();
    }
  }

  /**
   * Updates DB cell for all specified jobs.
   * @see #updateDBCell(MyJobInfo)
   * (From AtCom)
   */
  public void updateDBCells(Set<MyJobInfo> jobs){
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

    for(Iterator<MyJobInfo> it=jobs.iterator(); it.hasNext();){
      updateDBCell(it.next());
    }
    //updateJobsByStatus();

  }

  /**
   * Updates cells for all specified jobs.
   * @see #updateJobCell(MyJobInfo)
   * (From AtCom)
   */
  public void updateJobCells(Set<MyJobInfo> jobs){
    for(Iterator<MyJobInfo> it=jobs.iterator(); it.hasNext();){
      updateJobCell(it.next());
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
    
    String [] css = GridPilot.getClassMgr().getCSPluginMgr().getEnabledCSNames();
    submittedJobsByCS = new int[css.length];
    preprocessedJobsByCS = new int[css.length];
    MyJobInfo job;

    for(Iterator<MyJobInfo>it=monitoredjobs.iterator(); it.hasNext();){
      job = it.next();
      //Debug.debug("adding job "+job.getName()+
      //    " : "+job.getDBStatus()+
      //    " : "+(job.getDBStatus()<1?"":jobsByDBStatus[job.getDBStatus()-1]), 3);
      if(job.getDBStatus()>0){
        ++jobsByDBStatus[job.getDBStatus()-1];
      }

      switch(job.getStatus()){
        case MyJobInfo.STATUS_READY:
          ++jobsByStatus[DBPluginMgr.STAT_STATUS_WAIT];
          break;

        case MyJobInfo.STATUS_PREPARED:
          ++jobsByStatus[DBPluginMgr.STAT_STATUS_WAIT];
          break;

        case MyJobInfo.STATUS_RUNNING:
          ++jobsByStatus[DBPluginMgr.STAT_STATUS_RUN];
          break;

        case MyJobInfo.STATUS_DONE:
          ++jobsByStatus[DBPluginMgr.STAT_STATUS_DONE];
          break;

        case MyJobInfo.STATUS_FAILED:
          ++jobsByStatus[DBPluginMgr.STAT_STATUS_DONE];
          break;
      }
      
      for(int j=0; j<css.length; ++j){
        if(css[j].equalsIgnoreCase(job.getCSName())){
          if(job.getDBStatus()==DBPluginMgr.SUBMITTED){
            ++submittedJobsByCS[j];
          }
          if(job.getDBStatus()==DBPluginMgr.PREPARED){
            ++preprocessedJobsByCS[j];
          }
        }
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
    updateDBCells(monitoredjobs);
  }

  /**
   * Checks if there is already a job with the specified jobDefId in the monitored jobs. <p>
   * (From AtCom)
   */
  public boolean exists(String jobDefId){
    MyJobInfo job;
    Debug.debug("Checking jobs: "+monitoredjobs.size(), 3);
    Iterator<MyJobInfo> i = monitoredjobs.iterator();
    while(i.hasNext()){
      job = i.next();
      if(job.getIdentifier()==jobDefId){
        return true;
      }
    }
    return false;
  }

  /**
   * Removes row according to job name.
   * @throws InvocationTargetException 
   * @throws InterruptedException 
   */
  public void removeRow(final String jobDefID) throws InterruptedException, InvocationTargetException{
    if(SwingUtilities.isEventDispatchThread()){
      doRemoveRow(jobDefID);
    }
    else{
      SwingUtilities.invokeAndWait(
        new Runnable(){
          public void run(){
            doRemoveRow(jobDefID);
          }
        }
      );
    }
  }
 
  private void doRemoveRow(String jobDefID){
    String lfn = dbPluginMgr.getJobDefinition(jobDefID).getValue("name").toString();
    // Remove jobs from status vectors
    MyJobInfo job;
    for(Iterator<MyJobInfo>it=monitoredjobs.iterator(); it.hasNext();){
      job = it.next();
      if(job.getName().equals(lfn)){
        --jobsByDBStatus[job.getDBStatus()-1];
        monitoredjobs.remove(job);
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
    statusTable.validate();
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
        "  Status CS \t: " + job.getCSStatus() + "\n" +
        "  Status \t: " + MyJobInfo.getStatusName(job.getStatus()) + "\n" +
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
    Debug.debug("Failing job "+job.getIdentifier(), 2);
    updateDBStatus(job, DBPluginMgr.FAILED);
    if(job.getDBStatus()!=DBPluginMgr.FAILED){
      logFile.addMessage("Update DB status failed after job Failure ; " +
                         " this job is put back updatable", job);
      job.setNeedsUpdate(true);
    }
    else{
      // Resubmit job if value of resubmit spinner is larger than job.getResubmitCount().
      Integer resubmit = (Integer) GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel(
          ).getJobMonitoringPanel().getSAutoResubmit().getValue();
      int resubmitNr = resubmit.intValue();
      Debug.debug("Checking if job should be resubmitted, "+job.getResubmitCount()+":"+resubmitNr, 2);
      if(job.getResubmitCount()>-1 && job.getResubmitCount()<resubmitNr){
        job.incrementResubmitCount();
        logFile.addInfo("Auto-resubmitting job "+job.getIdentifier()+" : "+job.getResubmitCount()+":"+resubmitNr);
        // TODO: consider doing this in a thread...
        SubmissionControl submissionControl = GridPilot.getClassMgr().getSubmissionControl();
        Set<MyJobInfo> rJobs = new LinkedHashSet<MyJobInfo>();
        rJobs.add(job);
        submissionControl.resubmit(rJobs);
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
    Set<MyJobInfo> submJobs = GridPilot.getClassMgr().getMonitoredJobs();
    MyJobInfo job = null;
    for(Iterator<MyJobInfo>it=submJobs.iterator(); it.hasNext();){
      job = it.next();
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
    Set<MyJobInfo> submJobs = GridPilot.getClassMgr().getMonitoredJobs();
    //Debug.debug("Got jobs at row "+row+". "+submJobs.size(), 3);
    //return submJobs.get(row);
    MyJobInfo job = null;
    int i = 0;
    for(Iterator<MyJobInfo>it=submJobs.iterator(); it.hasNext();){
      job = it.next();
      if(i==row /*&& job.getTableRow()==row*/){
        return job;
      }
      ++i;
    }
    return null;
  }

  /**
   * Returns the jobs at the specified rows in statusTable
   * @see #getJobAtRow(int)
   */
  public static MyLinkedHashSet<MyJobInfo> getJobsAtRows(int[] row){
    MyLinkedHashSet<MyJobInfo> jobs = new MyLinkedHashSet<MyJobInfo>(row.length);
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
   * Checks if all jobs at these specified rows are cleanable. <p>
   * Called by {@link MonitoringPanel#selectionEvent(javax.swing.event.ListSelectionEvent)} <p>
   * @see #isCleanable(MyJobInfo)
   */
  public static boolean areCleanable(int[] rows) {
    for (int i = 0; i < rows.length; ++i) {
      MyJobInfo job = getJobAtRow(rows[i]);
      if (!isCleanable(job))
        return false;
    }
    return true;
  }
  
  /**
   * Checks if this specified job is killable.
   * A Killable job is a job which is Submitted, and have a job Id!=null.
   * Called by :
   * <li>{@link #areCleanable(int[])}
  * @see #areKillable(int[])
   */
  private static boolean isCleanable(MyJobInfo job){
    return (job.getDBStatus()==DBPluginMgr.UNDECIDED ||
            job.getDBStatus()==DBPluginMgr.UNEXPECTED ||
            job.getDBStatus()==DBPluginMgr.FAILED||
            job.getDBStatus()==DBPluginMgr.ABORTED) && job.getJobId()!=null;
  }

  /**
   * Checks if this specified job is killable.
   * A Killable job is a job which is Submitted, and have a job Id!=null.
   * Called by :
   * <li>{@link #areKillable(int[])}
   * @see #areKillable(int[])
   */
  private static boolean isKillable(MyJobInfo job){
    return job.getDBStatus()==DBPluginMgr.SUBMITTED && job.getJobId()!=null;
  }

  /**
   * Checks if all jobs at these specified rows are killable. <p>
   * Called by {@link MonitoringPanel#selectionEvent(javax.swing.event.ListSelectionEvent)} <p>
   * @see #isKillable(MyJobInfo)
   */
  public static boolean areKillable(int[] rows) {
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
  public static boolean areDecidable(int[] rows){
    for(int i = 0; i < rows.length; ++i){
      if(getJobAtRow(rows[i]).getDBStatus()!=DBPluginMgr.UNDECIDED)
        return false;
    }
    return true;
  }

  /**
   * Checks if all jobs at these specified rows are re-submitable.
   * A job is resubmitable is its DB status is Failed, and if it has a
   * attributed computing system.
   */
  public static boolean areResumbitable(int[] rows){
    for(int i = 0; i < rows.length; ++i){
      MyJobInfo job = getJobAtRow(rows[i]);
      if(job.getDBStatus()!=DBPluginMgr.FAILED || job.getCSName().equals(""))
        return false;
    }
    return true;
  }

  /**
   * Checks if all jobs at these specified rows are submitable.
   * A job is submitable iff its DB status is Defined.
   *
   */
  public static boolean areSubmitable(int[] rows){
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
          try{
            String confirmString = "WARNING: The transition from " +
               DBPluginMgr.getStatusName(job.getDBStatus()) + " to " +
               DBPluginMgr.getStatusName(dbStatus) + " is not allowed.\n"+
               authorization+".\nContinue on your own risk.";
            confirmString = "<html>"+confirmString.replaceAll("\\n", "<br>")+"</html>";
            if(rows.length-i>1){
              choice = MyUtil.showResult(GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel(),
                  new JLabel(confirmString),
                  "Confirm change status", MyUtil.OK_ALL_SKIP_ALL_OPTION, "Skip");
            }
            else{
              choice = MyUtil.showResult(GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel(),
                  new JLabel(confirmString),
                  "Confirm change status", MyUtil.OK_SKIP_OPTION, "Cancel");
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
   * Tells if a transition between DB statuses is allowed. <p>
   * @return <code>null</code> if the transition from the DB status <code>fromStatus</code>
   * to <code>toStatus</code> is allowed, and a String which explains why if this
   * transition is not allowed. <p>
   *
   */
  private static String isTransitionAllowed(int fromStatus, int toStatus) {

    String sameStatus = "Transitions between the same status are not allowed";

    String fromDefined = "Defined jobs cannot be set manually to other statuses than Aborted";
    String fromPrepared = "Prepared jobs cannot be set manually to other statuses than Aborted or Failed";
    String fromValidated = "A Validated job can only be set to Defined or Aborted";
    String fromAborted = "An Aborted job has to be set to Defined before manual resubmission";
    String fromSubmitted = "Kill this job and wait until GridPilot detects it has been terminated - " +
        "otherwise a ghost job may overwrite a new attempt";

    String toNotAllowed = "A job cannot be set to this status manually";

    String unToSub = "Set it Failed before, and resubmit it";
    String failToSub = "Resubmit it";
    String unToDef = "Set it to Failed first";

    String allowedTransition [][] = {
        /*   from\to    DEFINED        PREPARED        SUBMITTED      VALIDATED      UNDECIDED      UNEXPECTED      FAILED         ABORTED*/
        /*DEFINED*/    {sameStatus,    fromDefined,    fromDefined,   fromDefined,   fromDefined,   fromDefined,    fromDefined,   null},
        /*PREPARED*/   {fromPrepared,  sameStatus,     fromPrepared,  fromPrepared,  fromPrepared,  fromPrepared,   null,          null},
        /*SUBMITTED*/  {fromSubmitted, fromSubmitted,  sameStatus,    toNotAllowed,  toNotAllowed,  toNotAllowed,   fromSubmitted, fromSubmitted},
        /*VALIDATED*/  {null,          fromValidated,  fromValidated, sameStatus,    fromValidated, fromValidated,  fromValidated, null},
        /*UNDECIDED*/  {unToDef,       unToSub,        unToSub,       toNotAllowed,  sameStatus,    null,           null,          null},
        /*UNEXPECTED*/ {null,          unToSub,        unToSub,       toNotAllowed,  null,          sameStatus,     null,          null},
        /*FAILED*/     {null,          failToSub,      failToSub,     toNotAllowed,  toNotAllowed,   null,           sameStatus,    null},
        /*ABORTED*/    {null,          fromAborted,    fromAborted,   fromAborted,   fromAborted,   null,           fromAborted,   sameStatus}};

    return allowedTransition[fromStatus -1][toStatus -1];
  }

  /**
   * Called each time the DB status of the specified job changes (except at submission). <br>
   * In all cases the DB is updated.<br>
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
    // could have integrity problems), but should be done after post-processing
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
          /** if job was failed -> cleanup
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
        /** set DB status to submitted , status to ready and csStatus to wait -
         * notice that updateDBstatus(JobInfo) is not called on job submission, so
         * the below is only called on resubmission - on submission it is done elsewhere */
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
        /** cleanup, dereservation */
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
                             "cleanup and dereservation not done.\n"+
                             dbPluginMgr.getError(),
                             job);
        }
        break;

      case DBPluginMgr.FAILED:
        if(dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "status",
            dbStatus)){
          job.setDBStatus(dbStatusNr);
        }
        else{
          succes = false;
          logFile.addMessage("DB updateDBStatus(" + job.getIdentifier() + ", " +
              dbStatus + ") failed. "+dbPluginMgr.getError(), job);
        }
        /** if job was undecided, this status must have been decided and we can cleanup */
        if(previousStatus==DBPluginMgr.UNEXPECTED ||
            previousStatus==DBPluginMgr.UNDECIDED){
          GridPilot.getClassMgr().getCSPluginMgr().cleanup(job);
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
        /** post process */
        job.setDBStatus(dbStatusNr);
        queue(job);
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
  
  private void queue(MyJobInfo job) {
    Debug.debug("Adding job to post-processing queue "+job, 2);
    statusTable.setValueAt(iconWaiting, job.getTableRow(), JobMgr.FIELD_CONTROL);
    toPostProcessJobs.add(job);
    if(!timer.isRunning()){
      timer.restart();
    }
  }

  private void trigPostProcess(){
    if(toPostProcessJobs.isEmpty()){
      timer.stop();
      return;
    }
    Debug.debug("Looking for job to post-process from "+toPostProcessJobs, 3);
    final MyJobInfo job = toPostProcessJobs.get(0);
    if(checkProcessing(job)){
      Debug.debug("Removing job from post-processing queue "+job, 2);
      toPostProcessJobs.remove(job);
      postProcessingJobs.add(job);
      new Thread(){
        public void run(){
          try{
            postProcess(job);
            postProcessingJobs.remove(job);
          }
          catch(Exception e){
            logFile.addMessage("There was a problem post-processing job "+job, e);
            postProcessingJobs.remove(job);
          }
        }
      }.start();
    }
  }

  /**
   * Check if this job can be post-processed or we should wait.
   * @param job
   * @return
   */
  private boolean checkProcessing(MyJobInfo job) {
    boolean ok = postProcessingJobs.size()<=maxSimultaneousPostProcessing;
    Debug.debug("Post-process proceed "+ok+" --> "+postProcessingJobs.size()+":"+maxSimultaneousPostProcessing, 2);
    return ok;
  }
  
  public boolean isPostProcessing(){
    return !postProcessingJobs.isEmpty();
  }

  private boolean postProcess(MyJobInfo job) {
    Debug.debug("Post-processing job "+job, 2);
    statusTable.setValueAt(iconProcessing, job.getTableRow(), JobMgr.FIELD_CONTROL);
    boolean succes = true;
    if(!GridPilot.getClassMgr().getCSPluginMgr().postProcess(job)){
      logFile.addMessage("Post processing of job " + job.getName() +
                         " failed ; \n" +
                         GridPilot.getClassMgr().getCSPluginMgr().getError(job.getCSName()) +
                         "\n\tThis job is put back in Undecided status ; \n"
                         +"\tde-reservation won't be done",
                         job);
      //job.setDBStatus(DBPluginMgr.UNDECIDED);
      if(job.getDBStatus()!=DBPluginMgr.UNDECIDED){
        updateDBStatus(job, DBPluginMgr.UNDECIDED);
      }
    }
    else{
      int dbStatusNr = job.getDBStatus();
      String dbStatus = DBPluginMgr.getStatusName(dbStatusNr);
      /* Output files and stdout/stderr have now been copied to
         final destinations and the local run dir deleted by postProcessing.
         We now commit the status "validated" to the DB. This means that
         SubmissionControl.checkDependenceOnOtherJobs() will allow any possible
         other job depending on this one to run.
       */
      if(!dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "status",
          dbStatus)){
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
    if(job.getHost()!=null && !job.getHost().equals("") &&
       !dbPluginMgr.setJobDefsField(new String [] {job.getIdentifier()}, "host", job.getHost())){
      succes = false;
      logFile.addMessage("DB updateDBStatus(" + job.getIdentifier() + ", " +
          job.getHost() + ") failed.\n"+dbPluginMgr.getError(),
                         job);
    }
    statusTable.setValueAt(null, job.getTableRow(), JobMgr.FIELD_CONTROL);
    return succes;
  }

  /**
   * Kills all jobs at the specified rows.
   */
  public static void killJobs(final int [] rows){
    if(GridPilot.getClassMgr().getCSPluginMgr().killJobs(
        MyUtil.toJobInfos(getJobsAtRows(rows)))){
      // update db and monitoring panel
    }
  }

  /**
   * Cleans all jobs at the specified rows.
   */
  public static void cleanJobs(final int [] rows) {
    ConfirmBox confirmBox = new ConfirmBox(
        (Window) SwingUtilities.getRoot(GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel())); 
    try{
      int choice = confirmBox.getConfirm("Confirm clean",
          "This will clean all files produced by the job(s)\n" +
          "both remotely and locally.\n" +
          "Are you sure you want to do this?", new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()),
                                                             MyUtil.mkCancelObject(confirmBox.getOptionPane())});
      if(choice!=0){
        return;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }
    MyJobInfo job;
    for(int i=0; i<rows.length; ++i){
      job = getJobAtRow(rows[i]);
      GridPilot.getClassMgr().getCSPluginMgr().cleanup(job);
      // TODO: This will set the job as Defined. Not sure if this is a good idea...
      /*job.setDBStatus(DBPluginMgr.DEFINED);
      GridPilot.getClassMgr().getJobMgr(job.getDBName()).updateDBCell(job);
      GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()
          ).setJobDefsField(new String[] {job.getIdentifier()}, "status",
              DBPluginMgr.getStatusName(DBPluginMgr.DEFINED));*/
    }
  }

}
