package gridpilot;

import java.util.Vector;

/**
 * Interface a plugin for a computing system must implement. <p>
 *
 * Creating a plugin (let say 'systemName') requires following things :
 * <ul>
 * <li>implement this interface, with a constructor like ComputingSystem(String systemName);
 * <li>in configuration file, in section [Computing systems], add an item 'systemName'
 * <li>create a section ['systemName'], with minimum these attributes :
 * <ul>
 *  <li> class = Name.of.the.class
 *  <li> working directory = 'path'
 *  and optionaly
 *  <li> max jobs by update = 'number'
 * </ul>
 * </ul>
 *
 * An object of this class will be created for each system defined in [Computing systems],
 * using the constructor with one parameter (String).
 * StdOut and StdErr for a job will be created in directory 'working directory'
 * (AtCom creates names, not files)
 * 'max jobs by update' denotes the maximum size of JobVector that the function 'updateStatus'
 * will be receive
 * <p>
 * This interface defines some status ; these values are used by job.internalStatus.
 * Each plugin has to match status from his system to these status :
 * <dl><ul>
 * <li><dt><code>STATUS_WAIT</code><dd>First state, until this jobs find a cpu;
 * Different from <code>STATUS_RUNNING</code> only for statistics reason. Could be not used by
 * this plugin, in such case, the first accepted status for a job is running.
 * <li><dt><code>STATUS_RUNNING</code><dd>This job is currently running
 * <li><dt><code>STATUS_DONE</code><dd>This job has succesfully finished and is ready for validation
 *   (job outputs are in job.getStdOut, and job.getStdErr, if needed, 'get output' is already done
 * <li><dt><code>STATUS_ERROR</code><dd>An error occured, but it is temporary and possibly recoverable
 * <li><dt><code>STATUS_FAILED</code><dd>A fatal error occured, this job won't be checked any more
 * and validation will never be done
 * </ul></dl>
 * <p>
 *
 * This interface defines 7 methods :
 * <dl><ul>
 * <li>{@link #submit(atcom.jobcontrol.JobInfo) submit}(<code>JobInfo</code> job)
 *  <dd>Submits this job, initializes job.jobId
 * <li>{@link #updateStatus(atcom.jobcontrol.JobVector) updateStatus}(<code>JobVector</code> jobs)
 *  <dd>Updates status for all jobs in jobs ; set internalStatus to the correspondig value
 * <li>{@link #killJob(atcom.jobcontrol.JobInfo) killJob}(<code>JobInfo</code> job)
 *  <dd>Kills this job
 * <li>{@link #clearOutputMapping(atcom.jobcontrol.JobInfo) clearOutputMapping}(<code>JobInfo</code> job)
 *  <dd>Called when this job failed, in order to delete generated files, such .zebra, ...
 * <li>{@link #getCurrentOutputs(atcom.jobcontrol.JobInfo) getCurrentOutputs}(<code>JobInfo</code> job)
 *  <dd>If it is supported by this system, gets current outputs of this job
 * <li>{@link #getFullStatus(atcom.jobcontrol.JobInfo) getFullStatus}(<code>JobInfo</code> job)
 *   <dd>Gets a String which contains information about this job (typically the full outputs of
 *   the checking command on this system)
 * <li>{@link #exit() exit}()<p>
 * </ul></dl>
 *
 * Using {@link atcom.AtCom#getClassMgr() AtCom.getClassMgr()}, a plugin can access to :
 * <ul>
 * <li>{@link atcom.ConfigFile ConfigFile} : configuration file, should be used in order to store all configurable parameters
 * <li>{@link atcom.LogFile LogFile} : records all errors, exceptions, ...
 * <li>{@link atcom.databases.AMIMgt AMIMgt} : access to AMI database (where logicalFile (partition) key is job.PartId())
 * </ul>
 *
 * <p><a href="ComputingSystem.java.html">see sources</a>
 */

public interface ComputingSystem{

  /**
   * This job has been submitted, but hasn't yet started.
   */
  public final static int STATUS_WAIT = -1;
  /**
   * This job found a cpu, and is running
   */
  public final static int STATUS_RUNNING = -2;
  /**
   * This job has succesfully finished and is ready for validation
   */
  public final static int STATUS_DONE = -3;

  /**
   * An error occured, but it is temporary and possibly recoverable
   */
  public final static int STATUS_ERROR = -4;
  /**
   * A fatal error occured, this job won't be checked any more and validation will never be done
   */
  public final static int STATUS_FAILED = -5;


  /**
   * Submits this job.
   *
   * These attributes are initialized by AtCom :<ul>
   * <li>StdOut, StdErr
   * <li>Name
   * <li>AMIStatus (Submitted)
   * <li>ComputingSystem
   * <li>logicalFile (partition) ID
   * </ul>
   * Plugin has to set JobId and return <code>true</code> if this job has been
   * correctly submitted, and return <code>false</code> otherwise
   *
   * @param job Job to submit
   * @return true if job has been submitted, false otherwise
   */
  public boolean submit(JobInfo job);

  /**
   * Updates status of all jobs in JobVector jobs. <p>
   * In configuration file, this plugin section contain an attribute "max jobs by update" giving
   * the vector maximum size. If this value is not defined, this vector contains always only one
   * job. <p>
   * JobStatus an optionnaly host should be initialized (or updated), and internalStatus has to be
   * set to one of theses values : STATUS_WAIT, STATUS_RUNNING, STATUS_DONE, STATUS_ERROR or
   * STATUS_FAILED (cf above)
   *
   */
  public void updateStatus(Vector jobs);

  /**
   * Kills jobs 'jobs'.
   * Shouldn't change anything in 'jobs' ; all changed will be done by the next update.
   *
   */
  public void killJobs(Vector jobs);

  /**
   * Called when job 'job' failed, in order to delete all "garbages" made by job.
   */
  public void clearOutputMapping(JobInfo job);

  /**
   * Called when AtCom is closed.
   * Could be useful if some connection need to be closed, temp files te be deleted, ...
   */
  public void exit();

  /**
   * Gets a String which contains information about this job.
   * It could be typically the full outputs of the checking command on this system.
   * Null value or empty string is supported, but an expicit message is preferable in case of error.
   *
   * @return a String which contains some information about 'job'
   */
  public String getFullStatus(JobInfo job);

  /**
   * If it is supported by this system, gets current outputs of this job.
   * This method is called only when this job is running. After that, outputs are read in
   * job.StdOut and job.StdErr;
   * This return value is an String array, where the first String is StdOut, and the second one
   * is StdErr.
   * Return values are output, and not path of this outputs.
   *
   * @return String [] {'job' StdOut, 'job' StdErr}
   */
  public String [] getCurrentOutputs(JobInfo job);

  /**
   * Used for copying stdout to final destination.
   * 
   * @return true or false for success or failure
   */  
  public boolean copyFile(String csName, String src, String dest);

  /**
   * Used for cleaning up after job has run.
   * 
   * @return true or false for success or failure
   */  
  public boolean deleteFile(String csName, String src);
  /**
   * Returns user information from the credentials used by this plugin
   * for submitting jobs.
   * Usually this would be the subject of the grid certificate.
   * 
   * @return a String which contains some information about 'user'
   */
  public String getUserInfo(String csName);
}
