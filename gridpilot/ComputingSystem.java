package gridpilot;

import java.io.IOException;
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
 * 'max jobs by update' denotes the maximum size of the job vector that the function 'updateStatus'
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
 * This interface defines 13 methods :
 * <dl><ul>
 * <li>{@link #submit(JobInfo) submit}(<code>JobInfo</code> job)
 *  <dd>Submits this job, sets job.jobId.
 * <li>{@link #updateStatus(Vector) updateStatus}(<code>job vector</code> jobs)
 *  <dd>Updates status for all jobs in jobs ; set internalStatus to the correspondig value
 * <li>{@link #killJobs(Vector) killJobs}(<code>job vector</code> job)
 *  <dd>Kills this vector of jobs.
 * <li>{@link #clearOutputMapping(JobInfo) clearOutputMapping}(<code>JobInfo</code> job)
 *  <dd>Called when this job failed, in order to delete generated files ...
 * <li>{@link #getCurrentOutputs(JobInfo) getCurrentOutputs}(<code>JobInfo</code> job)
 *  <dd>If it is supported by this system, gets current outputs of this job.
 * <li>{@link #getFullStatus(JobInfo) getFullStatus}(<code>JobInfo</code> job)
 *   <dd>Gets a String which contains information about this job (typically the full outputs of
 *   the checking command on this system).
 * <li>{@link #getScripts(JobInfo) getScripts}(<code>JobInfo</code> job)
 *   <dd>Gets the scripts used by this job.
 * <li>{@link #getUserInfo(csName) getUserInfo}(<code>String</code> job)
 *   <dd>Gets the ID of the user who submitted this job.
 * <li>{@link #getError(csName) getError}(<code>String</code> job)
 *   <dd>Gets the last error if any.
 * <li>{@link #preProcess(JobInfo) preProcess}(<code>JobInfo</code> job)
 *   <dd>Optionally prepares for submitting this job.
 * <li>{@link #postProcess(JobInfo) postProcess}(<code>JobInfo</code> job)
 *   <dd>Optionally carries out actions after running this job.
 * <li>{@link #setupRuntimeEnvironments(csName) setupRuntimeEnvironments}(<code>String</code> job)
 *   <dd>Detects installed runtime environments and makes the relevant entry in the
 *   database configured in the configuration file.
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
   * Updates status of all jobs in job vector jobs. <p>
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
  public boolean killJobs(Vector jobs);

  /**
   * Called when job failed, in order to delete all "garbage" made by job.
   */
  public void clearOutputMapping(JobInfo job);

  /**
   * Called when GridPilot is closed.
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
   * Return values are output, and not paths.
   *
   * @return String [] {'job' StdOut, 'job' StdErr}
   */
  public String [] getCurrentOutputs(JobInfo job, boolean resyncFirst) throws IOException;

  /**
   * Gets scripts used for running this job.
   * Return values are paths to local copies of the scripts.
   *
   * @return String [] {'job' script, 'job' grid job description file}
   */
  public String [] getScripts(JobInfo job);

  /**
   * Returns user information from the credentials used by this plugin
   * for submitting jobs.
   * Usually this would be the subject of the grid certificate.
   * 
   * @return a String which contains some information about 'user'
   */
  public String getUserInfo(String csName);
  
  /**
   * Operations done after a job is Validated. <br>
   * Theses operations contain emcompasses two stages :
   * <ul>
   * <li>Moving of outputs in their final destination
   * <li>Extraction of some informations from outputs
   * </ul> <p>
   *
   * @return <code>true</code> if postprocessing went ok, <code>false</code> otherwise
   * 
   */
  public boolean postProcess(JobInfo job);

  /**
   * Operations done (by GridPilot) before a job is run. <br>
   *
   * @return <code>true</code> if postprocessing went ok, <code>false</code> otherwise
   * 
   */
  public boolean preProcess(JobInfo job);

  /**
   * Get the last error reported by the plugin. <br>
   *
   * @return error String
   * 
   */
  public String getError(String csName);

  /**
   * Write entries in the runtimeEnviroments table and
   * do necessary setup. <br>
   */
  public void setupRuntimeEnvironments(String csName);

}
