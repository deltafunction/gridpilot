package gridpilot;

import java.io.IOException;
import java.util.Vector;

/**
 * Interface a computing system plugin must implement. <p>
 *
 * Creating a plugin (let say 'systemName') requires following things :
 * <ul>
 * <li>implement this interface, with a constructor like ComputingSystem(String systemName);
 * <li>in configuration file, in section [Computing systems], add an item 'systemName'
 * <li>create a section ['systemName'], with minimum these attributes :
 * <ul>
 *  <li> class = Name.of.the.class
 *  and optionaly
 *  <li> enabled = 'yes or no'
 *  <li> max jobs by update = 'number'
 * </ul>
 * </ul>
 *
 * An object of this class will be created for each system defined in [Computing systems],
 * using the constructor with one parameter (String).
 * Stdout and Stderr for a job will be created in directory 'working directory'
 * 'max jobs by update' denotes the maximum size of the job vector that the function 'updateStatus'
 * will receive
 * <p>
 * This interface defines some status values.
 * Each plugin has to match status values from its system to these:
 * <dl><ul>
 * <li><dt><code>STATUS_WAIT</code><dd>First state, until this jobs find a cpu;
 * Different from <code>STATUS_RUNNING</code> only for statistics reason. Could be not used by
 * a plugin - in which case the first accepted status for a job is running
 * <li><dt><code>STATUS_RUNNING</code><dd>This job is currently running
 * <li><dt><code>STATUS_DONE</code><dd>This job has succesfully finished and is ready for validation
 *   (job outputs are in job.getStdOut(), and job.getStdErr()
 * <li><dt><code>STATUS_ERROR</code><dd>An error occured, but it is temporary and possibly recoverable
 * <li><dt><code>STATUS_FAILED</code><dd>A fatal error occured, this job won't be checked any more
 * and validation will never be done
 * </ul></dl>
 * <p>
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
   * These attributes are initialized by GridPilot :<ul>
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
   * In the configuration file, this plugin section contains an attribute "max jobs by update", giving
   * the vector maximum size. If this value is not defined, this vector contains always only one
   * job.
   * The internalStatus must set to one of these values :
   * STATUS_WAIT, STATUS_RUNNING, STATUS_DONE, STATUS_ERROR, STATUS_FAILED
   * 
   * @param jobs Vector of JobInfo objects
   */
  public void updateStatus(Vector jobs);

  /**
   * Kills jobs 'jobs'.
   * Shouldn't change anything in 'jobs' ; all changed will be done by the next update.
   *
   * @param jobs Vector of JobInfo objects
   * @return true if the killing was successfull, false otherwise
   */
  public boolean killJobs(Vector jobs);

  /**
   * Called when job failed, in order to delete all "garbage" made by job.
   * 
   * @param job the job in question
   */
  public void clearOutputMapping(JobInfo job);

  /**
   * Called when GridPilot is closed.
   * Could be useful if some connection needs to be closed, temp files need to be deleted, ...
   */
  public void exit();

  /**
   * Gets a String which contains information about this job.
   * It could be typically the full outputs of the checking command on this system.
   * Null value or empty string is supported, but an expicit message is preferable in case of error.
   *
   * @param job the job in question
   * @return a String which contains some information about 'job'
   */
  public String getFullStatus(JobInfo job);

  /**
   * If it is supported by this system, gets current outputs of this job.
    * This method is called only when this job is running. After that, outputs are read from their
    * final destinations.
    * The return value is a String array, where the first String is the stdout, and the second one
    * is the stderr.
    * Return values are output, and not paths.
   *
   * @param job the job in question
   * @return String [] {'job' StdOut, 'job' StdErr}
   */
  public String [] getCurrentOutputs(JobInfo job) throws IOException;

  /**
   * Gets scripts used for running this job.
   * Return values are paths to local copies of the scripts.
   *
   * @param job the job in question
   * @return String [] {'job' script, 'job' grid job description file}
   */
  public String [] getScripts(JobInfo job);

  /**
   * Returns user information from the credentials used by this plugin
   * for submitting jobs.
   * Usually this would be the subject of the grid certificate.
   * 
   * @param csName the name of the computing system
   * @return a String which contains some information about 'user'
   */
  public String getUserInfo(String csName);
  
  /**
   * Operations done after a job is Validated.
   *
   * @param job the job in question
   * @return <code>true</code> if postprocessing went ok, <code>false</code> otherwise
   * 
   */
  public boolean postProcess(JobInfo job);

  /**
   * Operations done (by GridPilot) before a job is run.
   *
   * @param job the job in question
   * @return <code>true</code> if postprocessing went ok, <code>false</code> otherwise
   */
  public boolean preProcess(JobInfo job);

  /**
   * Get the last error reported by the plugin.
   *
   * @return error String
   * 
   */
  public String getError(String csName);

  /**
   * Write entries in the runtimeEnviroments table and
   * do necessary setup.
   * 
   * @param csName the name of the computing system
   */
  public void setupRuntimeEnvironments(String csName);

  /**
   * Clear entries written by setupRuntimeEnvironments.
   * 
   * @param csName the name of the computing system
   */
  public void cleanupRuntimeEnvironments(String csName);
  
  /**
   * Returns a shell manager for this job - if the computing system
   * supports this. Otherwise returns null.
   * 
   * @param job the job in question
   * @return a ShellMgr object or null
   */
  public ShellMgr getShellMgr(JobInfo job);

}
