package gridpilot;

import gridfactory.common.jobrun.ComputingSystem;

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
 * An object of this class will be created for each system defined in [Computing systems].
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

public interface MyComputingSystem extends ComputingSystem {
  
  /** The job was successfully submitted. */
  public static final int RUN_OK = 0;
  /** The job cannot be run right now. */
  public static final int RUN_WAIT = 1;
  /** The job failed submission. */
  public static final int RUN_FAILED = 2;
  
  /** When LFC input files are used and LFNs are not looked up, they will be downloaded
   * on the worker nodes with file names input_file_0, input_file_1, ... */
  public static String LFC_INPUT_FILE_BASE_NAME = "input_file_";

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
   * Run a job.
   * @param job
   * @return one of the status codes RUN_OK, RUN_WAIT or RUN_FAILED
   */
  public int run(MyJobInfo job);

}
