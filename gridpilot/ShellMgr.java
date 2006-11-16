package gridpilot;

import java.io.*;
import java.util.HashSet;

public interface ShellMgr{
  
  /**
   * Get the user name that was used to authenticate
   */
  public String getUserName();

  /**
   * Executes in the shell the command 'cmd', in the current directory, with
   * the environment 'env'.
   * Standard output is written in stdOut (if stdOut != null)
   * Standard error is written in stdErr (if stdErr != null)
   *
   * All elements of cmd can be null, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return exit value of the command
   * @throws IOException
   */
  public int exec(String cmd, StringBuffer stdOut, StringBuffer stdErr) throws IOException;

  /**
   * Executes in the shell the command 'cmd', in the directory 'workingDirecory'.
   * Standard output is written in stdOut (if stdOut != null)
   * Standard error is written in stdErr (if stdErr != null)
   *
   * All elements of cmd can be null, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return exit value of the command
   * @throws IOException
   */
  public int exec(String cmd, String [] env, String workingDirectory,
                  StringBuffer stdOut, StringBuffer stdErr) throws IOException;


  /**
   * Returns the content of the file named by this abstract pathname
   */
  public String readFile(String path) throws FileNotFoundException, IOException;

  /**
   * Creates the file name, and write content in this file.
   * If this file exists, and append == false its content is erased.
   * If the parent directory doesn't exist, it is created.
   */
  public void writeFile(String name, String content, boolean append) throws IOException;

  /**
   * Checks if the specified file (or directory) exists.
   */
  public boolean existsFile(String name);

  /**
   * Creates the directory named by this abstract pathname, including any
   * necessary but nonexistent parent directories.
   *
   * @return true if and only if the directory was created,
   *          along with all necessary parent directories; false
   *          otherwise
   */
  public boolean mkdirs(String dir);

  /**
   * Deletes the specified file.
   * @return  true if and only if the file has been deleted;
   *          false otherwise
   */
  public boolean deleteFile(String path);

  /**
   * Deletes the specified directory.
   * @return  true if and only if the directory has been deleted;
   *          false otherwise
   */
  public boolean deleteDir(String dir);
  
  /**
   * Copies file 'src' to file 'dest'. 
   * If the parent directory doesn't exist, it is created.
   * @return  true if and only if the copy succeeded;
   *          false otherwise
   */
  public boolean copyFile(String src, String dest);

  /**
   * Moves file 'src' to file 'dest'. 
   * If the parent directory doesn't exist, it is created.
   * @return  true if and only if the move succeeded;
   *          false otherwise
   */
  public boolean moveFile(String src, String dest);

  /**
   * Returns an array of abstract pathnames denoting the files and directories
   * in the directory denoted by this abstract pathname. 
   * Returns a empty array if the direcory is empty ;
   * Returns null if this directory doesn't exist
   */
  public String [] listFiles(String dir);

  /**
   * Returns a HashSet of all files found, starting from
   * the specified file or directory
   */
  public HashSet listFilesRecursively(String fileOrDir);
  
  /**
   * Tests whether the file denoted by this abstract pathname is a
   * directory.
   */
  public boolean isDirectory(String dir);

  /**
   * Tells wether this is a local shell manager or not (remote).
   */
  public boolean isLocal();
  
  /**
   * Only implemented by remote shells: Uploads file 'src' on local
   * disk to file 'dest' on server. 
   * If the parent directory doesn't exist, it is created.
   * @return  true if and only if the copy succeeded;
   *          false otherwise
   */
  public boolean upload(String src, String dest);

  /**
   * Only implemented by remote shells: Downloads file 'src' on server
   * to file 'dest' on local disk. 
   * If the parent directory doesn't exist, it is created.
   * @return  true if and only if the copy succeeded;
   *          false otherwise
   */
  public boolean download(String src, String dest);

  /**
   * Run a command and add the command name + command to the hash map
   * of running processes.
   * @param cmd    the command string
   * @param workingDirectory  the directory where the command is executed
   * @param stdOutFile    a file destination for the stdout
   * @param stdErrFile    a file destination for the stderr
   * @return              a string identifying the running command
   * @throws Exception
   */
  public String submit(String cmd, String workingDirectory,
      String stdOutFile, String stdErrFile) throws Exception;

  /**
   * Kills a process and removes it from the hash map of running processes.
   * @param id    the string identifying the process.
   */
  public void killProcess(String id);

  /**
   * Check if a process is running.
   * @return  true or false.
   * @param id    the string identifying the process.
   */
  public boolean isRunning(String id);
      
  public void exit();


}
