/**
 * $Log: ShellMgr.java,v $
 * Revision 1.1  2005/06/15 15:22:20  fjob
 * Merging started.
 *
 * Revision 1.1.1.1  2004/03/29 20:20:48  fjob
 * initial import into CVS
 *
 * Revision 1.2  2003/07/02 12:55:14  mbranco
 * Several changes for AtCom
 *
 *
 * @author vberten
 * @author $Author: fjob $
 * @version $Revision: 1.1 $
 */

package gridpilot;

import java.io.*;

public interface ShellMgr {

  /**
   * Executes in the shell the command 'cmd', in the current directory, with
   * the current environment.
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
  public int exec(String[] cmd, StringBuffer stdOut, StringBuffer stdErr) throws IOException;

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
  public int exec(String[] cmd, String[] env, StringBuffer stdOut, StringBuffer stdErr) throws IOException;

  /**
   * Executes in the shell the command 'cmd', in the directory 'workingDirecory', with
   * the current environment.
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
  public int exec(String[] cmd, String workingDirectory,
                         StringBuffer stdOut, StringBuffer stdErr) throws IOException;


  /**
   * Executes in the shell the command 'cmd', in the directory 'workingDirecory', with
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
  public int exec(String[] cmd, String[] env, String workingDirectory,
                  StringBuffer stdOut, StringBuffer stdErr) throws IOException;


  /**
   * Creates a directory which doesn't exist, in the directory specified by parentDir,
   * and with a name beginning by prefix.
   * If the dircetory cannot be created, null is returned.
   * @return the name of a new directory, or null is this file
   * cannot be created
   *
   */
  public String createTempDir(String prefix, String parentDir);

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
   * Tests whether the file denoted by this abstract pathname is a
   * directory.
   */
  public boolean isDirectory(String dir);


  public void exit();


}
