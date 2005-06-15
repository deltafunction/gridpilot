package gridpilot;

import java.io.*;
import java.util.*;

public class LocalShellMgr implements ShellMgr {

  public LocalShellMgr() {
  }

  /**
   * copy file 'src' to file 'dest'. <br>
   * Creates parents directory if needed. <br>
   * @return  <code>true</code> if and only if the move succeeded;
   *          <code>false</code> otherwise
   *
   */
  public boolean copyFile(String src, String dest) {
    if (src.equals(dest))
      return true;

    File destFile = new File(dest);
    if(destFile.getParent() != null &&  !destFile.getParentFile().exists())
      if(!destFile.getParentFile().mkdirs()){
        Debug.debug("cannot create parent directory for " + dest, 3);
        return false;
      }


    String[] cmd = {"cp", src, dest};
//    String [] cmd = {"mv", src.getAbsolutePath(), dest.getAbsolutePath()};
    StringBuffer stdErr = new StringBuffer();
    StringBuffer stdOut = new StringBuffer();

    try {
      exec(cmd, stdOut, stdErr);
      if (stdErr.length() != 0) {
        GridPilot.getClassMgr().getLogFile().addMessage(
            "Error during move : \n" +
            "\tCommand\t: " + arrayToString(cmd) + "\n" +
            "\tStdOut\t: " + stdOut + "\n" +
            "\tStdErr\t: " + stdErr);
        return false;
      }

    }
    catch (IOException e) {
      GridPilot.getClassMgr().getLogFile().addMessage(
          "IOExeption during move : \n" +
          "\tCommand\t: " + arrayToString(cmd), e);
      return false;

    }
    return true;
  }

  /**
   * Converts an array of object in a String representing this array. <p>
   * Example : {"a", new Integer(32), "ABCE", null} is converted in "a 32 ABCE null"
   */
  public static String arrayToString(Object[] values) {
    String res = "";
    if (values == null)
      return "(null)";
    for (int i = 0; i < values.length; ++i) {
      res += (values[i] == null ? "null" : values[i].toString()) + " ";
    }
    return res;
  }

  /**
   * Executes in the shell the command 'cmd', in the current directory, with
   * the current environment. <br>
   * Standard output is written in stdOut (if stdOut != null) <br>
   * Standard error is written in stdErr (if stdErr != null) <br>
   * <p>
   * All elements of cmd can be <code>null</code>, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return exit value of the command
   * @throws IOException
   */
  public int exec(String[] cmd, StringBuffer stdOut, StringBuffer stdErr) throws
      IOException {
    return exec(cmd, null, null, stdOut, stdErr);
  }

  /**
   * Executes in the shell the command 'cmd', in the current directory, with
   * the environment 'env'. <br>
   * Standard output is written in stdOut (if stdOut != null) <br>
   * Standard error is written in stdErr (if stdErr != null) <br>
   * <p>
   * All elements of cmd can be <code>null</code>, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return exit value of the command
   * @throws IOException
   */
  public int exec(String[] cmd, String[] env, StringBuffer stdOut,
                         StringBuffer stdErr) throws IOException {
    return exec(cmd, env, null, stdOut, stdErr);
  }

  /**
   * Executes in the shell the command 'cmd', in the directory 'workingDirecory', with
   * the current environment. <br>
   * Standard output is written in stdOut (if stdOut != null) <br>
   * Standard error is written in stdErr (if stdErr != null) <br>
   * <p>
   * All elements of cmd can be <code>null</code>, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return exit value of the command
   * @throws IOException
   */
  public int exec(String[] cmd, String workingDirectory,
                         StringBuffer stdOut, StringBuffer stdErr) throws
      IOException {
    return exec(cmd, null, workingDirectory, stdOut, stdErr);
  }

  /**
   * Executes in the shell the command 'cmd', in the directory 'workingDirecory', with
   * the environment 'env'. <br>
   * Standard output is written in stdOut (if stdOut != null) <br>
   * Standard error is written in stdErr (if stdErr != null) <br>
   * <p>
   * All elements of cmd can be <code>null</code>, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return exit value of the command
   * @throws IOException
   */
  public int exec(String[] cmd, String[] env, String workingDirectory,
                         StringBuffer stdOut, StringBuffer stdErr) throws
      IOException {
    cmd = convert(cmd);
    int exitValue;
    Process p = Runtime.getRuntime().exec(cmd, env,
                                          (workingDirectory == null ? null : new File(workingDirectory)));

    waitOutputs(p, stdOut, stdErr);

    try {
      exitValue = p.waitFor();
    }
    catch (InterruptedException ie) {
      System.err.println("InterruptedException in Utils.exec : " +
                         "\tCommand : " + arrayToString(cmd) + "\n" +
                         "\tException : " + ie.getMessage() + "\n" +
                         "\texit value set to -1");
      exitValue = -1;
    }

    return exitValue;
  }

  /**
   * Removes all null elements, tokenizes all composit (which contains spaces) elements
   */
  private static String[] convert(String[] cmd) {
    Vector vectorRes = new Vector();

    for (int i = 0; i < cmd.length; ++i) {
      if (cmd[i] != null) {
        StringTokenizer st = new StringTokenizer(cmd[i]);

        while (st.hasMoreTokens())
          vectorRes.add(st.nextToken());
      }
    }

    String[] arrayRes = new String[vectorRes.size()];
    for (int i = 0; i < arrayRes.length; ++i)
      arrayRes[i] = (String) vectorRes.get(i);

    return arrayRes;
  }

  /**
   * Copies 'p' outputs in stdOut and stdErr.
   * Can be called before the end of p, returns only when p is finished.
   */
  private static void waitOutputs(Process p, StringBuffer stdOut,
                                  StringBuffer stdErr) throws IOException {

    byte[] b = new byte[256];
    int nbRead;
    if (stdOut != null)
      while ( (nbRead = p.getInputStream().read(b)) != -1)
        stdOut.insert(0, new String(b, 0, nbRead));

    if (stdErr != null)
      while ( (nbRead = p.getErrorStream().read(b)) != -1)
        stdErr.insert(0, new String(b, 0, nbRead));
  }


  public String createTempDir(String prefix, String parentDir) {
    File workingDirectory = new File(parentDir);

    if (!workingDirectory.exists())
      if (!workingDirectory.mkdirs())
        return null;

    try {
      if(prefix.length() < 3)
        prefix = "./tmp";
      File dir = File.createTempFile(prefix, "", workingDirectory);

      if (!dir.delete()){
        Debug.debug("cannot delete file " + dir.getAbsolutePath(), 3);
        return null;
      }
      if (!dir.mkdirs()){
        Debug.debug("cannot create dir " + dir.getAbsolutePath(), 3);
        return null;
      }

      return dir.getAbsolutePath();

    }catch(IOException ioe){
      return null;
    }

  }

  public String readFile(String path) throws FileNotFoundException, IOException {
    RandomAccessFile f = new RandomAccessFile(path, "r");

    byte [] b  = new byte [(int)f.length()];
    f.readFully(b);

    String res = new String(b);
    f.close();
    return res;
  }

  public void writeFile(String name, String content, boolean append) throws IOException {
    Debug.debug("name : " + name + "\nappend : " + append + "\ncontent : \n" + content, 1);
    File parent = new File(name).getParentFile();
    if(parent != null && !parent.exists())
      parent.mkdirs();

    RandomAccessFile of = new RandomAccessFile(name, "rw");
    if(!append)
      of.setLength(0);
    of.writeBytes(content);
    of.close();
  }

  public boolean existsFile(String name){
    return new File(name).exists();
  }

  public boolean mkdirs(String dir){
    return new File(dir).mkdirs();
  }

  public boolean deleteFile(String path){
    return new File(path).delete();
  }

  public boolean moveFile(String src, String dest){
    File destFile = new File(dest);
    if(destFile.getParent() != null &&  !destFile.getParentFile().exists())
      if(!destFile.getParentFile().mkdirs()){
        Debug.debug("cannot create parent directory for " + dest, 3);
        return false;
      }
    return new File(src).renameTo(destFile);
  }

  public String[] listFiles(String dir){
    File fdir = new File(dir);
    File [] fres = fdir.listFiles();
    if(fres == null)
      return null;
    String [] sres = new String [fres.length];
    for(int i=0; i<fres.length ; ++i)
      sres[i] = fres[i].getAbsolutePath();
    return sres;
  }

  public boolean isDirectory(String dir){
    return new File(dir).isDirectory();
  }
  public void exit(){}
}