package gridpilot;

import java.io.*;
import java.util.*;

public class LocalStaticShellMgr{

  public LocalStaticShellMgr(){
  }

  /**
   * copy file 'src' to file 'dest'. <br>
   * Creates parents directory if needed. <br>
   * @return  <code>true</code> if and only if the move succeeded;
   *          <code>false</code> otherwise
   *
   */
  public static boolean copyFile(String _src, String _dest){
    String src = Util.clearFile(_src);
    String dest = Util.clearFile(_dest);
    Debug.debug("copying file "+src+"->"+dest, 3);
    if(src.equals(dest)){
      return true;
    }

    File destFile = new File(dest);
    if(destFile.getParent()!=null &&  !destFile.getParentFile().exists())
      if(!destFile.getParentFile().mkdirs()){
        Debug.debug("cannot create parent directory for " + dest, 3);
        return false;
      }
      
    try{
      File srcFile = new File(src);
      // Copies src file to dst file.
      // If the dst file does not exist, it is created
      InputStream in = new FileInputStream(srcFile);
      OutputStream out = new FileOutputStream(destFile);
  
      // Transfer bytes from in to out
      byte[] buf = new byte[1024];
      int len;
      try{
        while ((len = in.read(buf))>0) {
          out.write(buf, 0, len);
      }
      in.close();
      out.close();
      }
      catch(IOException e){
        Debug.debug(
            "FileNotFoundException during copy : \n", 1);
        return false;
      }
    }
    catch(FileNotFoundException e){
      Debug.debug(
          "FileNotFoundException during copy : \n", 2);
      return false;
    }
    return true;    
  }

  /**
   * Executes in the shell the command 'cmd', in the current directory, with
   * the current environment. <br>
   * Standard output is written in stdOut (if stdOut!=null) <br>
   * Standard error is written in stdErr (if stdErr!=null) <br>
   * <p>
   * All elements of cmd can be <code>null</code>, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return exit value of the command
   * @throws IOException
   */
  public static int exec(String cmd, StringBuffer stdOut, StringBuffer stdErr) throws
      IOException {
    return exec(cmd, null, null, stdOut, stdErr);
  }

  /**
   * Executes in the shell the command 'cmd', in the directory 'workingDirecory', with
   * the current environment. <br>
   * Standard output is written in stdOut (if stdOut!=null) <br>
   * Standard error is written in stdErr (if stdErr!=null) <br>
   * <p>
   * All elements of cmd can be <code>null</code>, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return exit value of the command
   * @throws IOException
   */
  public static int exec(String cmd, String workingDirectory,
                         StringBuffer stdOut, StringBuffer stdErr) throws
      IOException {
    return exec(cmd, null, workingDirectory, stdOut, stdErr);
  }

  /**
   * Executes in the shell the command 'cmd', in the directory 'workingDirecory', with
   * the environment 'env'. <br>
   * Standard output is written in stdOut (if stdOut!=null) <br>
   * Standard error is written in stdErr (if stdErr!=null) <br>
   * <p>
   * All elements of cmd can be <code>null</code>, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return exit value of the command
   * @throws IOException
   */
  public static int exec(String cmd, String[] env, String workingDirectory,
                         StringBuffer stdOut, StringBuffer stdErr) throws
      IOException {
    cmd = Util.arrayToString(convert(Util.split(cmd)));
    int exitValue;
    Debug.debug("executing "+cmd, 3);
    Process p = Runtime.getRuntime().exec(cmd, env,
       (workingDirectory==null ? null : new File(workingDirectory)));

    waitOutputs(p, stdOut, stdErr);

    try{
      exitValue = p.waitFor();
    }
    catch(InterruptedException ie){
      GridPilot.getClassMgr().getLogFile().addMessage("InterruptedException in Utils.exec : " +
                         "\tCommand : " + cmd + "\n" +
                         "\tException : " + ie.getMessage() + "\n" +
                         "\texit value set to -1", ie);
      exitValue = -1;
    }

    return exitValue;
  }

  /**
   * Removes all null elements, tokenizes all composit (which contains spaces) elements
   */
  private static String[] convert(String[] cmd){
    Vector vectorRes = new Vector();

    for (int i = 0; i < cmd.length; ++i){
      if (cmd[i]!=null){
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
    if(stdOut!=null){
      while((nbRead=p.getInputStream().read(b))!=-1){
        stdOut.insert(0, new String(b, 0, nbRead));
      }
    }
    if(stdErr!=null){
      while((nbRead=p.getErrorStream().read(b))!=-1){
        stdErr.insert(0, new String(b, 0, nbRead));
      }
    }
  }

  public static String readFile(String _path) throws FileNotFoundException, IOException {
    String path = Util.clearFile(_path);
    Debug.debug("Reading file "+path, 2);
    RandomAccessFile f = new RandomAccessFile(path, "r");
    byte [] b  = new byte [(int)f.length()];
    f.readFully(b);
    String res = new String(b);
    f.close();
    return res;
  }

  public static void writeFile(String _name, String content, boolean append) throws IOException {
    String name = Util.clearFile(_name);
    Debug.debug("name : " + name + "\nappend : " + append + "\ncontent : \n" + content, 1);
    File parent = new File(name).getParentFile();
    if(parent!=null && !parent.exists()){
      parent.mkdirs();
    }
    RandomAccessFile of = new RandomAccessFile(name, "rw");
    if(!append){
      of.setLength(0);
    }
    of.writeBytes(content);
    of.close();
  }

  public static boolean existsFile(String name){
    return new File(name).exists();
  }

  public static boolean mkdirs(String _dir){
    String dir = Util.clearFile(_dir);
    Debug.debug("making dirs "+dir, 3);
    return new File(dir).mkdirs();
  }

  
  // Deletes all files and subdirectories under dir.
  // Returns true if all deletions were successful.
  // If a deletion fails, the method stops attempting to delete and returns false.
  public static boolean deleteDir(File dir){
    if(dir.isDirectory()){
      String[] children = dir.list();
      for(int i=0; i<children.length; i++){
        boolean success = deleteDir(new File(dir, children[i]));
        if(!success){
          return false;
        }
      }
    }
    // The directory is now empty so delete it
    return dir.delete();
  }

  public static boolean deleteFile(String _path){
    String path = Util.clearFile(_path);
    Debug.debug("deleting file "+path, 3);
    File dirOrFile = new File(path);
    if(dirOrFile.isDirectory()){
      return deleteDir(dirOrFile);
    }
    else{
      return dirOrFile.delete();
    }
  }

  public static boolean moveFile(String _src, String _dest){
    String src = Util.clearFile(_src);
    String dest = Util.clearFile(_dest);
    Debug.debug("moving file "+src+"->"+dest, 3);
    File destFile = new File(dest);
    if(destFile.getParent()!=null &&  !destFile.getParentFile().exists())
      if(!destFile.getParentFile().mkdirs()){
        Debug.debug("cannot create parent directory for " + dest, 3);
        return false;
      }
    return new File(src).renameTo(destFile);
  }

  public static String[] listFiles(String dir){
    File fdir = new File(dir);
    File [] fres = fdir.listFiles();
    if(fres==null){
      return null;
    }
    String [] sres = new String [fres.length];
    for(int i=0; i<fres.length ; ++i){
      sres[i] = fres[i].getAbsolutePath();
      if(!sres[i].endsWith("/") && isDirectory(sres[i])){
        sres[i] = sres[i]+"/";
      }
    }
    Arrays.sort(sres);
    return sres;
  }

  public static boolean isDirectory(String dir){
    return new File(dir).isDirectory();
  }
  
  private static int MAX_DEPTH = 10;
  
  private static HashSet listFilesRecursively(File fileOrDir, HashSet files, int depth){
    Debug.debug("Listing "+fileOrDir.getAbsolutePath()+":"+fileOrDir.isFile()+":"+
        fileOrDir.isDirectory()+":"+depth, 3);
    if(fileOrDir.isFile()){
      files.add(fileOrDir);
    }
    if(fileOrDir.isDirectory() && depth<=MAX_DEPTH){
      File [] dirContents = fileOrDir.listFiles(); // List of files/dirs.
      Arrays.sort(dirContents);
      for(int i=0; i<dirContents.length; ++i){
          listFilesRecursively(dirContents[i], files, depth+1); // Recursively list.
      }
    }
    return files;
  }
  
  public static HashSet listFilesRecursively(String fileOrDir){
    return listFilesRecursively(new File(fileOrDir), new HashSet(), 1);
  }

  /**
   * Executes in the shell the command 'cmd', in the directory 'workingDirecory'. <br>
   * Standard output is written to the file stdOutFile.
   * Standard error is written to the file stdErrFile.
   * <p>
   * All elements of cmd can be <code>null</code>, or contain several tokens ;
   * if an element of cmd contains spaces, all "words" are used like as much as
   * differents parameters.
   *
   * @return hash code of the started process
   * @throws IOException
   */
  public HashMap processes = new HashMap();
  
  public Process getProcess(String cmd){
    Process ret = null;
    try{
      ret = (Process) processes.get(cmd);
    }
    catch(Exception e){
      Debug.debug("Could not get process for "+cmd, 2);
    }
    return ret;
  }

  public void addProcess(String cmd, Process process){
    processes.put(cmd, process);
  }

  public void removeProcess(String cmd){
    processes.remove(cmd);
  }

  public Process submit(final String cmd, final String workingDirectory,
                  final String stdOutFile, final String stdErrFile) throws Exception {
    Debug.debug("executing "+cmd, 3);

    Thread runThread = new Thread(){
      public void run(){
        try{
          
          final Process proc = Runtime.getRuntime().exec(cmd, null, 
              (workingDirectory==null ? null : new File(workingDirectory)));
          
          FileOutputStream fos = new FileOutputStream(stdOutFile);
          StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "OUTPUT", fos);
            
          FileOutputStream fes = new FileOutputStream(stdErrFile);
          StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "ERROR", fes);            

          errorGobbler.start();
          outputGobbler.start();
                                
          addProcess(cmd, proc);

          proc.waitFor();

          fos.flush();
          fos.close(); 
          fes.flush();
          fes.close();
          
          removeProcess(cmd);
        }
        catch(Exception ie){
          GridPilot.getClassMgr().getLogFile().addMessage("Exception in LocalStaticShellMgr.exec : " +
                             "\tCommand : " + cmd + "\n" +
                             "\tException : " + ie.getMessage(), ie);
          ie.printStackTrace();
          interrupt();
        }
      }
    };
    
    Debug.debug("Running job: "+cmd, 2);
    runThread.start();
    
    for(int rep=0; rep<10; ++rep){
      Debug.debug("Sleeping 3 seconds...", 2);
      Thread.sleep(3000);
      if(getProcess(cmd)!=null){
        Debug.debug("Returning job: "+getProcess(cmd).hashCode(), 2);
        return getProcess(cmd);
      }
    }
    return null;
  }
  
  class StreamGobbler extends Thread{
    InputStream is;
    String type;
    OutputStream os;
    
    StreamGobbler(InputStream is, String type){
        this(is, type, null);
    }
    StreamGobbler(InputStream is, String type, OutputStream redirect){
        this.is = is;
        this.type = type;
        this.os = redirect;
    }     
      
    public void run(){
      try{
        PrintWriter pwo = null;
        if(os!=null){
          pwo = new PrintWriter(os);
        }
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line=null;
        while((line = br.readLine())!=null){
          if(pwo!=null){
            pwo.println(line);
          }
          System.out.println(type + ">" + line);    
        }
        if(pwo!=null){
          pwo.flush();
        }
      }
      catch (IOException ioe){
        ioe.printStackTrace();  
      }
    }
  }
  
}