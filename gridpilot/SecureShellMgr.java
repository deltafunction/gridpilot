package gridpilot;

import java.io.IOException;
import java.io.FileNotFoundException;

import com.jcraft.jsch.*;
import javax.swing.*;

import java.io.*;

import org.apache.log4j.*;

public class SecureShellMgr implements ShellMgr{

  private JSch jsch=new JSch();
  private Channel [] sftps;
  private Channel [] sshs;
  private String remoteHome;
  private String host;
  private String user;
  private String password;
  private LogFile logFile;
  private ConfigFile configFile;
  private Session session;
  private String sshChannelsString;
  private int sshChannels;
  private String scpChannelsString;
  private int scpChannels;
  private int sshChannelInUse;
  private int scpChannelInUse;

  public SecureShellMgr(String _host, String _user,
      String _password, String _remoteHome) {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ERROR);
    host = _host;
    user = _user;
    password = _password;
    remoteHome = "";
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    sshChannelsString = configFile.getValue("gridpilot", "ssh channels");
    if(sshChannelsString == null){
      Debug.debug("ssh channels not found in config file", 1);
      sshChannels = 4;
    }
    else{
      sshChannels = Integer.parseInt(sshChannelsString);
    }
    scpChannelsString = configFile.getValue("gridpilot", "scp channels");
    if(sshChannelsString == null){
      Debug.debug("scp channels not found in config file", 1);
      sshChannels = 4;
    }
    else{
      scpChannels = Integer.parseInt(scpChannelsString);
    }
    remoteHome = getFullPath(_remoteHome);
    logFile.addInfo("Authentication completed on " + host + "(user : " + user +
                ", home : " + remoteHome + ")");
    sshChannelInUse = 0;
    scpChannelInUse = 0;
    connect();
  }

  private void connect(){
    try{
      session=jsch.getSession(user, host, 22);
      java.util.Hashtable config=new java.util.Hashtable();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      if(password!=null){
        session.setPassword(password);
      }
      else{
        UserInfo ui=new MyUserInfo();
        session.setUserInfo(ui);
      }
      session.connect();
      for(int i=0; i<sftps.length;++i){
        sftps[i] = session.openChannel("sftp");
        sftps[i].connect();
      }
      sshs = new Channel[sshChannels];
      for(int i=0; i<sshs.length;++i){
        sftps[i] = session.openChannel("ssh");
        sftps[i].connect();
      }
    }
    catch (Exception e){
      Debug.debug("Could not connect via ssh, "+user+", "+password+", "+host+
          "."+e.getMessage(), 1);
      e.printStackTrace();
    }
  }

  public void reconnect(){
    if(session.isConnected()){
      session.disconnect();
    }
    connect();
   }

  public int exec(String cmd, StringBuffer stdOut, StringBuffer stdErr) throws IOException {
    return exec(cmd, null, null, stdOut, stdErr);
  }

  public int exec(String cmd, String[] env, StringBuffer stdOut, StringBuffer stdErr) throws IOException {
    return exec(cmd, env, null, stdOut, stdErr);
  }

  public int exec(String cmd, String workingDirectory, StringBuffer stdOut, StringBuffer stdErr) throws IOException {
    return exec(cmd, null, workingDirectory, stdOut, stdErr);
  }

  public int exec(String cmd, String[] env, String workingDirectory,
      StringBuffer stdOut, StringBuffer stdErr) throws IOException {
    Debug.debug(cmd, 1);
    ChannelExec channel = getSshChannel();
    ((ChannelExec) channel).setCommand(cmd);
    // first cd to workingDirectory  
    if(workingDirectory != null){
      String cdCommand = "cd " + workingDirectory + "\n";
      channel.getOutputStream().write(cdCommand.getBytes());
      String error = readStdErr(scc);
      if(stdErr!=null)
        Debug.debug("Working directory (" + workingDirectory + ") cannot be used " +
            " for exec " + Util.arrayToString(cmd) + " : " +
            error, 2);
        logFile.addMessage("Working directory (" + workingDirectory + ") cannot be used " +
                           " for exec " + Util.arrayToString(cmd) + " : " +
                           error);
    }
    //env
    if(env != null){
      for(int i=0; i<env.length ; ++i){
        if(env[i].indexOf("=") != -1)
          try{
            scc.setEnvironmentVariable(env[i].substring(0, env[i].indexOf("=")),
                                       env[i].substring(env[i].indexOf("=") + 1));
          }catch(Exception e){
            Debug.debug("Could not execute command via ssh, "+user+", "+password+", "+host+
                "."+e.getMessage(), 1);
            e.printStackTrace();
          }
      }
    }

    String command = "";
    for(int i=0; i<cmd.length; ++i)
      if(cmd[i] != null)
        command += cmd[i] + " ";

    write(scc, command + "\n");
    write(scc, "echo .$?\n"); // exit value of the command

    StringBuffer tmpStdOut = new StringBuffer();
    tmpStdOut.insert(0,readStdOut(scc));

//    Debug.debug("tmpStdOut : "+ tmpStdOut, 3);

    int dot = tmpStdOut.lastIndexOf(".");

    String exit = tmpStdOut.substring(dot + 1, tmpStdOut.length()-1);

    int intExit;
    try{intExit = new Integer(exit).intValue();}
    catch(NumberFormatException e){
      e.printStackTrace();
      intExit = -1;
    }

    if(stdOut!=null)
      stdOut.insert(0, tmpStdOut.substring(0, dot));
    if(stdErr != null)
      stdErr.insert(0, readStdErr(scc));

    Debug.debug("stdOut : " + stdOut, 2);


    if(workingDirectory != null){
      write(scc, "cd - \n");
      readStdOut(scc);
    }

    releaseChannel(scc);
    return intExit;
  }

  private String readStdOut(SessionChannelClient scc) throws IOException{
    String endOfMessage = "BigRandomStringAZERTYStdOut";

    write(scc, "echo " + endOfMessage + "\n");

    String res = read(scc.getInputStream(), endOfMessage);

    Debug.debug("res of readStdOut : " + res,1);
    Debug.debug("end", 1);

    return res;
  }

  private String readStdErr(SessionChannelClient scc) throws IOException{
    Debug.debug("", 1);
    String endOfMessage = "BigRandomStringAZERTYStdErr";

    write(scc, "echo " + endOfMessage + " >&2\n");

    String res = "";
    try{res = read(scc.getStderrInputStream(), endOfMessage);
    }catch(Exception e){e.printStackTrace();};

    Debug.debug("res of readStdErr : " + res,1);
    Debug.debug("end", 1);

    return res;
  }

  private String read(InputStream is, String endOfMessage) throws IOException{
    int bufSize = 128;
    int maxBufSize = 1024 * 1024;
    String res = "";
    int size;

    byte b[] = new byte[bufSize];

    while ( (size = is.read(b)) > 0) {
      if (size == 0)
        break;
      String s = new String(b, 0, size);
      res += s;
      Debug.debug("last : " + b[size-1] + "(size : " + size + ")", 1);
      if (res.endsWith(endOfMessage + "\n")) {
        res = res.substring(0, res.length() - (endOfMessage.length()+1));
        break;
      }
      if(size == bufSize){
        bufSize = Math.min(bufSize * 2, maxBufSize);
        b = new byte[bufSize];
      }
      Debug.debug("size : " + size + ", bufSize : " + bufSize, 2);
    }

//    Debug.debug("res : " + res, 3);

    return res;
  }

  java.util.Random rand = new java.util.Random();
  /**
   * Be carefull, this method is not realy save
   */
  public synchronized String createTempDir(String prefix, String parentDir) {

    Debug.debug("prefix : " + prefix + ", parent : "+ parentDir, 2);
    int number = 1; // = rand.nextInt(1000)+1;
    int step = rand.nextInt(10) + 1;
    boolean isOk = false;
    if(!parentDir.endsWith("/"))
      parentDir+="/";
    String fullPrefix = getFullPath(parentDir + prefix);

    String res = "";

      while(existsFile(fullPrefix + number))
        number +=step;

      // at this step, the file (or directory) fullPrefix+number should not exist
      if(!mkdirs(fullPrefix + number)){
        logFile.addMessage("Cannot create temp dir with parent dir = " +
                           parentDir + " and prefix = " + prefix);
        return null;
      }
      return fullPrefix+number;

  }

  public synchronized String readFile(String path) throws FileNotFoundException, IOException {
    Debug.debug(path + "("+ path+")", 2);
//    SessionChannelClient scc = ssh.openSessionChannel();//*/getFileChannel();
//    SftpSubsystemClient sftp = new SftpSubsystemClient();
//    scc.startSubsystem(sftp);
//    String fullPath = getFullPath(path);

    SftpFile file = getSftp().openFile(getFullPath(path), SftpSubsystemClient.OPEN_READ);

    SftpFileInputStream in = new SftpFileInputStream(file);


    StringBuffer buf = new StringBuffer();
    int bufSize = 128;
    int maxBufSize = 256 * 256 + 1;
    int size;

    byte b[] = new byte[bufSize];

    while ( (size = in.read(b)) > 0) {
      buf.append(new String(b, 0, size));

      if(size == bufSize){
        bufSize = Math.min(bufSize * 2, maxBufSize);
        b = new byte[bufSize];
      }
      Debug.debug(path.hashCode() + " : size = " + size + ", bufSize = " + bufSize, 2);
    }

    file.close();
    return buf.toString();
  }

  public synchronized void writeFile(String name, String content, boolean append) throws IOException {
//    Debug.debug("File : " + name + ", content : "+ content + ", append : " + append  , 3);

    Debug.debug("file : " + name, 2);

//    SessionChannelClient scc = getFileChannel();
    name = getFullPath(name);
    SftpFile file;

    checkConnection();

    try{
      SftpSubsystemClient sftp = getSftp();
      file = sftp.openFile(name,
                           append ? (SftpSubsystemClient.OPEN_APPEND |  SftpSubsystemClient.OPEN_WRITE) :
                           (SftpSubsystemClient.OPEN_CREATE |  SftpSubsystemClient.OPEN_WRITE) );
      if(!append)
        sftp.changePermissions(file, "rw-r--r--");

    }catch(IOException e){
//      releaseFileChannel(scc);
      throw e;
    }

    SftpFileOutputStream out = new SftpFileOutputStream(file);
    out.write(content.getBytes());

//    releaseFileChannel(scc);
    file.close();


  }

  private String getFullPath(String name) {
    if(!name.startsWith("/") && !name.startsWith("~"))
      name = remoteHome + (remoteHome.endsWith("/") ? "" : "/") + name;
    if(name.startsWith("~")){
      try {name = getSftp().getDefaultDirectory() + name.substring(1);}
      catch (IOException e) {e.printStackTrace();}
    }
    return name;
  }

  private String getError(String cmd) {
    String res = null;
    SessionChannelClient scc = getChannel();
    try {
      write(scc, cmd + "\n");
      String stdErr = readStdErr(scc);
       if(stdErr.length() >0)
         return stdErr;

       readStdOut(scc);
       return null;
    }catch (IOException e) {
      e.printStackTrace();
      return e.getMessage();

    }finally{
    }
  }

  public boolean existsFile(String name){
    Debug.debug(name, 2);
    if(name ==null)
      return false;
    try{
      SftpSubsystemClient sftp = getSftp();
      sftp.closeFile(sftp.openFile(getFullPath(name), SftpSubsystemClient.OPEN_READ));
    }catch(IOException e){
      Debug.debug(e.getMessage(), 2);
      return false;
    }
    return true;
  }

  public synchronized boolean mkdirs(String dir) {
    Debug.debug(dir, 2);
    try {
      dir = getFullPath(dir);
      if(dir.endsWith("/"))
        dir = dir.substring(0, dir.length()-1);
      String parent = dir.substring(0, dir.lastIndexOf("/"));
      if(!existsFile(parent))
        mkdirs(parent);

      getSftp().makeDirectory(getFullPath(dir));
      return true;
    }
    catch (IOException e) {
      return false;
    }
  }

  public boolean deleteFile(String path) {
    Debug.debug(path, 2);
    try{
      getSftp().removeFile(getFullPath(path));
      return true;
    }catch(IOException e){
      return false;
    }
  }

  public boolean copyFile(String src, String dest) {
    Debug.debug(src + " -> " + dest, 2);

    // if the parent directory of the destination doesn't exist, it creates it
    String parent = dest.substring(0, dest.lastIndexOf("/"));
    if(!existsFile(parent))
      if(!mkdirs(parent)){
        logFile.addMessage("Error during copy (" + src + " -> " + dest  +
                           ") : Cannot create parent directory");
        return false;
      }
    String cmd = "cp -f -p " + src + " " + dest;
    String error = getError(cmd);
    if(error != null)
      logFile.addMessage("Error during copy (" + src + " -> " + dest  + ") : " + error +
                         "\nCommand : " + cmd);

    return error  == null;
  }

  public boolean moveFile(String src, String dest) {
    Debug.debug(src + " -> " + dest, 2);

    // if the parent directory of the destination doesn't exist, it creates it
    String parent = dest.substring(0, dest.lastIndexOf("/"));
    if(!existsFile(parent))
      if(!mkdirs(parent)){
        logFile.addMessage("Error during move (" + src + " -> " + dest  +
                           ") : Cannot create parent directory");
        return false;
      }
    try{getSftp().renameFile(src, dest);}
    catch(IOException ioe){
      logFile.addMessage("Cannot move " + src + " into " + dest + " : "+ioe.getMessage());
      return false;
    }

    return true;
  }

  public String [] listFiles(String dirPath){
//    SessionChannelClient scc=getFileChannel();
    try{
//      SftpSubsystemClient sftp = new SftpSubsystemClient();
//      scc.startSubsystem(sftp);
      checkConnection();
      String dirFullPath = getFullPath(dirPath);
//      Logger.getRootLogger().setLevel(Level.ALL);

      if(dirFullPath.endsWith("/"))
        dirFullPath = dirFullPath.substring(0, dirFullPath.length() -1);

      Debug.debug("dirFullPath : " + dirFullPath, 2);
      SftpSubsystemClient sftp = getSftp();
      SftpFile dir = sftp.openDirectory(dirFullPath);
//      SftpFile dir = sftp.openFile(dirFullPath, SftpSubsystemClient.OPEN_READ);
      java.util.Vector children = new java.util.Vector();

      while(sftp.listChildren(dir, children)>0)
        Debug.debug("Children.size : " + children.size(), 2);

      String [] res = new String[children.size()];

      for(int i=0; i<res.length; ++i)
        res[i] = ((SftpFile)children.get(i)).getAbsolutePath();

      dir.close();
      return res;

    }catch(IOException e){
      logFile.addMessage("Cannot list this directory ("+dirPath +")", e);
      return null;
    }


  }

  public boolean isDirectory(String dir){
//    SessionChannelClient scc=getFileChannel();
    try{
//      SftpSubsystemClient sftp = new SftpSubsystemClient();
//      scc.startSubsystem(sftp);
//      SftpFile file = sftp.openDirectory(getFullPath(dir));
      SftpFile file = getSftp().openFile(getFullPath(dir), SftpSubsystemClient.OPEN_READ);
      boolean res = file.isDirectory();
      file.close();
      return res;
    }catch(IOException e){
      logFile.addMessage("Exeption when trying to know if " + dir +
                         " was a directory", e);
      return false;
    }
  }



  public void exit(){
    Debug.debug("", 3);

    if(ssh.isConnected())
      ssh.disconnect();
  }

  synchronized private ChannelExec getSshChannel(){
    ChannelExec channel = null;
    try{
      // pick first unused channel
      for(int i=0; i<=sshChannels; ++i){
        if(sshs[i].isEOF()){
          channel = (ChannelExec) sshs[i];
        }
      }
      // all channels used, cycle through them
      if(channel==null){
        if(sshChannelInUse==sshChannels){
          sshChannelInUse=0;
        }
        else{
          ++sshChannelInUse;
        }
        channel = (ChannelExec) sshs[sshChannelInUse];
      }
      return (ChannelExec) channel;
    }
    catch(Exception e){
      Debug.debug("Could not get ssh channel. "+e.getMessage(), 1);
      e.printStackTrace();
      return null;
    }
  }

  int next = 0;
  private SftpSubsystemClient getSftp(){
    next = (next+1) % sftps.length;
    return sftps[next];
  }


  private void checkConnection(){
    for(int i=0; i<sftps.length ; ++i){
      if (sftps[i].isClosed())
        Debug.debug("stfp is closed !!!", 3);
//      if(sftpSessions[i].isClosed())
//        Debug.debug("stfpSession is closed !!!", 3);

    }
    if(!ssh.isConnected())
      Debug.debug("ssh is disconnected !!!", 3);

  }

  public static class MyUserInfo implements UserInfo{
    public String getPassword(){ return passwd; }
    public boolean promptYesNo(String str){
      Object[] options={ "yes", "no" };
      int foo=JOptionPane.showOptionDialog(null, 
             str,
             "Warning", 
             JOptionPane.DEFAULT_OPTION, 
             JOptionPane.WARNING_MESSAGE,
             null, options, options[0]);
       return foo==0;
    }

    String passwd;
    JTextField passwordField=(JTextField)new JPasswordField(20);

    public String getPassphrase(){ return null; }
    public boolean promptPassphrase(String message){ return true; }
    public boolean promptPassword(String message){
      Object[] ob={passwordField}; 
      int result=JOptionPane.showConfirmDialog(null, ob, message,
                                               JOptionPane.OK_CANCEL_OPTION);
      if(result==JOptionPane.OK_OPTION){
        passwd=passwordField.getText();
        return true;
      }
      else{ 
        return false; 
      }
    }
    public void showMessage(String message){
      JOptionPane.showMessageDialog(null, message);
    }
  }
}
