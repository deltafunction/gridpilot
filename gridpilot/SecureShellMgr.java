package gridpilot;

import java.io.IOException;
import com.jcraft.jsch.*;
import javax.swing.*;
import java.io.*;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.log4j.*;
import org.safehaus.uuid.UUIDGenerator;

public class SecureShellMgr implements ShellMgr{

  private JSch jsch=new JSch();
  private Channel [] sshs;
  private String host;
  private String user;
  private String password;
  private LogFile logFile;
  private ConfigFile configFile;
  private Session session;
  private int channels;
  private int channelInUse;
  private int channelsNum = 1;

  public SecureShellMgr(String _host, String _user,
      String _password){
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ERROR);
    host = _host;
    user = _user;
    password = _password;
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    channelInUse = 0;
    connect();
    logFile.addInfo("Authentication completed on " + host + "(user : " + user +
        ", password : " + password + ")");
  }

  private void connect(){
    try{
      UserInfo ui = new MyUserInfo();
      boolean showDialog = true;
      // if global frame is set, this is a reload
      if(GridPilot.getClassMgr().getGlobalFrame()==null){
        showDialog = false;
      }
      boolean gridAuth = false;
      String [] up = null;
      for(int rep=0; rep<3; ++rep){
        if(showDialog ||
            user==null || (password==null && !gridAuth) || host==null){
          up = GridPilot.userPwd("Shell login on "+host, new String [] {"User", "Password", "Host"},
              new String [] {user, password, host});
          if(up==null){
            return;
          }
          else{
            user = up[0];
            password = up[1];
            host = up[2];
          }
        }
        try{
          session = jsch.getSession(user, host, 22);
          session.setHost(host);
          session.setPassword(password);
          session.setUserInfo(ui);
          java.util.Hashtable config = new java.util.Hashtable();
          config.put("StrictHostKeyChecking", "no");
          session.setConfig(config);
          session.connect();
          break;
        }
        catch(Exception e){
          password = null;
          continue;
        }
      }
      
      try{
        channelsNum = Integer.parseInt(
            configFile.getValue("GridPilot", "maximum simultaneous submissions"))+
        Integer.parseInt(
            configFile.getValue("GridPilot", "maximum simultaneous checking"))+
            Integer.parseInt(
                configFile.getValue("GridPilot", "maximum simultaneous validating"));
      }
      catch(Exception e){
        Debug.debug("WARNING: could not construct number of channels. "+
            e.getMessage(), 1);
      }      
      sshs = new Channel[channelsNum];
    }
    catch (Exception e){
      Debug.debug("Could not connect via ssh, "+user+", "+password+", "+host+
          ". "+e.getMessage(), 1);
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

  public int exec(String cmd, String [] env, String workingDirectory,
      StringBuffer stdOut, StringBuffer stdErr) throws IOException{
    Debug.debug(cmd, 1);
    ChannelExec channel = getChannel();
    InputStream in = channel.getInputStream();
    InputStream err = channel.getErrStream();
    byte[] tmp=new byte[1024];
    byte[] tmp1=new byte[1024];
    // first cd to workingDirectory  
    if(workingDirectory!=null){
      ((ChannelExec) channel).setCommand("cd "+workingDirectory);
      while(true){
        while(in.available()>0){
          int i=in.read(tmp, 0, 1024);
          if(i<0) break;
          stdOut.append(new String(tmp, 0, i));
        }
        while(err.available()>0){
          int j=err.read(tmp1, 0, 1024);
          if(j<0)break;
          stdErr.append(new String(tmp1, 0, j));
        }
        if(channel.isClosed()){
          break;
        }
      }
      int exitStatus = channel.getExitStatus();
      if(exitStatus!=0 || stdErr!=null){
        Debug.debug("Working directory (" + workingDirectory + ") cannot be used " +
            " for exec " + cmd + " : " +
            stdErr, 2);
        logFile.addMessage("Working directory (" + workingDirectory + ") cannot be used " +
                           " for exec " + cmd + " : " +
                           stdErr);
        channel.disconnect();
        return(exitStatus);
      }
    }
    
    // Environment
    if(env!=null){
      ((ChannelExec) channel).setCommand(Util.arrayToString(env, "; "));
      while(true){
        while(in.available()>0){
          int i=in.read(tmp, 0, 1024);
          if(i<0) break;
          stdOut.append(new String(tmp, 0, i));
        }
        while(err.available()>0){
          int j=err.read(tmp1, 0, 1024);
          if(j<0)break;
          stdErr.append(new String(tmp1, 0, j));
        }
        if(channel.isClosed()){
          break;
        }
      }
      int exitStatus = channel.getExitStatus();
      if(exitStatus!=0 || stdErr!=null){
        Debug.debug("Environment (" + Util.arrayToString(env, "; ") + ") cannot be used " +
            " for exec " + cmd + " : " +
            stdErr, 2);
        logFile.addMessage("Environment (" + Util.arrayToString(env, "; ") + ") cannot be used " +
                           " for exec " + cmd + " : " +
                           stdErr);
        channel.disconnect();
        return(exitStatus);
      }
    }

    ((ChannelExec) channel).setCommand(cmd);
    while(true){
      while(in.available()>0){
        int i=in.read(tmp, 0, 1024);
        if(i<0) break;
        stdOut.append(new String(tmp, 0, i));
      }
      while(err.available()>0){
        int j=err.read(tmp1, 0, 1024);
        if(j<0)break;
        stdErr.append(new String(tmp1, 0, j));
      }
      if(channel.isClosed()){
        Debug.debug("exit-status: "+channel.getExitStatus(), 2);
        break;
      }
    }
    return(channel.getExitStatus());
  }

  public synchronized String readFile(String path) throws IOException {
    Debug.debug("reading file "+path, 2); 
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    int ret = exec("cat "+path, stdOut, stdErr);
    if(ret!=0 || stdErr.length()>0){
      throw new IOException("Could not read file "+path+". "+stdErr);
    }
    return stdOut.toString();
  }

  public synchronized void writeFile(String name, String content, boolean append) throws IOException {
    Debug.debug("writing file " + name, 2);
    name = getFullPath(name);
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    String op = ">";
    if(append){
      op = ">>";
    }
    int ret = exec("echo "+content+op+name, stdOut, stdErr);
    if(ret!=0 || stdErr.length()>0){
      throw new IOException("Could not write to file "+name+". "+stdErr);
    }
  }

  private String getFullPath(String name){
    if(!name.startsWith("/") && !name.startsWith("~")){
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();
      try{
        int ret = exec("echo $PWD", stdOut, stdErr);
        if(ret!=0 || stdErr.length()>0){
          Debug.debug("ERROR: cannot get current directory. "+stdErr, 3);
        }
      }
      catch(IOException e){
        Debug.debug(e.getMessage(), 2);
      }
      name = stdOut.toString()+name.substring(1);
    }
    if(name.startsWith("~")){
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();
      try{
        int ret = exec("echo $HOME", stdOut, stdErr);
        if(ret!=0 || stdErr.length()>0){
          Debug.debug("ERROR: cannot get home directory. "+stdErr, 3);
        }
      }
      catch(IOException e){
        Debug.debug(e.getMessage(), 2);
      }
      name = stdOut.toString()+name.substring(1);
   }
    return name;
  }

  public boolean existsFile(String name){
    if(name ==null){
      return false;
    }
    Debug.debug("checking file " + name, 2);
    name = getFullPath(name);
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      int ret = exec("ls "+name, stdOut, stdErr);
      if(ret!=0 || stdErr.length()>0){
        Debug.debug("WARNING: file "+name+" does not exist. "+stdErr, 3);
        return false;
      }
    }
    catch(IOException e){
      Debug.debug(e.getMessage(), 2);
      return false;
    }
    return true;
  }

  public synchronized boolean mkdirs(String dir){
    if(dir ==null){
      return false;
    }
    Debug.debug("creating dir " + dir, 2);
    dir = getFullPath(dir);
    if(dir.endsWith("/")){
      dir = dir.substring(0, dir.length()-1);
    }
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      int ret = exec("mkdir -p "+dir, stdOut, stdErr);
      if(ret!=0 || stdErr.length()>0){
        Debug.debug("WARNING: directory "+dir+" could not be created. "+stdErr, 2);
        return false;
      }
    }
    catch(IOException e){
      Debug.debug(e.getMessage(), 2);
      return false;
    }
    return true;
  }

  public boolean deleteFile(String path){
    Debug.debug("deleting file "+path, 2); 
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      int ret = exec("rm -f "+path, stdOut, stdErr);
      if(ret!=0 || stdErr.length()>0){
        Debug.debug("WARNING: could not delete file "+path+". "+stdErr, 1);
        return false;
      }
    }
    catch(IOException e){
      Debug.debug(e.getMessage(), 2);
      return false;
    }
    return true;
  }

  public boolean deleteDir(String path){
    if(!isDirectory(path)){
      return false;
    }
    Debug.debug("deleting directory "+path, 2); 
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      int ret = exec("rm -rf "+path, stdOut, stdErr);
      if(ret!=0 || stdErr.length()>0){
        Debug.debug("WARNING: could not delete directory "+path+". "+stdErr, 1);
        return false;
      }
    }
    catch(IOException e){
      Debug.debug(e.getMessage(), 2);
      return false;
    }
    return true;
  }

  public boolean copyFile(String src, String dest){
    Debug.debug("copying file "+src+"->"+dest, 2); 
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    String cmd = "cp -f -p " + src + " " + dest;
    // if the parent directory of the destination doesn't exist, it creates it
    String parent = dest.substring(0, dest.lastIndexOf("/"));
    if(!existsFile(parent)){
      if(!mkdirs(parent)){
        logFile.addMessage("Error during copy (" + src + " -> " + dest  +
                           ") : Cannot create parent directory");
        return false;
      }
    }
    try{
      int ret = exec(cmd, stdOut, stdErr);
      if(ret!=0 || stdErr.length()>0){
        logFile.addMessage("Error during copy (" + src + " -> " + dest  + ") : " +
            stdErr + "\nCommand : " + cmd);
        Debug.debug("Could not copy file "+src+"->"+dest+". "+stdErr, 1);
        return false;
      }
    }
    catch(IOException e){
      Debug.debug(e.getMessage(), 2);
      return false;
    }
    return true;
  }

  public boolean moveFile(String src, String dest){
    Debug.debug("moving file "+src+"->"+dest, 2); 
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    String cmd = "mv " + src + " " + dest;
    // if the parent directory of the destination doesn't exist, it creates it
    String parent = dest.substring(0, dest.lastIndexOf("/"));
    if(!existsFile(parent)){
      if(!mkdirs(parent)){
        logFile.addMessage("Error during move (" + src + " -> " + dest  +
                           ") : Cannot create parent directory");
        return false;
      }
    }
    try{
      int ret = exec(cmd, stdOut, stdErr);
      if(ret!=0 || stdErr.length()>0){
        logFile.addMessage("Error during move (" + src + " -> " + dest  + ") : " +
            stdErr + "\nCommand : " + cmd);
        Debug.debug("Could not move file "+src+"->"+dest+". "+stdErr, 1);
        return false;
      }
    }
    catch(IOException e){
      Debug.debug(e.getMessage(), 2);
      return false;
    }
    return true;
  }

  public String [] listFiles(String dirPath){
    String dirFullPath = "";
    if(dirPath ==null || dirPath.equals("")){
      dirFullPath = "";
    }
    else{
      dirFullPath = getFullPath(dirPath);
    }
    if(dirFullPath.endsWith("/")){
      dirFullPath = dirFullPath.substring(0, dirFullPath.length() -1);
    }
    Debug.debug("dirFullPath : " + dirFullPath, 2);
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      int ret = exec("ls "+dirFullPath+" | awk '{print ENVIRON[\"PWD\"]\"/\"$1}'", stdOut, stdErr);
      if(ret!=0 || stdErr.length()>0){
        Debug.debug("directory "+dirFullPath+" does not exist. "+stdErr, 3);
        return new String [] {};
      }
    }
    catch(IOException e){
      Debug.debug(e.getMessage(), 2);
      return new String [] {};
    }
    return Util.split(stdOut.toString());
  }

  public boolean isDirectory(String dir){
    if(dir ==null){
      return false;
    }
    Debug.debug("checking directory " + dir, 2);
    dir = getFullPath(dir);
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      int ret = exec("ls -pd "+dir+"| grep'.*/$'", stdOut, stdErr);
      if(ret!=0 || stdErr.length()>0){
        Debug.debug("directory "+dir+" does not exist. "+stdErr, 3);
        return false;
      }
    }
    catch(IOException e){
      logFile.addMessage("Exeption when checking if " + dir +
          " was a directory", e);
      Debug.debug(e.getMessage(), 2);
      return false;
    }
    return(stdOut!=null && !stdOut.equals(""));
  }

  public void exit(){
    if(session.isConnected()){
      session.disconnect();
    }
  }

  synchronized private ChannelExec getChannel(){
    ChannelExec  channel = null;
    try{
      // pick first unused channel
      for(int i=0; i<=channels; ++i){
        if(sshs[i]==null || sshs[i].isClosed()){
          sshs[i] = session.openChannel("exec");
          if(!sshs[i].isConnected()){
            sshs[i].connect();
            channel = (ChannelExec) sshs[i];
            break;
          }
        }
        else if(sshs[i].isEOF()){
          sshs[i].disconnect();
          sshs[i].connect();
          channel = (ChannelExec) sshs[i];
          break;
        }
      }
      // all channels used, cycle through them
      if(channel==null){
        channelInUse = (channelInUse+1) % channels;
        channel = (ChannelExec) sshs[channelInUse];
      }
      return (ChannelExec) channel;
    }
    catch(Throwable e){
      Debug.debug("Could not get ssh channel. "+e.getMessage(), 1);
      e.printStackTrace();
      return null;
    }
  }

  public static class MyUserInfo implements UserInfo{
    public String getPassword(){
      return passwd;
    }
    public boolean promptYesNo(String str){
      Object[] options={"yes", "no"};
      int foo = JOptionPane.showOptionDialog(null, 
             str,
             "Warning", 
             JOptionPane.DEFAULT_OPTION, 
             JOptionPane.WARNING_MESSAGE,
             null, options, options[0]);
       return foo==0;
    }

    String passwd;
    JTextField passwordField=(JTextField)new JPasswordField(20);

    public String getPassphrase(){
      return null;
    }
    public boolean promptPassphrase(String message){
      return true;
    }
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
  
  public boolean isLocal(){
    return false;
  }
  
  private static int MAX_DEPTH = 10;
    
  private HashSet listFilesRecursively(String fileOrDir, HashSet files, int depth){
    if(!isDirectory(fileOrDir)){
      files.add(fileOrDir);
    }
    else if(depth<=MAX_DEPTH){
      String [] dirContents = listFiles(fileOrDir); // List of files/dirs.
      Arrays.sort(dirContents);
      for(int i=0; i<dirContents.length; ++i){
          listFilesRecursively(dirContents[i], files, depth+1); // Recursively list.
      }
    }
    return files;
  }
  
  public HashSet listFilesRecursively(String fileOrDir){
    return listFilesRecursively(fileOrDir, new HashSet(), 10);
  }

  public String submit(String cmd, String workingDirectory, String stdOutFile,
      String stdErrFile) throws Exception{
    // first write small script containing the command and returning the pid of itself
    String uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
    writeFile("/tmp/"+uuid+".sh",
        cmd+"2>"+stdOutFile+">"+stdErrFile+"&\n" +
        "echo $$", false);
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    // execute the script
    exec("/tmp/"+uuid+".sh", stdOut, stdErr);
    String pid = stdOut.toString();
    stdOut = new StringBuffer();
    stdErr = new StringBuffer();
    exec("rm /tmp/"+uuid+".sh", stdOut, stdErr);
    return pid;
  }

  public void killProcess(String id){
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      exec("kill "+id, stdOut, stdErr);
    }
    catch(IOException e){
      e.printStackTrace();
    }
  }

  public boolean isRunning(String id){
    String out = null;
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      exec("ps -p "+id+" -o comm=", stdOut, stdErr);
      out = stdOut.toString();
    }
    catch(IOException e){
      e.printStackTrace();
    }
    return (out!=null && !out.equals(""));
  }

}
