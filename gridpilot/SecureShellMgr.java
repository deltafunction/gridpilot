package gridpilot;

import java.io.IOException;
import com.jcraft.jsch.*;
import javax.swing.*;
import java.io.*;
import org.apache.log4j.*;

public class SecureShellMgr implements ShellMgr{

  private JSch jsch=new JSch();
  private Channel [] sshs;
  private String remoteHome;
  private String host;
  private String user;
  private String password;
  private LogFile logFile;
  private ConfigFile configFile;
  private Session session;
  private int channels;
  private int channelInUse;

  public SecureShellMgr(String _host, String _user,
      String _password, String _remoteHome){
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ERROR);
    host = _host;
    user = _user;
    password = _password;
    remoteHome = _remoteHome;
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    channelInUse = 0;
    connect();
    logFile.addInfo("Authentication completed on " + host + "(user : " + user +
        ", password : " + password + ", home : " + remoteHome + ")");
  }

  private void connect(){
    try{
      session = jsch.getSession(user, host, 22);
      java.util.Hashtable config = new java.util.Hashtable();
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
      int channelsNum = 1;
      try{
      	channelsNum = Integer.parseInt(
            configFile.getValue("GridPilot", "maximum simultaneous submission"))+
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
      //remoteHome = getFullPath(remoteHome);
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
    return exec(cmd, null, stdOut, stdErr);
  }

  public int exec(String cmd, String workingDirectory,
      StringBuffer stdOut, StringBuffer stdErr) throws IOException{
    Debug.debug(cmd, 1);
    ChannelExec channel = getChannel();
    InputStream in = channel.getInputStream();
    InputStream err = channel.getErrStream();
    OutputStream out = channel.getOutputStream();
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
    if(!name.startsWith("/") && !name.startsWith("~"))
      name = remoteHome + (remoteHome.endsWith("/") ? "" : "/") + name;
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
      int ret = exec("rm "+path, stdOut, stdErr);
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
}
