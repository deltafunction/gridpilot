package gridpilot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import com.jcraft.jsch.*;
import javax.swing.*;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.log4j.*;
import org.apache.log4j.Logger;
import org.safehaus.uuid.UUIDGenerator;

public class SecureShellMgr implements ShellMgr{

  private JSch jsch=new JSch();
  private Channel [] sshs;
  private String host;
  private String user;
  private String password;
  private File keyFile;
  private String keyPassphrase;
  private int port;
  private LogFile logFile;
  private ConfigFile configFile;
  private Session session;
  private int channels;
  private int channelsNum = 1;
  private String prefix = "/tmp/GridPilot-job-";
  
  private static final int MAX_SSH_LOGIN_ATTEMPTS = 3;

  public SecureShellMgr(String _host, String _user,
      File _keyFile, String _keyPassphrase){
    keyFile = _keyFile;
    keyPassphrase = _keyPassphrase;
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ERROR);
    host = _host;
    port = 22;
    if(host.indexOf(":")>0){
      port = Integer.parseInt(host.substring(host.indexOf(":")+1));
      host = host.substring(0, host.indexOf(":"));
    }
    user = _user;
    password = null;
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    connect();
    logFile.addInfo("Authentication completed on " + host + "(user : " + user +
        ", keyFile : " + keyFile + ")");
  }
  
  public SecureShellMgr(String _host, String _user,
      String _password){
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ERROR);
    host = _host;
    port = 22;
    if(host.indexOf(":")>0){
      port = Integer.parseInt(host.substring(host.indexOf(":")+1));
      host = host.substring(0, host.indexOf(":"));
    }
    user = _user;
    password = _password;
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    connect();
    logFile.addInfo("Authentication completed on " + host + "(user : " + user +
        ", password : " + password + ")");
  }

  public SecureShellMgr(String _host, String _user, String _password,
      File _keyFile, String _keyPassphrase){
    keyFile = _keyFile;
    keyPassphrase = _keyPassphrase;
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ERROR);
    host = _host;
    port = 22;
    if(host.indexOf(":")>0){
      port = Integer.parseInt(host.substring(host.indexOf(":")+1));
      host = host.substring(0, host.indexOf(":"));
    }
    user = _user;
    password = _password;
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    connect();
    logFile.addInfo("Authentication completed on " + host + "(user : " + user +
        ", privateKeyFile : " + keyFile + ")");
  }
  
  private void connect(){
    try{
      UserInfo ui = new MyUserInfo();
      boolean showDialog = true;
      // if global frame is set, this is a reload
      if(GridPilot.getClassMgr().getGlobalFrame()!=null){
        showDialog = false;
      }
      String [] up = null;
      for(int rep=0; rep<MAX_SSH_LOGIN_ATTEMPTS; ++rep){               
        if(showDialog ||
            user==null || (password==null && keyFile==null || keyPassphrase==null) || host==null){
          Debug.debug("Shell login:"+
          Util.arrayToString(new String [] {"User", "Key passphrase", "Host"})+" --> "+
          Util.arrayToString(new String [] {user, (keyPassphrase==null?keyPassphrase:""), host}), 2);
          // Only try private key once
          if(keyFile!=null && rep==0){
            up = GridPilot.userPwd("Shell login with private key on "+host,
                new String [] {"User", "Key passphrase", "Host"},
                new String [] {user, (keyPassphrase==null?keyPassphrase:""), host});
            if(up==null){
              return;
            }
            else{
              user = up[0].trim();
              keyPassphrase = up[1];
              host = up[2].trim();
              try{
                jsch.addIdentity(keyFile.getAbsolutePath(), (keyPassphrase==null?keyPassphrase:""));
              }
              catch(Exception e){
                logFile.addMessage("Could not load SSH private key.", e);
                up = null;
              }
            }
          }
          if(up==null || rep>0){
            up = GridPilot.userPwd("Shell login with password on "+host, new String [] {"User", "Password", "Host"},
                new String [] {user, password, host});
            if(up==null){
              return;
            }
            else{
              user = up[0].trim();
              password = up[1];
              host = up[2].trim();
              Debug.debug("SSH user: "+user+":", 2);
              if(user==null || user.equals("")){
                user = null;
                if(rep==MAX_SSH_LOGIN_ATTEMPTS-1){
                  if(GridPilot.splash!=null){
                    GridPilot.splash.hide();
                  }
                  Util.showError("SSH login failed on "+host);
                }
                continue;
              }
            }
          }
        }
        try{
          session = jsch.getSession(user, host, port);
          session.setHost(host);
          if(password!=null && !password.equals("")){
            session.setPassword(password);
          }
          session.setUserInfo(ui);
          java.util.Hashtable config = new java.util.Hashtable();
          config.put("StrictHostKeyChecking", "no");
          session.setConfig(config);
          if(GridPilot.splash!=null){
            GridPilot.splash.hide();
          }
          session.connect(30000);
          break;
        }
        catch(Exception e){
          if(rep==MAX_SSH_LOGIN_ATTEMPTS-1){
            if(GridPilot.splash!=null){
              GridPilot.splash.hide();
            }
            Util.showError("SSH login failed on "+host);
          }
          password = null;
          continue;
        }
      }
      
      try{
        channelsNum = Integer.parseInt(
            configFile.getValue("Computing systems", "maximum simultaneous submissions"))+
        Integer.parseInt(
            configFile.getValue("GridPilot", "maximum simultaneous checking"))+
            Integer.parseInt(
                configFile.getValue("Computing systems", "maximum simultaneous validating"));
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

  public boolean isConnected(){
    return session!=null && session.isConnected();
  }

  public void reconnect(){
    if(session!=null && session.isConnected()){
      session.disconnect();
    }
    connect();
   }

  public int exec(String cmd, StringBuffer stdOut, StringBuffer stdErr) throws IOException {
    return exec(cmd, null, null, stdOut, stdErr);
  }

  public int exec(String cmd, String [] env, String workingDirectory,
      StringBuffer stdOut, StringBuffer stdErr) throws IOException{
    
    if(env!=null){
      cmd = Util.arrayToString(env, "; ")+";"+cmd;
    }
    if(workingDirectory!=null){
      cmd = "cd "+workingDirectory+";"+cmd;
    }
    
    Debug.debug("Executing command "+cmd, 1);
    
    int exitStatus = 0;
    ChannelExec channel = null;
    String error = "";
    try{
      channel = getChannel();
      InputStream in = channel.getInputStream();
      InputStream err = channel.getErrStream();
      byte[] tmp=new byte[1024];
      byte[] tmp1=new byte[1024];
      ((ChannelExec) channel).setCommand(cmd);
      channel.connect();
      while(true){
        while(in.available()>0){
          int i=in.read(tmp, 0, 1024);
          if(i<0){
            break;
          }
          stdOut.append(new String(tmp, 0, i));
        }
        while(err.available()>0){
          int j=err.read(tmp1, 0, 1024);
          if(j<0){
            break;
          }
          stdErr.append(new String(tmp1, 0, j));
        }
        if(channel.isClosed()){
          break;
        }
        try{
          Thread.sleep(1000);
          }
        catch(Exception ee){        
        }
      }
      if(stdOut.length()>0){
        stdOut.delete(stdOut.length()-1, stdOut.length());
      }
      if(stdErr.length()>0){
        stdErr.delete(stdErr.length()-1, stdErr.length());
      }
      Debug.debug("Command stdout: "+stdOut.toString(), 3);
      exitStatus = channel.getExitStatus();
    }
    catch(Exception e){
      error = e.getMessage();
      exitStatus = -1;
    }
    finally{
      try{
        channel.disconnect();
      }
      catch(Exception e){
      }
    }
    if(exitStatus!=0 || stdErr!=null && !stdErr.toString().equals("")){
      Debug.debug("WARNING: error executing command " + cmd + " : " +
                         stdErr+":"+error, 2);
    }
    return(exitStatus);
  }

  public String readFile(String path) throws IOException {
    Debug.debug("reading file "+path, 2); 
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    int ret = exec("cat "+path, stdOut, stdErr);
    if(ret!=0 || stdErr.length()>0){
      throw new IOException("Could not read file "+path+". "+stdErr);
    }
    return stdOut.toString();
  }

  public void writeFile(String name, String content, boolean append) throws IOException {
    Debug.debug("writing file " + name, 2);
    name = getFullPath(name);
    
    /*StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    String op = ">";
    if(append){
      op = ">>";
    }
    content = content.replaceAll("'", "\\\\'");
    int ret = exec("echo '"+content+"'"+op+name, stdOut, stdErr);
    if(ret!=0 || stdErr.length()>0){
      throw new IOException("Could not write to file "+name+". "+stdErr);
    }*/

    InputStream is = new ByteArrayInputStream(content.getBytes());
    BufferedReader in = new BufferedReader(new InputStreamReader(is));
    File tmpFile = File.createTempFile("GridPilot-", "");
    PrintWriter out = new PrintWriter(new FileWriter(tmpFile)); 
    String line;
    while((line = in.readLine())!=null){
      out.println(line);
    }
    in.close();
    out.close();
    Util.dos2unix(tmpFile);
    upload(tmpFile.getAbsolutePath(), name);
    tmpFile.delete(); 
  }
  
  /**
   * Upload file on ssh server.
   * 
   * @param lFile    local file of the form /dir/file or c:\dir\file
   * @param rFile    remote file name of the form /dir/file, or dir/file
   */
  public boolean upload(String lFile, String rFile){
    rFile = getFullPath(rFile);
    FileInputStream is = null;
    Channel channel = null;
    try{
      // exec 'scp -t rfile' remotely
      String command = "scp -p -t "+rFile;
      channel = getChannel();
      ((ChannelExec) channel).setCommand(command);

      // get I/O streams for remote scp
      OutputStream out = channel.getOutputStream();
      InputStream in = channel.getInputStream();

      channel.connect();

      if(checkAck(in)!=0){
        logFile.addMessage("ERROR: could not copy file "+lFile+"->"+user+"@"+host+":"+rFile);
        return false;
      }

      // send "C0644 filesize filename", where filename should not include '/'
      long filesize = (new File(lFile)).length();
      command = "C0644 "+filesize+" ";
      if(lFile.lastIndexOf('/')>0){
        command += lFile.substring(lFile.lastIndexOf('/')+1);
      }
      else{
        command += lFile;
      }
      command += "\n";
      out.write(command.getBytes());
      out.flush();
      if(checkAck(in)!=0){
        logFile.addMessage("ERROR: could not copy file "+lFile+"->"+user+"@"+host+":"+rFile);
        return false;
      }

      // send content of lfile
      is = new FileInputStream(lFile);
      byte[] buf = new byte[1024];
      while(true){
        int len = is.read(buf, 0, buf.length);
        if(len<=0) break;
        out.write(buf, 0, len); //out.flush();
      }
      is.close();
      is = null;
      // send '\0'
      buf[0] = 0;
      out.write(buf, 0, 1);
      out.flush();
      if(checkAck(in)!=0){
        logFile.addMessage("ERROR: could not copy file "+lFile+"->"+user+"@"+host+":"+rFile);
        return false;
      }
      out.close();
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not copy file "+lFile+"->"+user+"@"+host+":"+rFile, e);
      e.printStackTrace();
      try{
        if(is!=null)is.close();
      }
      catch(Exception ee){
      }
      return false;
    }
    finally{
      try{
        channel.disconnect();
      }
      catch(Exception e){
      }
    }
    return true;
  }

  public boolean download(String rFile, String lFile){
    rFile = getFullPath(rFile);
    FileOutputStream fos = null;
    Channel channel = null;
    try{
      String prefix = null;
      if(new File(lFile).isDirectory()){
        prefix = lFile+File.separator;
      }
      // exec 'scp -f rfile' remotely
      String command = "scp -f "+rFile;
      channel = getChannel();
      ((ChannelExec)channel).setCommand(command);
      // get I/O streams for remote scp
      OutputStream out = channel.getOutputStream();
      InputStream in = channel.getInputStream();
      channel.connect();
      byte[] buf = new byte[1024];
      // send '\0'
      buf[0] = 0;
      out.write(buf, 0, 1);
      out.flush();
      while(true){
        int c = checkAck(in);
        if(c!='C'){
          break;
        }
        // read '0644 '
        in.read(buf, 0, 5);
        long filesize = 0L;
        while(true){
          if(in.read(buf, 0, 1)<0){
            // error
            break; 
          }
          if(buf[0]==' '){
            break;
          }
          filesize = filesize*10L+(long)(buf[0]-'0');
        }
        String file = null;
        for(int i=0;;i++){
          in.read(buf, i, 1);
          if(buf[i]==(byte)0x0a){
            file = new String(buf, 0, i);
            break;    
          }
        }
        Debug.debug("filesize="+filesize+", file="+file, 2);
        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();
        // read content of lfile
        fos = new FileOutputStream(prefix==null ? lFile : prefix+file);
        int foo;
        while(true){
          if(buf.length<filesize){
            foo = buf.length;
          }
          else{
            foo = (int)filesize;
          }
          foo = in.read(buf, 0, foo);
          if(foo<0){
            // error 
            break;
          }
          fos.write(buf, 0, foo);
          filesize-=foo;
          if(filesize==0L){
            break;
          }
        }
        fos.close();
        fos = null;
        if(checkAck(in)!=0){
          return false;
        }
        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not copy file "+user+"@"+host+":"+rFile+"->"+lFile, e);
      e.printStackTrace();
      try{
        if(fos!=null){
          fos.close();
        }
      }
      catch(Exception ee){
      }
      return false;
    }
    finally{
      try{
        channel.disconnect();
      }
      catch(Exception e){
      }
    }
    return true;
  }

  private static int checkAck(InputStream in) throws IOException{
    int b = in.read();
    // b may be 0 for success,
    //          1 for error,
    //          2 for fatal error,
    //          -1
    if(b==0){
      return b;
    }
    if(b==-1){
      return b;
    }

    if(b==1 || b==2){
      StringBuffer sb = new StringBuffer();
      int c;
      do{
        c = in.read();
        sb.append((char)c);
      }
      while(c!='\n');
      if(b==1){ // error
        Debug.debug(sb.toString(), 2);
      }
      if(b==2){ // fatal error
        Debug.debug(sb.toString(), 1);
      }
    }
    return b;
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
        if(ret!=0 || stdErr!=null && stdErr.length()>0){
          Debug.debug("WARNING: problem getting home directory. "+stdErr, 3);
        }
        Debug.debug("Got home directory: "+stdOut.toString(), 3);
      }
      catch(IOException e){
        e.printStackTrace();
        Debug.debug(e.getMessage(), 1);
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
      int ret = exec("ls -d "+name, stdOut, stdErr);
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
    if(dirPath==null || dirPath.equals("")){
      dirFullPath = "";
    }
    else{
      dirFullPath = getFullPath(dirPath);
    }
    if(dirFullPath.endsWith("/")){
      dirFullPath = dirFullPath.substring(0, dirFullPath.length()-1);
    }
    Debug.debug("dirFullPath : " + dirFullPath, 2);
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      int ret = 0;
      String parentDir = "";
      if(isDirectory(dirFullPath)){
        parentDir = dirFullPath;
      }
      else{
        int lastSlash = dirFullPath.lastIndexOf("/");
        if(lastSlash>0){
          parentDir = dirFullPath.substring(0, lastSlash);
        }
      }
      ret = exec("cd "+parentDir+"; ls "+dirFullPath+" | awk '{print ENVIRON[\"PWD\"]\"/\"$1}'", stdOut, stdErr);
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
    if(dir==null){
      return false;
    }
    Debug.debug("checking directory " + dir, 2);
    dir = getFullPath(dir);
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      int ret = exec("ls -pd "+dir+" | grep '.*/$'", stdOut, stdErr);
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
    try{
      ChannelExec channel = null;
      int maxTries = 5;
      int count = 0;
      boolean channelOk = false;
      while(true){
        // wait for first free channel
        for(int i=0; i<=channels; ++i){
          if(sshs[i]==null || sshs[i].isClosed()){
            sshs[i] = session.openChannel("exec");
            channel = (ChannelExec) sshs[i];
            channelOk = true;
            break;
          }
        }
        if(channelOk){
          break;
        }
        Debug.debug("WARNING: no free ssh channels, waiting for commands to finish...", 2);
        Thread.sleep(3000);
        ++count;
        if(count>maxTries-1){
          java.util.Random r = new java.util.Random(19580427);
          int rn = (r.nextInt() & Integer.MAX_VALUE) % sshs.length;
          Debug.debug("WARNING: liberating ssh channel #"+rn+" by force...", 2);
          sshs[rn].disconnect();
        }
        if(count>maxTries){
          break;
        }
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
    // first write small script containing the command script
    String uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
    writeFile(prefix+uuid+".sh", cmd+" 2> "+stdErrFile+" > "+stdOutFile+" &", false);
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    // execute the script
    exec("sh "+prefix+uuid+".sh", null, workingDirectory, stdOut, stdErr);
    stdOut = new StringBuffer();
    stdErr = new StringBuffer();
    exec("rm "+prefix+uuid+".sh; head -1 "+stdOutFile, stdOut, stdErr);
    String pid = stdOut.toString().trim();
    if(stdErr!=null && stdErr.length()>0 || stdOut==null || stdOut.length()==0){
      throw new IOException("ERROR: could not get PID for job "+cmd+". "+stdErr);
    }
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
      // TODO: it seems to get the parent process of
      // /bin/sh /afs/cern.ch/user/f/fjob/GridPilot/jobs/testDataset.00001/testDataset.00001.sh
      exec("ps -p "+id+" -o comm=", stdOut, stdErr);
      out = stdOut.toString();
    }
    catch(IOException e){
      e.printStackTrace();
    }
    return (out!=null && !out.equals(""));
  }

  public String getUserName(){
    return user;
  }
  
  public String getHostName(){
    return host;
  }
  
  public int getJobsNumber(){
    // TODO: check
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      int ret = exec("ps auxw | grep "+prefix+" | grep -v grep | wc -l", stdOut, stdErr);
      if(ret!=0 || stdErr.length()>0){
        Debug.debug("ERROR: could not find process number."+stdErr, 3);
        return -1;
      }
      int retInt = Integer.parseInt(stdOut.toString());
      return retInt;
    }
    catch(IOException e){
      logFile.addMessage("ERROR: could not find process number.", e);
      Debug.debug(e.getMessage(), 2);
      return -1;
    }
  }

}
