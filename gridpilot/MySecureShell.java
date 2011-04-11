package gridpilot;

import java.io.File;
import java.io.IOException;

import com.jcraft.jsch.*;

import javax.security.auth.login.LoginException;
import javax.swing.*;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.jobrun.SecureShell;

public class MySecureShell extends SecureShell{

  private ConfigFile configFile;
  
  // Try 3 times - getChannel tries MAX_GET_CHANNEL_TRIES times, so MAX_GET_CHANNEL_TRIES x 3 times in all...
  private static final int MAX_SSH_LOGIN_ATTEMPTS = 3;

  public MySecureShell(String _host, String _user, File _keyFile, String _keyPassphrase) throws JSchException, IOException{
    super(_host, _user, _keyFile, _keyPassphrase, GridPilot.getClassMgr().getLogFile());
    prefix = "/tmp/GridPilot-job-";
    MAX_GET_CHANNEL_TRIES = 1;
  }
  
  public MySecureShell(String _host, String _user, String _password) throws JSchException, IOException{
    super(_host, _user, _password, GridPilot.getClassMgr().getLogFile());
    prefix = "/tmp/GridPilot-job-";
    MAX_GET_CHANNEL_TRIES = 1;
  }

  public MySecureShell(String _host, String _user, String _password,
      File _keyFile, String _keyPassphrase) throws JSchException, IOException{
    super(_host, _user, _password, _keyFile, _keyPassphrase, GridPilot.getClassMgr().getLogFile());
    prefix = "/tmp/GridPilot-job-";
    MAX_GET_CHANNEL_TRIES = 1;
  }
  
  protected Session connect(Session session) throws IOException{
    Debug.debug("Connecting shell", 2);
    while(connecting){
      try{
        Thread.sleep(3000);
      }
      catch(InterruptedException e){
        return session;
      }
    }
    if(session!=null && session.isConnected()){
      return session;
    }
    connecting = true;
    configFile = GridPilot.getClassMgr().getConfigFile();
    try{
      doConnect(session);
    }
    finally{
      connecting = false;
    }
    return session;
  }

  private void doConnect(Session session) throws IOException {
    boolean showDialog = true;
    // if global frame is set, this is a reload
    if(GridPilot.getClassMgr().getGlobalFrame()!=null){
      showDialog = false;
    }
    for(int rep=0; rep<MAX_SSH_LOGIN_ATTEMPTS; ++rep){
      try{
        Debug.debug("SSH login attempt "+rep, 2);
        singleConnect(showDialog, rep, session);
        break;
      }
      catch(LoginException e){
        e.printStackTrace();
        if(rep<MAX_SSH_LOGIN_ATTEMPTS-1){
          try{
            Thread.sleep(10000);
          }
          catch(InterruptedException e1) {
            e1.printStackTrace();
            break;
          }
        }
        continue;
      }
    }
    try{
      int newMaxChannels = 3 + Integer.parseInt(
          configFile.getValue("Computing systems", "max simultaneous submissions"))+
      Integer.parseInt(
          configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "max simultaneous checking"))+
          Integer.parseInt(
              configFile.getValue("Computing systems", "max simultaneous validating"));
      if(newMaxChannels>maxChannels){
        maxChannels = newMaxChannels;
      }
    }
    catch(Exception e){
      Debug.debug("WARNING: could not construct number of channels. "+
          e.getMessage(), 1);
    }      
  }

  private void singleConnect(boolean showDialog, int rep, Session session) throws LoginException, IOException {
    String [] up = null;

    if(showDialog ||
        user==null || (password==null && (keyFile==null || keyPassphrase==null)) || host==null){
      Debug.debug("Shell login:"+showDialog+":"+
         MyUtil.arrayToString(new String [] {"User", "password", "Host"})+" --> "+
         MyUtil.arrayToString(new String [] {user, (password==null?"":password), host}), 2);
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
            getJsch().addIdentity(keyFile.getAbsolutePath(), (keyPassphrase==null?keyPassphrase:""));
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
        keyFile = null;
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
            if(rep>=MAX_SSH_LOGIN_ATTEMPTS-1){
              if(GridPilot.SPLASH!=null){
                GridPilot.SPLASH.hide();
              }
              MyUtil.showError("SSH login failed on "+host);
            }
            throw new IOException("SSH login failed on "+host);
          }
        }
      }
    }
    try{
      if(keyFile!=null){
        try{
          getJsch().addIdentity(keyFile.getAbsolutePath(), (keyPassphrase==null?keyPassphrase:""));
        }
        catch(Exception e){
          logFile.addMessage("Could not load SSH private key.", e);
          up = null;
        }
      }
      //session = getJsch().getSession(user, host, port);
      session.setHost(host);
      if(password!=null && !password.equals("")){
        session.setPassword(password);
      }
      setSessionUI(session);
      java.util.Hashtable<String, String> config = new java.util.Hashtable<String, String>();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      if(GridPilot.SPLASH!=null){
        GridPilot.SPLASH.hide();
      }
      // Get rid of popups - well, doesn't seem to work...
      session.setX11Host(null);
      session.connect(30000);
      return;
    }
    catch(Exception e){
      e.printStackTrace();
      if(rep==MAX_SSH_LOGIN_ATTEMPTS-1){
        if(GridPilot.SPLASH!=null){
          GridPilot.SPLASH.hide();
        }
        MyUtil.showError((rep+1)+" SSH login(s) failed on "+host);
      }
      else{
        try{
          Thread.sleep(10000L);
        }
        catch(InterruptedException e1) {
          e1.printStackTrace();
        }
      }
      password = null;
      throw new LoginException();
    }
  }

  private void setSessionUI(final Session session) {
    if(SwingUtilities.isEventDispatchThread()){
      UserInfo ui = new MyUserInfo();
      session.setUserInfo(ui);
    }
    else{
      try{
        SwingUtilities.invokeAndWait(
          new Runnable(){
            public void run(){
              UserInfo ui = new MyUserInfo();
              session.setUserInfo(ui);
            }
          }
        );
      }
      catch(Exception e){
        e.printStackTrace();
      }
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
    JTextField passwordField = (JTextField) new JPasswordField(20);

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
