package gridpilot;

import java.io.File;


import com.jcraft.jsch.*;

import javax.swing.*;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.LogFile;
import gridfactory.common.SecureShell;

public class MySecureShell extends SecureShell{

  private ConfigFile configFile;
  
  private static final int MAX_SSH_LOGIN_ATTEMPTS = 3;

  public MySecureShell(String _host, String _user, File _keyFile, String _keyPassphrase) throws JSchException{
    super(_host, _user, _keyFile, _keyPassphrase, GridPilot.getClassMgr().getLogFile());
    prefix = "/tmp/GridPilot-job-";
  }
  
  public MySecureShell(String _host, String _user, String _password) throws JSchException{
    super(_host, _user, _password, GridPilot.getClassMgr().getLogFile());
    prefix = "/tmp/GridPilot-job-";
  }

  public MySecureShell(String _host, String _user, String _password,
      File _keyFile, String _keyPassphrase) throws JSchException{
    super(_host, _user, _password, _keyFile, _keyPassphrase, GridPilot.getClassMgr().getLogFile());
    prefix = "/tmp/GridPilot-job-";
  }
  
  protected void connect(){
    configFile = GridPilot.getClassMgr().getConfigFile();
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
          MyUtil.arrayToString(new String [] {"User", "Key passphrase", "Host"})+" --> "+
          MyUtil.arrayToString(new String [] {user, (keyPassphrase==null?keyPassphrase:""), host}), 2);
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
                if(rep==MAX_SSH_LOGIN_ATTEMPTS-1){
                  if(GridPilot.splash!=null){
                    GridPilot.splash.hide();
                  }
                  MyUtil.showError("SSH login failed on "+host);
                }
                continue;
              }
            }
          }
        }
        try{
          if(keyFile!=null){
            try{
              jsch.addIdentity(keyFile.getAbsolutePath(), (keyPassphrase==null?keyPassphrase:""));
            }
            catch(Exception e){
              logFile.addMessage("Could not load SSH private key.", e);
              up = null;
            }
          }
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
            MyUtil.showError("SSH login failed on "+host);
          }
          password = null;
          continue;
        }
      }
      
      try{
        maxChannels = Integer.parseInt(
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
      sshs = new Channel[maxChannels];
    }
    catch (Exception e){
      Debug.debug("Could not connect via ssh, "+user+", "+password+", "+host+
          ". "+e.getMessage(), 1);
      e.printStackTrace();
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

}
