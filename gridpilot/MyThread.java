package gridpilot;

import java.util.Vector;

import gridpilot.DBResult;
import gridpilot.DBRecord;

public class MyThread extends Thread{
  
  private Exception ex;
  
  public void run(){
    ex = null;
    super.run();
  }
    
  public void requestStop(){
  }
  
  public Exception getException(){
    return ex;
  }

  public void setException(Exception _ex){
    ex = _ex;
  }

  public void clearRequestStop(){
  }

  public int getIntRes(){
    throw new UnsupportedOperationException("getIntRes not implemented!");
  }
  
  public boolean getBooleanRes(){
    throw new UnsupportedOperationException("getBooleanRes not implemented!");
  }
  
  public String getStringRes(){
    throw new UnsupportedOperationException("getStringRes not implemented!");
  }
  
  public String[] getString2Res(){
    throw new UnsupportedOperationException("getString2Res not implemented!");
  }
  
  public String[][] getString3Res(){
    throw new UnsupportedOperationException("getString2Res not implemented!");
  }
  
  public DBRecord getDBRes(){
    throw new UnsupportedOperationException("getDBRes not implemented!");
  }
  
  public DBResult getDB2Res(){
    throw new UnsupportedOperationException("getDB2Res not implemented!");
  }
  
  public boolean getBoolRes(){
    throw new UnsupportedOperationException("getBoolRes not implemented!");
  }
  
  public Vector getVectorRes(){
    throw new UnsupportedOperationException("getVectorRes not implemented!");
  }

  public ShellMgr getShellMgrRes(){
    throw new UnsupportedOperationException("getShellMgrRes not implemented!");
  }
  
}