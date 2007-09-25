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
    
  /**
   * Implement this to be able to stop processes running in a thread,
   * i.e. stop the tread in a "soft" manner.
   */
  public void requestStop(){
  }
  
  /**
   * Get any exception that may have been thrown in this thread.
   * Assumes you have used setException.
   * @return an Exception
   */
  public Exception getException(){
    return ex;
  }

  /**
   * Sets an exception to be returned by getException().
   * @param _ex an Exception
   */
  public void setException(Exception _ex){
    ex = _ex;
  }

  /**
   * Stop requesting this thread to stop.
   */
  public void clearRequestStop(){
  }

  /**
   * Implement this to be able to get an integer result from the process running in the thread.
   * @return an integer
   */
  public int getIntRes(){
    throw new UnsupportedOperationException("getIntRes not implemented!");
  }
  
  /**
   * Implement this to be able to get a String result from the process running in the thread.
   * @return a String
   */
  public String getStringRes(){
    throw new UnsupportedOperationException("getStringRes not implemented!");
  }
  
  /**
   * Implement this to be able to get a String array result from the process running in the thread.
   * @return a String array
   */
  public String[] getString2Res(){
    throw new UnsupportedOperationException("getString2Res not implemented!");
  }
  
  /**
   * Implement this to be able to get a two-dimensional String array result from the process running in the thread.
   * @return a two-dimensional String array
   */
  public String[][] getString3Res(){
    throw new UnsupportedOperationException("getString2Res not implemented!");
  }
  
  /**
   * Implement this to be able to get a DBRecord result from the process running in the thread.
   * @return a DBRecord
   */
  public DBRecord getDBRecordRes(){
    throw new UnsupportedOperationException("getDBRes not implemented!");
  }
  
  /**
   * Implement this to be able to get a DBResult result from the process running in the thread.
   * @return a DBResult
   */
  public DBResult getDBResultRes(){
    throw new UnsupportedOperationException("getDB2Res not implemented!");
  }
  
  /**
   * Implement this to be able to get an boolean result from the process running in the thread.
   * @return a boolean
   */
  public boolean getBoolRes(){
    throw new UnsupportedOperationException("getBoolRes not implemented!");
  }
  
  /**
   * Implement this to be able to get an Vector result from the process running in the thread.
   * @return a Vector
   */
  public Vector getVectorRes(){
    throw new UnsupportedOperationException("getVectorRes not implemented!");
  }

  /**
   * Implement this to be able to get an ShellMgr result from the process running in the thread.
   * @return a ShellMgr
   */
  public ShellMgr getShellMgrRes(){
    throw new UnsupportedOperationException("getShellMgrRes not implemented!");
  }
  
}