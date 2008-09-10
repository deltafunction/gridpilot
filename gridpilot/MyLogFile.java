package gridpilot;

import gridfactory.common.LogFile;

import java.io.*;

public class MyLogFile extends LogFile {

  public MyLogFile(String arg0) {
    super(arg0);
  }

  /**
   * Adds an error message related to a job (job).
   * msg is appended to the log file, as well as some job informations
   */
  public void addMessage(String msg, MyJobInfo job){
    addMessage(msg + "\nJob : \n" + job, ERROR_MESSAGE);
  }

  /**
   * Adds an error message related to a job (job) and a Throwable (Error or Exception).
   * msg is appended to the log file, as well as some job information and e stack trace
   */

  public void addMessage(String msg, MyJobInfo job, Throwable t){
    if(this.isFake()){
      return;
    }
    StringWriter sw = new StringWriter();
    t.printStackTrace( new PrintWriter(sw));

    addMessage(msg + "\nJob : \n" + job + "\n" +
               (t instanceof Exception ? "Exception" : "Error") + " : \n" + sw, EXCEPTION_MESSAGE);
  }



}


