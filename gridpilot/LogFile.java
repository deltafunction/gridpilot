package gridpilot;

import java.io.*;
import java.util.*;

/**
 * The LogFile class implements a tool allowing to records messages.
 * Theses messages can be simple error message, or exception message, error message related
 * to a specified job, exception message related to a specified job.
 * Each message contains a header with the type of this message, and the date.
 *
 */

public class LogFile {
  public class MyAction{
    public MyAction(){}
  }

  private Vector actionsOnMessages = new Vector();
  private static final int ERROR_MESSAGE = 0;
  private static final int INFORMATION_MESSAGE = 1;
  private static final int EXCEPTION_MESSAGE = 2;

  private static final String messagesSeparator = "*-------*";
  private static final String headerSeparator = "-----";

  private String fileName;
  private RandomAccessFile file;

  private String getMessageTypeName(int type){
    switch(type){
      case ERROR_MESSAGE : return "Error";
      case INFORMATION_MESSAGE : return "Information";
      case EXCEPTION_MESSAGE : return "Exception";
      default : return "unknown type";
    }
  }

  private boolean printMessages = true;
  private boolean showMessages = false;
  private boolean debugMode = true;

  public LogFile(String fileName) {
    this.fileName = fileName;
  }

  /**
   * Gets the first message in this logs file.
   * 
   * @return a String which contains the first message of this log file, or null
   * if file cannot be open, or is empty
   */
  public String getFirstMessage(){
    if(!openFile(false))
      return null;

    return getNextMessage();
  }

  /**
   * Gets the next message in this logs file.
   *
   * @return a String which contains the next message, or null if there is no more message
   */
  public String getNextMessage(){
    try{
      String line = file.readLine();
      if(line == null)
        return null;
      String res = "";

      while(line != null && !line.trim().equals(messagesSeparator)){
        // read file until next separator is reached
        res += line + "\n";
        line = file.readLine();
      }

      return res;

    }catch(IOException ioe){
      ioe.printStackTrace();
      return null;
    }
  }


  /**
   * Adds an error message related to a Throwable (Error or Exception).
   * msg is appended to the log file, as well as e stack trace
   */
  public void addMessage(String msg, Throwable t){
    StringWriter sw = new StringWriter();
    t.printStackTrace( new PrintWriter(sw));
    addMessage(msg + "\n" + (t instanceof Exception ? "Exception" : "Error") + " : \n" + sw, EXCEPTION_MESSAGE);

  }

  /**
   * Adds an error message related to a job (job).
   * msg is appended to the log file, as well as some job informations
   */
  public void addMessage(String msg, JobInfo job){
    addMessage(msg + "\nJob : \n" + job, ERROR_MESSAGE);
  }

  /**
   * Adds an error message related to a job (job) and a Throwable (Error or Exception).
   * msg is appended to the log file, as well as some job information and e stack trace
   */

  public void addMessage(String msg, JobInfo job, Throwable t){
    StringWriter sw = new StringWriter();
    t.printStackTrace( new PrintWriter(sw));

    addMessage(msg + "\nJob : \n" + job + "\n" +
               (t instanceof Exception ? "Exception" : "Error") + " : \n" + sw, EXCEPTION_MESSAGE);
  }


  /**
   * Adds an error message.
   * msg is appended to the log file
   */

  public void addMessage(String s){
    addMessage(s, ERROR_MESSAGE);
  }

  public void addInfo(String s){
    addMessage(s, INFORMATION_MESSAGE);
  }

  /**
   * Add a message s.
   * A header is add before s, with this message type and date.
   */
  private synchronized void addMessage(String s, int type){


    String message;
    String header = "";

    // header :

    // 1. type
    String msgType = "Type = " + getMessageTypeName(type);

    header += msgType + "\n";

    // 2. date
    String date = "Date = " + Calendar.getInstance().getTime().toString();

    header += date + "\n";

    // 3. Debug
    if(debugMode){

      Throwable t = new Throwable();
      String className = t.getStackTrace()[2].getClassName();
      className = className.substring(className.lastIndexOf(".")+1);
      className += "." + t.getStackTrace()[2].getMethodName();
      className += ":" + t.getStackTrace()[2].getLineNumber();

      header += "Debug infos : " + className + "\n";

    }

    message = header;
    // 4. Separator

    message += headerSeparator + "\n";

    // Message :

    message += s + "\n" ;
    message += messagesSeparator + "\n";


    if(type != INFORMATION_MESSAGE){
      try {
        if (!openFile(true)) {
          System.err.println("Cannot add this message :\n" + s);
        }
        else {
          file.writeBytes(message);
          file.close();
        }
      }
      catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    if(printMessages)
      System.out.println(s);
    if(showMessages)
      MessagePane.showMessage(s, getMessageTypeName(type));

    Enumeration e = actionsOnMessages.elements();
    while(e.hasMoreElements()){
      ((ActionOnMessage)e.nextElement()).newMessage(header, s,
          type!=INFORMATION_MESSAGE);
    }
  }

  /**
   * Opens the file named configFileName.
   * If 'append' == true, file pointer is set to end.
   * @return true if opening was ok, false otherwise
   */
  private synchronized boolean openFile(boolean append){
    try{
      file = new RandomAccessFile(fileName, "rw");
    }catch(FileNotFoundException fnfe){
      System.err.println("cannot found file "+ fileName);
      return false;
    }
    try{
      if(append)
        file.seek(file.length());
      else
        file.seek(0);
    }catch(IOException ioe){
      System.err.println("LogFile : Exception during opening");
      ioe.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Clears this file.
   */
  public void clear(){
    try{
      file.setLength(0);
    }catch(IOException ioe){
      ioe.printStackTrace();
    }
  }

  public void addActionOnMessage(ActionOnMessage aom){
    actionsOnMessages.add(aom);
  }
}


