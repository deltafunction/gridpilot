package gridpilot;
import java.io.*;
import java.net.URL;
import java.util.*;

 /**
  *
  * This class manages access to configuration file.
  * This file is splitted in several sections.
  * - Each section begins by [sectionName] and ends by the begin of the next section or EOF
  * - Each section contains several attribute=value
  * 
  * Configuration file management is fully case insensitive
  * 
  */

public class ConfigFile{
  private String configFileName;
  private RandomAccessFile file;

  /**
   * Constructor. Initalizes this configuration file manager with the file 'confiFileName'
   */

  public ConfigFile(String configFileName){
    this.configFileName = configFileName;
    if(GridPilot.tmpConfFile==null){
    	makeTmpConfigFile();
    }
  }
  
  public void makeTmpConfigFile(){
  	try{
      // To be able to open with random access when running from a jar
      // we first extract the config file to a tmp file and then open
      // the tmp file.
      URL fileURL = getClass().getClassLoader().getResource(configFileName);
      //Debug.debug("fileURL: "+configFileName, 3);
      BufferedReader in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
    	GridPilot.tmpConfFile = File.createTempFile("GridPilot","conf");
      PrintWriter out = new PrintWriter(
      		new FileWriter(GridPilot.tmpConfFile)); 
      String line;
      while((line = in.readLine())!=null){
        out.println(line);
      }
      in.close();
      out.close();
    }
    catch(IOException e){
      Debug.debug("cannot find file "+ configFileName+". "+
      		e.getMessage(), 1);
    }
  }

  /**
   * public methods
   */

	/*
	 * tells if this is a real config or do we just use fake values. Returns true based on the file name
	 * */
	public boolean isFake(){
		return (configFileName.equals(""));
	}
  /**
   * Returns the first value of attribute "attribute" in the first section "section".
   * If section "[section]" contains
   * - attribute = a b <br>
   * - attribute = c
   * this function return "a b"
   *
   * If there is no such section or attribute in section, return null.
   * If file 'file' cannot be opened, return null.
   *
   * @return A String corresponding to this request, null if a such value cannot be found
   */
  public synchronized String getValue(String section, String attribute){

	  if(this.isFake()){
      return null;
    }
    
	  Debug.debug("getValue("+section+", "+attribute+")", 3);
    
    String result;
    if(!openFile()){
      return null;
    }
    if(searchSection(section)){
      result = searchAttribute(attribute);
    }
    else{
      result = null; // this section doesn't exist
    }
    if(result==null){
      Debug.debug("WARNING: The attribute "+attribute+
      		" is not defined in section "+section+
      		" of the config file.", 1);
    }
    try{
      file.close();
    }
    catch(IOException ioe){
      System.err.println("cannot close "+ configFileName);
    }
    if(result!=null && result.equals("\"\"")){
      Debug.debug("WARNING: Empty config value!", 2);
      result = "";
    }
    Debug.debug("got value: "+result, 3);
    return result;
  }

  /**
   * Returns all values of attribute "attribute" in the first section "section".
   * if section "[section]" contains :
   * - attribute = a b
   * - attribute = c
   * this function return {"a", "b", "c"}
   *
   * If there are no such attributes, returns a empty array (size = 0, new String[0])
   *
   * @return an array of String which contains all values matching this request
   */

  public synchronized String [] getValues(String section, String attribute){

    Vector l = new Vector();
    String res;
    if(!openFile())
      return null;

    if(searchSection(section)){
      do{
        res = searchAttribute(attribute);
        if(res == null){
          break;
        }
        StringTokenizer st = new StringTokenizer(res);
        while(st.hasMoreTokens()){
          l.add(st.nextToken());
        }
      }
      while(true);
    }

    String [] stringRes = new String[l.size()];
    for(int i=0; i<l.size(); ++i){
      stringRes[i] = l.elementAt(i).toString();
    }
    return stringRes;
  }


  /**
   * Opens the file named configFileName.
   * @return true if opening was ok, false otherwise
   */
  private synchronized boolean openFile(){
    try{
      file = new RandomAccessFile(GridPilot.tmpConfFile, "r");
    }
    catch(FileNotFoundException e){
      System.err.println("cannot find file "+ configFileName+". "+e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Searches for section "[section]" in file opened by openFile().
   * Returns true if "[section]" exists. The file pointer is located to first line after
   * line "[section]".
   * Returns false if section doesn't exist in file or if IOException occured.
   *
   * Called by
   * - this.getValue
   * - this.getValues
   * 
   * @return true if this section exist, false if this section doesn't exist or an
   * IOException occured
   */
  private synchronized  boolean searchSection(String section){
    String line;
    try{
      int begin;
      int end;
      do{
        line = readLine();
        if(line == null)
          break;
      }
      while(line!=null && (
            (begin = line.indexOf('['))==-1 ||
            (end = line.indexOf(']'))==-1 ||
            !line.substring(begin+1, end).trim().equalsIgnoreCase(section.trim())));

      if(line==null){
        return false;
      }

    }
    catch(IOException ioe){
      System.err.println("cannot read "+ configFileName);
      System.err.println(ioe.getMessage());
      return false;
    }
    return true;
  }


  /**
   * Search for next "attribute=value" in the current section.
   * Return a string which contains "value" if any value has been found, null otherwise
   *
   * Called by
   * - this.getValue
   * - this.getValues
   */
  private synchronized String searchAttribute(String attribute){

    String line;
    String res;
    int isIndex=0; // index of '='
    try{
      do{ // read the file until end of file is reached, next section is reached or
          // a suitable 'attribute = value' is found
        line = readLine();
      }
      while(line!=null && !line.startsWith("[") &&
            ((isIndex = line.indexOf('='))==-1 ||
            !line.substring(0, isIndex).trim().equalsIgnoreCase(attribute.trim())));

      if(line==null || line.trim().startsWith("[")){
        res = null;
      }
      else{
        res = line.substring(isIndex+1).trim();
        if (res.length() == 0)
          res = null; // case 'attribute = '
      }
    }
    catch(IOException ioe){
      System.err.println("cannot read "+ configFileName);
      res = null;
    }

    return res;
  }

  /**
   * Reads next line in file, remove all comments and commented lines, remove space
   * at the end
   */
  private String readLine() throws IOException{
    String res;
	  do{
      res= file.readLine();
      if(res!=null){
        if(res.indexOf('#')!=-1){
          // Allow \#, strip off the \
          if(res.indexOf('#')!=0 && res.indexOf('#')==res.indexOf('\\')+1){
            res = res.substring(0, res.indexOf('#')-1)+
            res.substring(res.indexOf('#'));           
          }
          else{
            res = res.substring(0, res.indexOf('#'));           
          }
        }
        res = res.trim();
      }
    }
    while(res!=null && res.length()==0);

    return res;
  }

  /**
   * Prints a message (getMissingMessage(section, attribute));
   */
  public void missingMessage(String section, String attribute){
    System.err.println(getMissingMessage(section, attribute));
  }

  /**
   * Gets a normalized message saying that 'attribute' doesn't exist in [section]
   */
  public String getMissingMessage(String section, String attribute){
    return "Attribute \n\t" +attribute + " = <attribute_value> \n"+
                       "missing in section [" + section + "] in configuration file ("+
                       configFileName + ")";
  }
}



