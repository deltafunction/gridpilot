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
  * Configuration file management is fully case insensitive.
  * 
  * The hierarchy of items has three section levels:
  * GridPilot - module (Databases, ...) - plugin (My_DB_Local, ...)
  * 
  * Each section (on anylevel) can have configuration items.
  * These can be grouped by sectionGroups
  * 
  */

public class ConfigFile{
  private String configFileName;
  private RandomAccessFile file;
  private boolean inJar;
  private HashMap valueStrings;
  private HashMap valueArrays;
  /**
   * List of main section headers in config file
   */
  private String [] sections =
    {"File transfer systems", "Databases", "Computing systems"};
  /**
   * List of items that will not be included in the GUI editor.
   */
  private String [] excludeItems = {"Systems", "*field*", "class", "driver", "database",
      "parameters", "randomized", "* name", "* identifier"};
  /**
   * Tree of ConfigNodes
   */
  private ConfigNode configuration;
  private Vector sectionsVector;

  /**
   * Constructor. Initalizes this configuration file manager with the file 'configFile'
   */
  public ConfigFile(File configFile){
    configFileName = configFile.getAbsolutePath();
    inJar = false;
    valueStrings = new HashMap();
    valueArrays = new HashMap();
    sectionsVector = new Vector();
    Collections.addAll(sectionsVector, sections);
    configuration = new ConfigNode("GridPilot");
  }
  
  public ConfigNode getHeadNode(){
    return configuration;
  }
  
  /**
   * Debugging.
   */
  public void printConfig(){
    getSections();
    getHeadNode().printAll(0);
    System.exit(0);
  }

  /**
   * Reads the config file and constructs tree of ConfigNodes
   */
  public void getSections(){
    String line;
    String sectionName = null;
    String newSectionName = null;
    String nodeName = null;
    String value;
    String belowItemDescription = "";
    String aboveItemDescription = "";
    ConfigNode sectionNode = null;
    ConfigNode subSectionNode = null;
    ConfigNode groupNode = null;
    ConfigNode node;
    int begin;
    int end;
    int isIndex=0; // index of '='
    try{
      file.close();
    }
    catch(IOException ioe){
      Debug.debug("cannot close "+ configFileName, 1);
    }
    if(!openFile()){
      return;
    }

    try{
      do{ // read the file
        line = readLineWithDescriptions();
        if(line==null){
          break;
        }
        // Add group node to current subsection node.
        // Group nodes have no description.
        if(line.startsWith("##")){
          groupNode = new ConfigNode(line.substring(3).trim());
          if(subSectionNode!=null){
            subSectionNode.addNode(groupNode);
          }
          else if(sectionNode!=null){
            sectionNode.addNode(groupNode);
          }
          else{
            configuration.addNode(groupNode);
          }
          belowItemDescription = "";
          aboveItemDescription = "";
        }
        else if((begin = line.indexOf('['))!=-1 && (end = line.indexOf(']'))!=-1){
          groupNode = null;
          newSectionName = line.substring(begin+1, end).trim();
          // Add to the head node
          if(newSectionName.equalsIgnoreCase("GridPilot")){
            configuration.setDescription(aboveItemDescription);
            aboveItemDescription = "";
          }
          if(sectionName!=null && !checkExclude(sectionName)){
           // Add the just-finished section node
           if(sectionsVector.contains(sectionName) && sectionNode!=null){
              sectionNode.setDescription(belowItemDescription);
              configuration.addNode(sectionNode);
              belowItemDescription = "";
            }
            // Add the just-finished sub-section node
            else if(subSectionNode!=null){
              subSectionNode.setDescription(belowItemDescription);
              sectionNode.addNode(subSectionNode);
              belowItemDescription = "";
            }
            // Begin a new section node
            if(sectionsVector.contains(newSectionName)){
              sectionNode = new ConfigNode(newSectionName);
              subSectionNode = null;
            }
            // Begin a new sub-section node
            else{
              subSectionNode = new ConfigNode(newSectionName);
            }
          }
          sectionName = newSectionName;
        }
        else{
          // Allow escaped hashes in attribute=value lines
          // Description/comments: the convention is that
          // lines starting with '# ' are description lines,
          // lines starting with '#' are comments and will be ignored.
          if(line.indexOf('#')>-1 && line.indexOf('#')==line.indexOf('\\')+1){
            line = line.substring(line.indexOf('#')+1);
            aboveItemDescription += line;
            belowItemDescription += line;
          }
          isIndex = line.indexOf('=');
          // attribute = value
          if(isIndex>0){
            nodeName = line.substring(0, isIndex).trim();
            value = line.substring(isIndex+1).trim();
            node = new ConfigNode(nodeName);
            node.setValue(value);
            node.setDescription(belowItemDescription);
            if(groupNode!=null){
              aboveItemDescription = "";
              groupNode.addNode(node);
            }
            if(subSectionNode!=null){
              aboveItemDescription = "";
              subSectionNode.addNode(node);
            }
            else if(sectionNode!=null){
              aboveItemDescription = "";
              sectionNode.addNode(node);
            }
            else{
              configuration.addNode(node);
            }
            aboveItemDescription = "";
          }
        }
        //System.out.println("line --->"+line);
        //System.out.println("nodeName --->"+nodeName);
        //System.out.println("sectionName --->"+sectionName);
        //System.out.println("newSectionName --->"+newSectionName);
        //System.out.println("groupName --->"+groupNode);
        //System.out.println("belowItemDescription --->"+belowItemDescription);
        //System.out.println("aboveItemDescription --->"+aboveItemDescription);
      }
      while(line!=null);
    }
    catch(IOException ioe){
      Debug.debug("cannot read "+ configFileName, 1);
      sectionName = null;
    }
  }
    
  /**
   * Check if an item is to be excluded.
   * @param name String name of item
   * @return true if the item is to be excluded
   */
  private boolean checkExclude(String name){
    String regEx = null;
    for(int i=0; i<excludeItems.length; ++i){
      regEx = excludeItems[i].replaceAll("\\*", "\\.\\*");
      if(name.matches(regEx)){
        return true;
      }
    }
    return false;
  }

  /**
   * Constructor. Initalizes this configuration file manager with the file of name 'configFileName'
   */
  public ConfigFile(String _configFileName){
    configFileName = _configFileName;
    if(!GridPilot.tmpConfFile.containsKey(configFileName) ||
        GridPilot.tmpConfFile.get(configFileName)==null){
      makeTmpConfigFile();
      // caching
      valueStrings = new HashMap();
      valueArrays = new HashMap();
    }
    inJar = true;
  }
  
  public void makeTmpConfigFile(){
    InputStream is =  null;
    BufferedReader in = null;
    PrintWriter out = null;
    String prefix = "";
    String suffix = "";      
    String shortName = (new File(configFileName).getName());
    if(shortName.indexOf(".")>0){
      prefix = shortName.substring(0, shortName.indexOf(".")+1);
      suffix = shortName.substring(shortName.indexOf("."));
    }
    else{
      prefix = shortName;
    }      
    try{
      // First just see if we can open file
      try{
        in = new BufferedReader(new FileReader((new File(configFileName))));
      }
      catch(Exception e){
        // To be able to open with random access when running from a jar
        // we first extract the config file to a tmp file and then open
        // the tmp file.
        URL fileURL = getClass().getClassLoader().getResource(configFileName);
        // If file.conf is used, create temp file with prefix file and suffix conf.
        Debug.debug("fileURL: "+configFileName+":"+prefix+":"+suffix+":"+fileURL, 3);
        is = fileURL.openStream();
        in = new BufferedReader(new InputStreamReader(is));
      }
      GridPilot.tmpConfFile.put(configFileName, File.createTempFile(prefix, suffix));
      out = new PrintWriter(new FileWriter((File) GridPilot.tmpConfFile.get(configFileName))); 
      String line;
      while((line = in.readLine())!=null){
        out.println(line);
      }
      in.close();
      out.close();
    }
    catch(IOException e){
      try{
        in.close();
        out.close();
        is.close();
      }
      catch(Exception ee){
      }
      Debug.debug("cannot find file "+ configFileName+". "+
          e.getMessage(), 1);
      e.printStackTrace();
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
  public String getValue(String section, String attribute){

    if(this.isFake()){
      return null;
    }
    
    if(valueStrings.keySet().contains(section+"=="+attribute)){
      return (String) valueStrings.get(section+"=="+attribute);
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
      Debug.debug("cannot close "+ configFileName, 1);
    }
    if(result!=null && result.equals("\"\"")){
      Debug.debug("WARNING: Empty config value!", 2);
      result = "";
    }
    Debug.debug("got value: "+result, 3);
    valueStrings.put(section+"=="+attribute, result);
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

  public String [] getValues(String section, String attribute){

    if(this.isFake()){
      return null;
    }
    
    if(valueArrays.keySet().contains(section+"=="+attribute)){
      return (String []) valueArrays.get(section+"=="+attribute);
    }
    
    Debug.debug("getValues("+section+", "+attribute+")", 3);

    Vector l = new Vector();
    String res;
    if(!openFile()){
      return null;
    }
    if(searchSection(section)){
      do{
        res = searchAttribute(attribute);
        if(res==null){
          break;
        }
        StringTokenizer st = new StringTokenizer(res);
        while(st.hasMoreTokens()){
          l.add(st.nextToken());
        }
      }
      while(true);
    }
    
    try{
      file.close();
    }
    catch(IOException ioe){
      Debug.debug("cannot close "+ configFileName, 1);
    }

    String [] stringRes = new String[l.size()];
    for(int i=0; i<l.size(); ++i){
      stringRes[i] = l.elementAt(i).toString();
    }
    valueArrays.put(section+"=="+attribute, stringRes);
    return stringRes;
  }


  /**
   * Opens the file named configFileName.
   * @return true if opening was ok, false otherwise
   */
  private boolean openFile(){
    if(!inJar){
      // ~/.gridpilot
      try{
        file = new RandomAccessFile(configFileName, "r");
      }
      catch(FileNotFoundException e){
        // if not available, try and see if a tmp file has been saved
        Debug.debug("cannot find file "+ configFileName+". "+e.getMessage(), 1);
        return false;
      }
    }
    else{
      // see if a tmp file has been saved
      try{
        file = new RandomAccessFile((File) GridPilot.tmpConfFile.get(configFileName), "r");
      }
      catch(FileNotFoundException ee){
        Debug.debug("cannot find file "+ configFileName+". "+ee.getMessage(), 1);
        return false;
      }
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
  private boolean searchSection(String section){
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
      Debug.debug("cannot read "+ configFileName+". "+ioe.getMessage(), 1);
      ioe.printStackTrace();
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
  private String searchAttribute(String attribute){

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
        if(res.length()==0){
          res = null; // case 'attribute = '
        }
      }
    }
    catch(IOException ioe){
      Debug.debug("cannot read "+ configFileName, 1);
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
      res = file.readLine();
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
   * Reads next line in file, remove commented lines, remove space
   * at the end
   */
  private String readLineWithDescriptions() throws IOException{
    String res;
    do{
      res = file.readLine();
      if(res==null){
        break;
      }
      if(res!=null){
        // Commented out line - ignore
        if((res.indexOf('#')==0 && res.length()==1 ||
            res.indexOf('#')==0 && res.indexOf(' ')!=1) &&
            !res.matches("## .*")){
          res = null;
        }
        else{
          res = res.trim();
        }
      }
    }
    while(res==null);
    return res;
  }

  /**
   * Prints a message (getMissingMessage(section, attribute));
   */
  public void missingMessage(String section, String attribute){
    Debug.debug(getMissingMessage(section, attribute), 2);
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



