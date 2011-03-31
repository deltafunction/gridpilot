package gridpilot.csplugins.glite;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.jobrun.ScriptGenerator;
import gridpilot.DBPluginMgr;
import gridpilot.GridPilot;
import gridpilot.MyComputingSystem;
import gridpilot.MyJobInfo;
import gridpilot.MyUtil;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class GLiteScriptGenerator extends ScriptGenerator {

  private String cpuTime = null;
  private String memory = null;
  private String reRun = null;
  private List<String> remoteInputFilesList = null;
  private List<String> lfcInputFilesList = null;
  private String [] uses = null;
  protected HashMap<String, String> reverseRteTranslationMap;
  protected HashMap<String, String> rteApproximationMap;
  private  DBPluginMgr dbPluginMgr = null;
  private MyJobInfo job = null;
  private String exeFileName = null;
  private String jdlFileName = null;
  private String [] outputFileNames = null;
  private String shortScriptName = null;
  private String jobDefID = null;
  private ConfigFile configFile;
  private String csName;
  private String[] clusters = null;
  private String[] excludedClusters = null;
  private boolean sendJobsToData = false;
  private boolean forceSpecificCatalog = false;
  private String lfcHost = null;

  // These files will be uploaded to the sandbox.
  protected List<String> localInputFilesList = null;

  public GLiteScriptGenerator(String _csName, MyJobInfo _job, String _exeFileName, String _jdlFileName) {
    super(GridPilot.getClassMgr().getLogFile(), false);
    csName = _csName;
    job = _job;
    exeFileName= _exeFileName;
    jdlFileName = _jdlFileName;
    configFile = GridPilot.getClassMgr().getConfigFile();
    cpuTime = configFile.getValue(csName, "CPU time");
    memory = configFile.getValue(csName, "Memory");
    reRun = configFile.getValue(csName, "Max rerun");
    clusters = configFile.getValues(csName, "Clusters");
    excludedClusters = configFile.getValues(csName, "Excluded clusters");
    reverseRteTranslationMap = GridPilot.getClassMgr().getReverseRteTranslationMap(csName);
    rteApproximationMap = GridPilot.getClassMgr().getRteApproximationMap(csName);
    String sendJobsToDataStr = GridPilot.getClassMgr().getConfigFile().getValue(csName, "Send jobs to data");
    if(sendJobsToDataStr==null || sendJobsToDataStr.equalsIgnoreCase("")){
      sendJobsToData = false;
    }
    else{
      sendJobsToData = ((sendJobsToDataStr.equalsIgnoreCase("yes")||
          sendJobsToDataStr.equalsIgnoreCase("true"))?true:false);
    }
    String forceSpecificCatalogStr = GridPilot.getClassMgr().getConfigFile().getValue(csName, "Force specific catalog");
    if(forceSpecificCatalogStr==null || forceSpecificCatalogStr.equalsIgnoreCase("")){
      forceSpecificCatalog = false;
    }
    else{
      forceSpecificCatalog = ((forceSpecificCatalogStr.equalsIgnoreCase("yes")||
          forceSpecificCatalogStr.equalsIgnoreCase("true"))?true:false);
    }
    localInputFilesList = new Vector<String>();
    remoteInputFilesList = new Vector<String>();
    lfcInputFilesList = new Vector<String>();
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    jobDefID = job.getIdentifier();
    uses = dbPluginMgr.getRuntimeEnvironments(jobDefID);
    outputFileNames = dbPluginMgr.getOutputFiles(jobDefID);
    shortScriptName = dbPluginMgr.getExecutableFile(jobDefID);
    int lastSlash = shortScriptName.replaceAll("\\\\", "/").lastIndexOf("/");
    if(lastSlash>-1){
      shortScriptName = shortScriptName.substring(lastSlash + 1);
    }
  }

  public void createScript() throws IOException{

    String [] formalParam = dbPluginMgr.getExecutableArguments(jobDefID);
    String [] actualParam = dbPluginMgr.getJobDefExecutableParameters(jobDefID);

    // create job script

    String scriptLine = "";
    // Header
    StringBuffer bufScript = new StringBuffer();
    writeLine(bufScript, "#!/bin/bash");
    writeLine(bufScript,"# Script generated by GridPilot/GLiteScriptGenerator");
    writeBlock(bufScript, csName + " wrapper script", 0, "# ");

    // Start time
    writeLine(bufScript, "START_TIME=`date '+%s'`");

    // Runtime environment dependencies. Text from runtimeEnvironment.initText
    writeBlock(bufScript, "runtime environment dependencies", 1, "# ");
    for(int i=0; i<uses.length; ++i){
      writeBlock(bufScript, "use "+ uses[i], 2, "# ");
      String initTxt = (String) dbPluginMgr.getRuntimeInitText(reverseApproximateRte(uses[i]), csName);
      if(initTxt!=null){
        writeLine(bufScript, MyUtil.dos2unix(initTxt).replaceAll("\\\\n", "\n"));
        writeLine(bufScript, "");
      }
    }

    // Get remote input files
    writeBlock(bufScript, "input files download", 1, "# ");
    String url;
    String guid;
    String name;
    int lfc_input_file_nr = 0;
    boolean lfcStuffWritten = false;
    for(int i=0; i<lfcInputFilesList.size(); ++i){
      url = lfcInputFilesList.get(i);
      guid = url.toString().replaceFirst(".*guid=(.+)", "$1");
      //name = url.toString().replaceFirst("^.*/([^/]+)", "$1");
      name = url.toString().replaceFirst(".*lfn=(.+)", "$1");
      if(!lfcStuffWritten && !url.equals(guid)){
        writeLine(bufScript, "echo \"uname -a\"; uname -a");
        writeLine(bufScript, "echo \"getconf LONG_BIT\"; getconf LONG_BIT");
        writeLine(bufScript, "export LCG_CATALOG_TYPE=lfc");
        if(sendJobsToData && !forceSpecificCatalog && GridPilot.VO!=null && !GridPilot.VO.trim().equals("")){
          writeLine(bufScript, "export LFC_HOST=`lcg-infosites --vo "+GridPilot.VO+" lfc`");
        }
        else{
          writeLine(bufScript, "export LFC_HOST="+lfcHost);
        }
        writeLine(bufScript, "if [ \"`uname -a | grep -r 'i[3456]86'`\" ]; " +
        		"then export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${GLITE_EXTERNAL_ROOT}/usr/lib; else " +
        		"export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${GLITE_EXTERNAL_ROOT}/usr/lib64; fi");
        writeLine(bufScript, "lcg-cp guid:"+guid+" file://`pwd`/"+MyComputingSystem.LFC_INPUT_FILE_BASE_NAME+lfc_input_file_nr);
        ++lfc_input_file_nr;
        lfcStuffWritten = true;
      }
      else if(!url.equals(name)){
        writeLine(bufScript, "lcg-cp lfn:"+name+" file://`pwd`/"+name);
      }
      else{
        logFile.addMessage("WARNING: could not parse guid or lfn from "+url);
      }
    }
    String localName;
    String remoteName;
    for(int i=0; i<remoteInputFilesList.size(); ++i){
      remoteName = (String) remoteInputFilesList.get(i);
      localName = remoteName.replaceFirst("^.*/([^/]+)", "$1");
      if(remoteName.startsWith("srm:")){
        writeLine(bufScript, "lcg-cp "+remoteName+" file://`pwd`/"+localName);
      }
      else if(remoteName.startsWith("gsiftp:")){
        writeLine(bufScript, "globus-url-copy "+remoteName+" file://`pwd`/"+localName);
      }
      else if(remoteName.startsWith("https:")){
        writeLine(bufScript, "curl -k --cert $X509_USER_PROXY --key $X509_USER_PROXY " +
              "--capath /etc/grid-security/certificates -o "+
              localName+" "+remoteName+" || curl -k -o "+localName+" "+remoteName);
      }
    }
    
    // Parameter translation
    writeBlock(bufScript, "parameter translation", 1, "# ");
    scriptLine ="PARAM";
    for(int i=0; i<formalParam.length; ++i){
      scriptLine += " "+formalParam[i];
    }
    writeBlock(bufScript, scriptLine, 1, "# ");
    String [] tmpParams = null;
    // Use actualParam.length instead of formalParam.length.
    // This way, if not all parameters are filled in the first will
    // be used and the rest just left empty.
    for(int i=0; i</*formalParam.length*/actualParam.length; ++i){
      try{
        // replace spaces with commas
        tmpParams = MyUtil.split(actualParam[i]);
        writeLine(bufScript, "p"+(i+1)+"="+MyUtil.arrayToString(tmpParams, ","));
      }
      catch(Exception ex){
          logFile.addMessage("Warning: problem with job parameter "+i);
        Debug.debug("WARNING: problem with job parameter "+i+": "+ex.getMessage(), 1);
        ex.printStackTrace();
      }
    }
    writeLine(bufScript, "");

    // core script call
    writeBlock(bufScript, "core script call", 1, "# ");
    scriptLine = shortScriptName ;
    for(int i=0; i<actualParam.length; ++i){
      scriptLine += " $p"+(i+1);
    }
    // workaround for bug in NG on Condor
    writeLine(bufScript, "if [ -e "+shortScriptName+" ]; then");
    writeLine(bufScript, "chmod +x "+shortScriptName);
    writeLine(bufScript, "./"+scriptLine);
    writeLine(bufScript, "else");
    writeLine(bufScript, scriptLine);
    writeLine(bufScript, "fi");
    writeLine(bufScript, "");
    
    // Metadata section
    /* Print the running time, size and md5sum of the output file for validation
       to pick up and write in the job DB or file catalog.
       NOTICE that we assume one output file per job. There is nothing
       in principle preventing multiple output files per job, but as it is now,
       only the first of the output files will be registered. */
    writeBlock(bufScript, "Metadata", ScriptGenerator.TYPE_SUBSECTION);
    writeLine(bufScript, "END_TIME=`date '+%s'`");
    writeLine(bufScript, "echo " +
        gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG +
        ": cpuSeconds = $(( END_TIME - START_TIME ))");
    for(int i=0; i<outputFileNames.length; ++i){
      writeLine(bufScript, "echo "+gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG+
          ": outputFileBytes = `du -b "+outputFileNames[i]+" | awk '{print $1}'`");
      writeLine(bufScript, "echo "+gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG+
          ": outputFileChecksum = md5:`md5sum "+outputFileNames[i]+" | awk '{print $1}'`");
      break;
    }      

    // upload output files
    Vector<String[]> uploadVector = new Vector<String[]>();
    // output file copy
    for(int i=0; i<outputFileNames.length; ++i){
      localName = dbPluginMgr.getJobDefOutLocalName(jobDefID, outputFileNames[i]);
      remoteName = dbPluginMgr.getJobDefOutRemoteName(jobDefID, outputFileNames[i]);
      if(remoteName.startsWith("/") || remoteName.matches("^\\w:.*")){
        // In analogy with ForkComputingSystem, this should trigger
        // copying the file to a place on the file system on the server.
        // There should not be any reason to support this
        // (if needed we could add a cp command to the job script).
        throw new IOException("ERROR: copying files locally on the worker node" +
              " is not supported. "+remoteName);
      }
      else if(remoteName.startsWith("file:")){
        // These are copied back to the client and GridPilot
        // moves them locally on the client to their final destination
        uploadVector.add(new String [] {localName, remoteName});
      }
      else if(remoteName.toLowerCase().startsWith("lfn:") || remoteName.toLowerCase().startsWith("guid:")){
        writeLine(bufScript, "lcg-cr -l "+remoteName+" file://`pwd`/"+localName+" "+remoteName);
      }
      else if(remoteName.startsWith("srm:")){
        writeLine(bufScript, "lcg-cp file://`pwd`/"+localName+" "+remoteName);
      }
      else if(remoteName.startsWith("gsiftp:")){
        writeLine(bufScript, "globus-url-copy file://`pwd`/"+localName+" "+remoteName);
      }
      else if(remoteName.startsWith("https:")){
        writeLine(bufScript, "curl -k --cert $X509_USER_PROXY --key $X509_USER_PROXY " +
              "--capath /etc/grid-security/certificates --upload-file "+
              localName+" "+remoteName);
      }
    }
    
    // job.getUploadFiles is not used
    /*String [][] uploadFiles = new String [uploadVector.size()][2];
    for(int i=0; i<uploadVector.size(); ++i){
      uploadFiles[i] = (String []) uploadVector.get(i);
    }
    job.setUploadFiles(uploadFiles);*/

    // this is for getStatus
    writeLine(bufScript, "echo job "+jobDefID+" done");

    try{
      LocalStaticShell.writeFile(exeFileName, bufScript.toString(), false);
      MyUtil.dos2unix(new File(exeFileName));
    }
    catch(FileNotFoundException fnfe){
      System.err.print(fnfe.getMessage());
      Debug.debug("Could not write file. "+fnfe.getMessage(), 1);
      return;
    }
    catch(Exception ioe){
      Debug.debug(ioe.getMessage(), 1);
      ioe.printStackTrace();
      return;
    }
  }

  private String reverseTranslateRte(String rte) {
    Debug.debug("Reverse translating using "+rteApproximationMap+"-->"+reverseRteTranslationMap, 2);
    String ret = rte;
    if(reverseRteTranslationMap.containsKey(rte)){
      ret = reverseRteTranslationMap.get(rte);
    }
    if(rteApproximationMap.containsKey(rte)){
      String realRte = rteApproximationMap.get(rte);
      if(reverseRteTranslationMap.containsKey(realRte)){
        ret = reverseRteTranslationMap.get(realRte);
      }
      else{
        ret = realRte;
      }
    }
    Debug.debug("Reverse translated "+rte+"-->"+ret, 2);
    return ret;
  }

  private String reverseApproximateRte(String rte) {
    Debug.debug("Reverse approximating using "+rteApproximationMap, 2);
    String ret = rte;
    if(rteApproximationMap.containsKey(rte)){
      ret = rteApproximationMap.get(rte);
    }
    Debug.debug("Reverse approximated "+rte+"-->"+ret, 2);
    return ret;
  }

  public void createJDL(){
  
    String jdlLine;
  
    // The executable script
    String scriptFileName = dbPluginMgr.getExecutableFile(jobDefID);
    // names starting with file: will be uploaded, others are considered to be locally available on the server
    if(scriptFileName.startsWith("file:")){
      localInputFilesList.add(MyUtil.clearTildeLocally(MyUtil.clearFile(scriptFileName)));
    }
  
    String shortExeFileName = new File(exeFileName).getName();
    String jdlExeFileName = MyUtil.clearTildeLocally(MyUtil.clearFile(exeFileName));
    Debug.debug("shortName : " + shortExeFileName, 3);
    localInputFilesList.add(jdlExeFileName);
  
    String inputFileURL = null;
  
    // create jdl file
    StringBuffer bufJdl = new StringBuffer();
    try{
      //writeLine(bufJdl, "Executable = \"/bin/bash\";");
      //writeLine(bufJdl, "Arguments = \""+shortExeFileName+"\";");
      writeLine(bufJdl, "Executable = \""+shortExeFileName+"\";");
      writeLine(bufJdl, "StdOutput = \"stdout\";");
      writeLine(bufJdl, "StdError = \"stderr\";");
      
      // Input files: scripts
      jdlLine = "InputSandbox = {";
      /*scriptFileName = MyUtil.clearTildeLocally(MyUtil.clearFile(scriptFileName));
      if(scriptFileName.startsWith("/") || scriptFileName.startsWith("\\w+:")){
        scriptFileName = "file:///"+scriptFileName;
      }
      jdlLine += "\"" + scriptFileName + "\", ";
      jdlLine += "\"file:///" + exeFileName + "\", ";*/
      // unqualified executables that are just assumed to be on the path should
      // not be listed in the input sandbox
      String exeScriptFileName = (new File(scriptFileName)).getName();
      if(!scriptFileName.equals(exeScriptFileName)){
        jdlLine += "\"" + exeScriptFileName + "\", ";
      }
      jdlLine += "\"" + (new File(exeFileName)).getName() + "\", ";
  
      // Input files.
      String[] inputFiles1 = new String [] {};
      String [] inputs = dbPluginMgr.getJobDefInputFiles(jobDefID);
      Debug.debug("input files: "+inputs.length+" "+MyUtil.arrayToString(inputs), 3);
      if(inputs!=null && inputs.length>0){
        inputFiles1 = inputs;
      }
      // Input files from executable definition
      // (typically a tarball with code)
      String[] inputFiles2 = new String [] {};
      inputs = dbPluginMgr.getExecutableInputs(
          dbPluginMgr.getJobDefExecutableID(jobDefID));
      Debug.debug("input files: "+inputs.length+" "+MyUtil.arrayToString(inputs), 3);
      if(inputs!=null && inputs.length>0){
        inputFiles2 = inputs;
      }
      String [] inputFiles = new String[inputFiles1.length+inputFiles2.length];
      System.arraycopy(inputFiles1, 0, inputFiles, 0, inputFiles1.length);
      System.arraycopy(inputFiles2, 0, inputFiles, inputFiles1.length, inputFiles2.length);
            
      for(int i=0; i<inputFiles.length; ++i){
        inputFileURL = null;
        if(MyUtil.isLocalFileName(inputFiles[i])){
          // URL is full path of input file
          inputFileURL = MyUtil.clearTildeLocally(MyUtil.clearFile(inputFiles[i]));
        }
        else{
          inputFileURL = inputFiles[i];
        }
        Debug.debug("Input file physical name: "+inputFiles[i]+"-->"+inputFileURL, 3);
       
        // Add local files to the return value.
        // Files not starting with file: are assumed to already be on the server.
        if(inputFiles[i].startsWith("file:")){
          jdlLine += "\"" + (new File(inputFileURL)).getName() + "\", ";
          localInputFilesList.add(inputFileURL);
        }
        else if(MyUtil.isLocalFileName(inputFiles[i]) && !inputFiles[i].startsWith("file:")){
          // do nothing
        }
        else if(inputFiles[i].toLowerCase().startsWith("lfc:")){
          lfcInputFilesList.add(inputFileURL);
        }
        else{
          //line += "\"" + inputFileURL + "\", ";
          remoteInputFilesList.add(inputFileURL);
        }
      }
      jdlLine += "};" ;
      jdlLine = jdlLine.replaceAll(",\\s*}", "}") ;
      writeLine(bufJdl, jdlLine);
      
      String [] remoteInputFilesArray = new String [remoteInputFilesList.size()];
      for(int i=0; i<remoteInputFilesList.size(); ++i){
        remoteInputFilesArray[i] = (String) remoteInputFilesList.get(i);
      }
      // job.getDownloadFiles is not used; we set it just for aesthetics...
      // remoteInputFilesArray will be downloaded by the job script
      job.setDownloadFiles(remoteInputFilesArray);
  
      if(!lfcInputFilesList.isEmpty()){
        if(sendJobsToData){
          jdlLine = "DataRequirements = {[\n";
          jdlLine += "InputData = {";
        }
        else{
          jdlLine = "";
        }
        String oldLfcHost = null;
        String cat = null;
        String guid;
        String name;
        for(Iterator<String> it=lfcInputFilesList.iterator(); it.hasNext();){
          cat = it.next();
          if(sendJobsToData){
            if(cat.startsWith("lfc:")){
              //name = cat.toString().replaceFirst("^.*/([^/]+)", "$1");
              name = cat.toString().replaceFirst(".*lfn=(.+)", "$1");
              guid = cat.toString().replaceFirst(".*guid=(.+)", "$1");
              lfcHost = cat.replaceFirst("lfc:/*([^:^/]+)[:/].*", "$1");
              if(lfcHost.equals(cat)){
                logFile.addMessage("WARNING: could not parse LFC host from "+cat);
                lfcHost = null;
              }
              if(!guid.equals(cat)){
                jdlLine += "\"guid:"+guid+"\", ";
              }
              else if(!name.equals(cat)){
                jdlLine += "\"lfn:"+name+"\", ";
              }
              else{
                logFile.addMessage("WARNING: could not parse lfn or guid from "+cat);
              }
              if(oldLfcHost!=null && !lfcHost.equals(oldLfcHost)){
                throw new IOException("ERROR: cannot use more than one catalog per job. "+
                    lfcHost+"!="+oldLfcHost);
              }
              oldLfcHost = lfcHost;
            }
            else{
              logFile.addMessage("WARNING: LFC file list contains non-lfc URL: "+cat);
              jdlLine += "\""+cat+"\", ";
            }
          }
        }
        // sendJobsToData implies that we'll download files with lcg-cp guid:... instead of lcg-cp srm://...
        if(sendJobsToData){
          jdlLine += "};";
          jdlLine = jdlLine.replaceFirst(", }","}") ;
          writeLine(bufJdl, jdlLine);
          // Not necessary, in fact not wanted - it'll likely cause jobs not to be picked up.
          if(lfcHost!=null && forceSpecificCatalog){
            writeLine(bufJdl, "DataCatalog = \"http://"+lfcHost+":8085\";");
          }
          writeLine(bufJdl, "DataCatalogType = \"DLI\";");
          jdlLine = "]};";
        }
        writeLine(bufJdl, jdlLine);
      }
      //jdlLine = "DataAccessProtocol = {\"https\", \"http\", \"srm\", \"gridftp\", \"file\"};";
      //jdlLine = "DataAccessProtocol = {\"rfio\", \"gsidcap\", \"gsiftp\", \"https\", \"http\"};";
      jdlLine = "DataAccessProtocol = {\"rfio\", \"gsidcap\", \"gsiftp\"};";
      writeLine(bufJdl, jdlLine);
            
      // Various options
      //writeLine(bufJdl, "DataAccessProtocol =  {\"rfio\", \"gsiftp\", \"gsidcap\"};");
      Vector<String> reqVec = new Vector<String>();
      // Run only on specific clusters
      if(clusters!=null && clusters.length>0){
        String clustersStr = "";
        for(int i=0; i<clusters.length; ++i){
          if(excludedClusters!=null && MyUtil.arrayContains(excludedClusters, clusters[i])){
            continue;
          }
          if(i>0){
            clustersStr += " || ";
          }
          //clustersStr += "other.GlueCEUniqueID == \""+clusters[i]+"\"";
          clustersStr += "REgExp(\""+clusters[i]+"\", other.GlueCEUniqueID)";
        }
        reqVec.add("("+clustersStr+")");
      }
      if(excludedClusters!=null && excludedClusters.length>0 && (clusters==null || clusters.length==0)){
        String clustersStr = "";
        for(int i=0; i<clusters.length; ++i){
          if(i>0){
            clustersStr += " && ";
          }
          //clustersStr += "other.GlueCEUniqueID != \""+clusters[i]+"\"";
          clustersStr += "!REgExp(\""+clusters[i]+"\", other.GlueCEUniqueID)";
        }
        reqVec.add("("+clustersStr+")");
      }
      if(cpuTime!=null&&!cpuTime.equals("")){
        reqVec.add("(other.GlueCEPolicyMaxCPUTime >= "+cpuTime+")");
      }
      if(memory!=null&&!memory.equals("")){
        reqVec.add("(other.other.GlueHostMainMemoryRAMSize >= "+memory+")");
      }
      reqVec.add("(other.GlueCEStateStatus == \"Production\")");
      writeLine(bufJdl, "rank = -other.GlueCEStateEstimatedResponseTime;");
      writeLine(bufJdl, "DefaultRank = -other.GlueCEStateEstimatedResponseTime;");
      writeLine(bufJdl, "SignificantAttributes = {\"Requirements\", \"Rank\", \"FuzzyRank\"};");
      //if(GridPilot.vo!=null && !GridPilot.vo.equals("")){
      //  writeLine(bufJdl, "VirtualOrganisation = \"" + GridPilot.vo + "\";");
      //}
      writeLine(bufJdl, "JobType  = \"normal\";");
      writeLine(bufJdl, "Type  = \"job\";");
      writeLine(bufJdl, "RetryCount  = 0;");
      writeLine(bufJdl, "ShallowRetryCount = " + reRun + ";");
      
      // Runtime environments
      String rteReq;
      if(uses!=null && uses.length>0 && uses[0]!=null){
        for(int i=0; i<uses.length; ++i){
          // At least for now, we only have Linux resources on EGEE
          if(uses[i].equals(GLiteComputingSystem.OS)){
            continue;
          }
          rteReq = "Member(\""+MyUtil.dos2unix(reverseTranslateRte(uses[i]))+"\", " +
             "other.GlueHostApplicationSoftwareRunTimeEnvironment)";
          reqVec.add(rteReq);
        }
      }
      
      writeLine(bufJdl, "Requirements = "+
          MyUtil.arrayToString(reqVec.toArray(new String[reqVec.size()]), " && ")+";");    
      // Output files
      jdlLine = "OutputSandbox = {\"stdout\", \"stderr\"";
      // upload output files
      String localName;
      String remoteName;
      // output file copy
      for(int i=0; i<outputFileNames.length; ++i){
        localName = dbPluginMgr.getJobDefOutLocalName(jobDefID, outputFileNames[i]);
        remoteName = dbPluginMgr.getJobDefOutRemoteName(jobDefID, outputFileNames[i]);
        if(remoteName.startsWith("file:")){
          // These are copied back to the client and GridPilot
          // moves them locally on the client to their final destination
          jdlLine += ", \""+localName+"\"";
        }
      }

      jdlLine += "};";
      writeLine(bufJdl, jdlLine);
  
      try{
        LocalStaticShell.writeFile(jdlFileName, bufJdl.toString(), false);
        MyUtil.dos2unix(new File(jdlFileName));
      }
      catch(FileNotFoundException fnfe){
        System.err.print(fnfe.getMessage());
        Debug.debug("Could not write file. "+fnfe.getMessage(), 1);
        return;
      }
      catch(Exception ioe){
        Debug.debug(ioe.getMessage(), 1);
        ioe.printStackTrace();
        return;
      }
    }
    catch(IOException ioe){
      logFile.addMessage("ERROR generating job scripts", ioe);
      return;
    }
  }
}