package gridpilot.csplugins.glite;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.jobrun.ScriptGenerator;
import gridpilot.DBPluginMgr;
import gridpilot.GridPilot;
import gridpilot.MyJobInfo;
import gridpilot.MyUtil;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class GLiteScriptGenerator extends ScriptGenerator {

  String cpuTime = null;
  String memory = null;
  String reRun = null;
  List remoteInputFilesList = null;
  List lfcInputFilesList = null;
  String replicaCatalog = null;
  String [] uses = null;
  DBPluginMgr dbPluginMgr = null;
  MyJobInfo job = null;
  String exeFileName = null;
  String jdlFileName = null;
  String [] outputFileNames = null;
  String shortScriptName = null;
  String jobDefID = null;
  ConfigFile configFile;
  String csName;

  // These files will be uploaded to the sandbox.
  protected List localInputFilesList = null;

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
    replicaCatalog = configFile.getValue(csName, "ReplicaCatalog");
    localInputFilesList = new Vector();
    remoteInputFilesList = new Vector();
    lfcInputFilesList = new Vector();
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    jobDefID = job.getIdentifier();
    uses = dbPluginMgr.getRuntimeEnvironments(jobDefID);
    outputFileNames = dbPluginMgr.getOutputFiles(jobDefID);
    shortScriptName = dbPluginMgr.getTransformationExeFile(jobDefID);
    int lastSlash = shortScriptName.replaceAll("\\\\", "/").lastIndexOf("/");
    if(lastSlash>-1){
      shortScriptName = shortScriptName.substring(lastSlash + 1);
    }
  }

  public void createScript(){

    String [] formalParam = dbPluginMgr.getTransformationArguments(jobDefID);
    String [] actualParam = dbPluginMgr.getJobDefTransPars(jobDefID);

    try{
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
        String initTxt = dbPluginMgr.getRuntimeInitText(uses[i], csName).toString();
        writeLine(bufScript, MyUtil.dos2unix(initTxt));
        writeLine(bufScript, "");
      }

      // Get remote input files
      writeBlock(bufScript, "input files download", 1, "# ");
      for(int i=0; i<lfcInputFilesList.size(); ++i){
        writeLine(bufScript, "lcg-cp "+lfcInputFilesList.get(i)+
            "file://`pwd`/"+
            (lfcInputFilesList.get(i).toString().replaceFirst("^.*/([^/]+)", "$1")));
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
                localName+" "+remoteName);
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
      
      // workaround for bug in NG on Condor
      writeLine(bufScript, "chmod +x "+shortScriptName);
      scriptLine = "./"+shortScriptName ;
      for(int i=0; i<formalParam.length; ++i)
        scriptLine += " $p"+(i+1);
      writeLine(bufScript, scriptLine);
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
      Vector uploadVector = new Vector();
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
        else if(remoteName.toLowerCase().startsWith("lfn:")){
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
    catch(IOException ioe){
      logFile.addMessage("ERROR generating job scripts", ioe);
      return;
    }
  }

  public void createJDL(){
  
    String jdlLine;
  
    // The transformation script
    String scriptFileName = dbPluginMgr.getTransformationExeFile(jobDefID);
    // names starting with file: will be uploaded, names starting with
    // / or c:\ are considered to be locally available on the server
    localInputFilesList.add(MyUtil.clearTildeLocally(MyUtil.clearFile(scriptFileName)));
  
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
      jdlLine += "\"" + MyUtil.clearTildeLocally(MyUtil.clearFile(scriptFileName)) + "\", ";
      jdlLine += "\"" + exeFileName + "\", ";
  
      // Input files.
      String[] inputFiles1 = new String [] {};
      String [] inputs = dbPluginMgr.getJobDefInputFiles(jobDefID);
      Debug.debug("input files: "+inputs.length+" "+MyUtil.arrayToString(inputs), 3);
      if(inputs!=null && inputs.length>0){
        inputFiles1 = inputs;
      }
      // Input files from transformation definition
      // (typically a tarball with code)
      String[] inputFiles2 = new String [] {};
      inputs = dbPluginMgr.getTransformationInputs(
          dbPluginMgr.getJobDefTransformationID(jobDefID));
      Debug.debug("input files: "+inputs.length+" "+MyUtil.arrayToString(inputs), 3);
      if(inputs!=null && inputs.length>0){
        inputFiles2 = inputs;
      }
      String [] inputFiles = new String[inputFiles1.length+inputFiles2.length];
      System.arraycopy(inputFiles1, 0, inputFiles, 0, inputFiles1.length);
      System.arraycopy(inputFiles2, 0, inputFiles, inputFiles1.length, inputFiles2.length);
            
      for(int i=0; i<inputFiles.length; ++i){
        inputFileURL = null;
        if(inputFiles[i].startsWith("http://") ||
            inputFiles[i].startsWith("https://") ||
            inputFiles[i].startsWith("gsiftp://") ||
            inputFiles[i].startsWith("ftp://")){
          inputFileURL = inputFiles[i];
        }
        else{
          // URL is full path of input file
          inputFileURL = MyUtil.clearTildeLocally(MyUtil.clearFile(inputFiles[i]));
        }
        Debug.debug("Input file physical name: "+inputFileURL, 3);
       
        // Add local files to the return value.
        // Files starting with / are assumed to already be on the server.
        if(inputFiles[i].startsWith("file:")){
          jdlLine += "\"" + inputFileURL + "\", ";
          localInputFilesList.add(inputFileURL);
        }
        else if(inputFiles[i].startsWith("/")){
          // do nothing
        }
        else if(inputFiles[i].toLowerCase().startsWith("lfn:")){
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
        jdlLine = "InputData = {";
        String lfcHost = null;
        String oldLfcHost = null;
        String cat = null;
        for(Iterator it=lfcInputFilesList.iterator(); it.hasNext();){
          cat = (String) it.next();
          if(cat.startsWith("lfc:")){
            lfcHost = cat.replaceFirst("lfc:/*(.*)[:/]", "$1");
            jdlLine += "\"lfn:"+cat+"\", ";
            if(oldLfcHost!=null && !lfcHost.equals(oldLfcHost)){
              throw new IOException("ERROR: cannot use more than one catalog per job. "+
                  lfcHost+"!="+oldLfcHost);
            }
            oldLfcHost = lfcHost;
          }
        }
        jdlLine += "};";
        jdlLine = jdlLine.replaceFirst(", }","  }") ;
        writeLine(bufJdl, jdlLine);
        if(lfcHost!=null){
          writeLine(bufJdl, "DataCatalog = \""+lfcHost+"\";");
        }
      }
      
      // Various options
      //writeLine(bufJdl, "DataAccessProtocol =  {\"rfio\", \"gsiftp\", \"gsidcap\"};");
      writeLine(bufJdl, "Requirements =" +
          (cpuTime==null||cpuTime.equals("")?"":"(other.GlueCEPolicyMaxCPUTime >= "+cpuTime+") ") +
          (cpuTime!=null&&!cpuTime.equals("")?" && ":"")+
          (memory==null||memory.equals("")?"":"(other.other.GlueHostMainMemoryRAMSize >= "+memory+") ") +
          ((cpuTime==null||cpuTime.equals(""))&&(memory!=null&&!memory.equals(""))?" && ":"")+
          "(other.GlueCEStateStatus == \"Production\")" +
          ";");
      writeLine(bufJdl, "rank = -other.GlueCEStateEstimatedResponseTime;");
      writeLine(bufJdl, "DefaultRank = -other.GlueCEStateEstimatedResponseTime;");
      writeLine(bufJdl, "SignificantAttributes = { \"Requirements\",\"Rank\",\"FuzzyRank\" };");
      //if(GridPilot.vo!=null && !GridPilot.vo.equals("")){
      //  writeLine(bufJdl, "VirtualOrganisation = \"" + GridPilot.vo + "\";");
      //}
      writeLine(bufJdl, "JobType  = \"normal\";");
      writeLine(bufJdl, "Type  = \"job\";");
      writeLine(bufJdl, "RetryCount  = 3;");
      writeLine(bufJdl, "ShallowRetryCount = " + reRun + ";");
      
      // Runtime environments
      if(uses!=null && uses.length>0 && uses[0]!=null){
        for(int i=0; i<uses.length; ++i){
          // At least for now, we only have Linux resources on EGEE
          if(uses[i].equals(GLiteComputingSystem.OS)){
            continue;
          }
          writeLine(bufJdl,"Requirements = Member(\""+MyUtil.dos2unix(uses[i])+"\", " +
          "other.GlueHostApplicationSoftwareRunTimeEnvironment);");
        }
      }
    
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