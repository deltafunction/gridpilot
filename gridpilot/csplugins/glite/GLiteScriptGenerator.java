package gridpilot.csplugins.glite;

import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.JobInfo;
import gridpilot.LocalStaticShellMgr;
import gridpilot.ScriptGenerator;
import gridpilot.Util;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class GLiteScriptGenerator extends ScriptGenerator {

  String cpuTime = null;
  String reRun = null;
  List localInputFilesList = null;
  List remoteInputFilesList = null;
  List lfcInputFilesList = null;
  String vo = null;
  String replicaCatalog = null;

  public GLiteScriptGenerator(String _csName) {
    super(_csName);
    csName = _csName;
    cpuTime = configFile.getValue(csName, "CPU time");
    reRun = configFile.getValue(csName, "Max rerun");
    vo = configFile.getValue(csName, "Virtual organization");
    replicaCatalog = configFile.getValue(csName, "ReplicaCatalog");
    localInputFilesList = new Vector();
    remoteInputFilesList = new Vector();
    lfcInputFilesList = new Vector();

  }

  public List createJDL(JobInfo job, String exeFileName, String jdlFileName){

    String jdlLine;
    String jobDefID = job.getJobDefId();
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] formalParam = dbPluginMgr.getTransformationArguments(jobDefID);
    String [] actualParam = dbPluginMgr.getJobDefTransPars(jobDefID);

    // The transformation script
    String scriptFileName = dbPluginMgr.getTransformationScript(jobDefID);
    String shortScriptName = scriptFileName;
    int lastSlash = shortScriptName.replaceAll("\\\\", "/").lastIndexOf("/");
    if(lastSlash>-1){
      shortScriptName = shortScriptName.substring(lastSlash + 1);
    }
    // names starting with file: will be uploaded, names starting with
    // / or c:\ are considered to be locally available on the server
    if(scriptFileName.startsWith("file:")){
      localInputFilesList.add(Util.clearTildeLocally(Util.clearFile(scriptFileName)));
    }

    String shortExeFileName = new File(exeFileName).getName();
    String jdlExeFileName = Util.clearTildeLocally(Util.clearFile(exeFileName));
    Debug.debug("shortName : " + shortExeFileName, 3);
    localInputFilesList.add(jdlExeFileName);

    String inputFileURL = null;

    // create jdl file
    StringBuffer bufJdl = new StringBuffer();
    try{
      writeLine(bufJdl, "Executable = \"/usr/bin/sh\";");
      writeLine(bufJdl, "Arguments = \""+scriptFileName+"\";");
      writeLine(bufJdl, "StdOutput = \"stdout\";");
      writeLine(bufJdl, "StdError = \"stderr\";");
      
      // Input files: scripts
      jdlLine = "InputSandbox = {";
      if(scriptFileName.startsWith("file:")){
        jdlLine += "\"" + Util.clearTildeLocally(Util.clearFile(scriptFileName)) + "\", ";
      }
      jdlLine += "\"" + exeFileName + "\", ";

      // Input files.
      String[] inputFiles1 = new String [] {};
      String [] inputs = dbPluginMgr.getJobDefInputFiles(jobDefID);
      Debug.debug("input files: "+inputs.length+" "+Util.arrayToString(inputs), 3);
      if(inputs!=null && inputs.length>0){
        inputFiles1 = inputs;
      }
      // Input files from transformation definition
      // (typically a tarball with code)
      String[] inputFiles2 = new String [] {};
      inputs = dbPluginMgr.getTransformationInputs(
          dbPluginMgr.getJobDefTransformationID(jobDefID));
      Debug.debug("input files: "+inputs.length+" "+Util.arrayToString(inputs), 3);
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
          inputFileURL = Util.clearTildeLocally(Util.clearFile(inputFiles[i]));
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
      jdlLine += "\"};" ;
      jdlLine = jdlLine.replaceFirst(", }","  }") ;
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
      writeLine(bufJdl, "DataAccessProtocol =  {\"rfio\", \"gsiftp\", \"gsidcap\"};");
      writeLine(bufJdl, "Requirements = other.GlueCEPolicyMaxCPUTime > "+cpuTime+";");
      writeLine(bufJdl, "VirtualOrganisation = \"" + vo + "\";");
      writeLine(bufJdl, "RetryCount  = \"0\";");
      writeLine(bufJdl, "ShallowRetryCount = \"" + reRun + "\";");
      
      // Runtime environments
      String [] uses = dbPluginMgr.getRuntimeEnvironments(jobDefID);
      if(uses!=null && uses.length>0 && uses[0]!=null){
        for(int i=0; i<uses.length; ++i){
          // At least for now, we only have Linux resources on LCG
          if(uses[i].equals("Linux")){
            continue;
          }
          writeLine(bufJdl,"Requirements = Member(\""+Util.dos2unix(uses[i])+"\", " +
          "other.GlueHostApplicationSoftwareRunTimeEnvironment);");
        }
      }

      // create job script

      String scriptLine = "";
      // Header
      StringBuffer bufScript = new StringBuffer();
      writeLine(bufScript, "#!" + configFile.getValue(csName, "Shell"));
      writeLine(bufScript,"# Script generated by GridPilot/GLiteScriptGenerator");
      writeBloc(bufScript, csName + " wrapper script", 0, "# ");

      // Runtime environment dependencies. Text from runtimeEnvironment.initText
      writeBloc(bufScript, "runtime environment dependencies", 1, "# ");
      for(int i=0; i<uses.length; ++i){
        writeBloc(bufScript, "use "+ uses[i], 2, "# ");
        String initTxt = dbPluginMgr.getRuntimeInitText(uses[i], csName).toString();
        writeLine(bufScript, Util.dos2unix(initTxt));
        writeLine(bufScript, "");
      }

      // Get remote input files
      writeBloc(bufScript, "input files download", 1, "# ");
      for(int i=0; i<lfcInputFilesList.size(); ++i){
        writeLine(bufScript, "lcg-cp "+lfcInputFilesList.get(i)+
            "file://`pwd`/"+
            (lfcInputFilesList.get(i).toString().replaceFirst("^.*/([^/]+)", "$1")));
      }
      for(int i=0; i<remoteInputFilesList.size(); ++i){
        writeLine(bufScript, "lcg-cp "+remoteInputFilesList.get(i)+
            "file://`pwd`/"+
            (remoteInputFilesList.get(i).toString().replaceFirst("^.*/([^/]+)", "$1")));
      }
      
      // Parameter translation
      writeBloc(bufScript, "parameter translation", 1, "# ");
      scriptLine ="PARAM";
      for(int i=0; i<formalParam.length; ++i){
        scriptLine += " "+formalParam[i];
      }
      writeBloc(bufScript, scriptLine, 1, "# ");
      String [] tmpParams = null;
      // Use actualParam.length instead of formalParam.length.
      // This way, if not all parameters are filled in the first will
      // be used and the rest just left empty.
      for(int i=0; i</*formalParam.length*/actualParam.length; ++i){
        try{
          // replace spaces with commas
          tmpParams = Util.split(actualParam[i]);
          writeLine(bufScript, "p"+(i+1)+"="+Util.arrayToString(tmpParams, ","));
        }
        catch(Exception ex){
            logFile.addMessage("Warning: problem with job parameter "+i);
          Debug.debug("WARNING: problem with job parameter "+i+": "+ex.getMessage(), 1);
          ex.printStackTrace();
        }
      }
      writeLine(bufScript, "");

      // core script call
      writeBloc(bufScript, "core script call", 1, "# ");
      
      // workaround for bug in NG on Condor
      writeLine(bufScript, "chmod +x "+shortScriptName);
      scriptLine = "./"+shortScriptName ;
      for(int i=0; i<formalParam.length; ++i)
        scriptLine += " $p"+(i+1);
      writeLine(bufScript, scriptLine);
      writeLine(bufScript, "");
      
      // Output files
      jdlLine = "OutputSandbox = {\"stdout\", \"stderr\"";
      // upload output files
      String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getJobDefId());
      String localName;
      String remoteName;
      Vector uploadVector = new Vector();
      // output file copy
      for(int i=0; i<outputFileNames.length; ++i){
        localName = dbPluginMgr.getJobDefOutLocalName(job.getJobDefId(), outputFileNames[i]);
        remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(), outputFileNames[i]);
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
          jdlLine += ", \""+localName+"\"";
          uploadVector.add(new String [] {localName, remoteName});
        }
        else if(remoteName.toLowerCase().startsWith("lfn:")){
          writeLine(bufScript, "lcg-cr -l "+remoteName+" file://`pwd`/"+localName+" "+remoteName);
        }
        else if(remoteName.startsWith("srm:") || remoteName.startsWith("gsiftp:")){
          writeLine(bufScript, "lcg-cp file://`pwd`/"+localName+" "+remoteName);
        }
        Debug.debug("remote name: "+remoteName,3);
        scriptLine += "(\""+localName+"\" \""+remoteName+"\")";
      }
      
      // job.getUploadFiles is not used; we set it just for aesthetics...
      String [][] uploadFiles = new String [uploadVector.size()][2];
      for(int i=0; i<uploadVector.size(); ++i){
        uploadFiles[i] = (String []) uploadVector.get(i);
      }
      job.setUploadFiles(uploadFiles);

      jdlLine += "};";
      writeLine(bufScript, scriptLine);
      writeLine(bufJdl, jdlLine);

      try{
        LocalStaticShellMgr.writeFile(exeFileName,
            bufScript.toString(), false);
        Util.dos2unix(new File(exeFileName));
        LocalStaticShellMgr.writeFile(jdlFileName,
            bufJdl.toString(), false);
        Util.dos2unix(new File(exeFileName));
      }
      catch(FileNotFoundException fnfe){
        System.err.print(fnfe.getMessage());
        Debug.debug("Could not write file. "+fnfe.getMessage(), 1);
        return null;
      }
      catch(Exception ioe){
        Debug.debug(ioe.getMessage(), 1);
        ioe.printStackTrace();
        return null;
      }
      // These files will be uploaded to the sandbox.
      return localInputFilesList;
    }
    catch(IOException ioe){
      logFile.addMessage("ERROR generating job scripts", ioe);
      return null;
    }
  }
}