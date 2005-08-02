package gridpilot;

import java.io.*;


/**
 * Abstract class, has to be extended for each computing system. <p>
 *
 * Provides 4 (defined) protected methods : <ul>
 * <li> protected String getFullPath(String file) : <br>
 *    converts a file name into a full path name
 * <li> protected void writeLine(RandomAccessFile out, String s) : <br>
 *    writes a line in file out
 * <li> protected void writeBloc(RandomAccessFile out, String [] s, int type) : <br>
 *    writes a comment "bloc" containing multi lines s. type is in {0,1,2}, from the biggest
 *    bloc to the smallest
 * <li> protected void writeBloc(RandomAccessFile out, String s, int type) : <br>
 *    idem, but for a simple line
 *
 * <p><a href="ScriptGenerator.java.html">see sources</a>
 */



abstract public class ScriptGenerator{
  protected ConfigFile configFile;
  protected String csName;
  protected LogFile logFile;


  /**
   * Constructors
   */

  public ScriptGenerator(String _csName) {
    csName = _csName;
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();

  }

  /**
   * protected methods
   */

  // TODO: These two methods are duplicated in JobControl.java. FIX!
  
  private String getLine(String s){return s + "\n";}

  private String getBloc(String []s, int type){
    String bloc=null;
    switch(type){
      case 0:
        bloc = "###########################################################################\n";
        for(int i = 0; i<s.length ; ++i)
          bloc += "#          " + s[i]+"\n";
        bloc +="###########################################################################\n";
        break;

      case 1:
        bloc = "#--------------------------------------------------------------------------\n";
        for(int i = 0; i<s.length ; ++i)
          bloc += "#          " + s[i]+"\n";
        bloc +="#--------------------------------------------------------------------------\n";
        break;

      case 2:
        bloc = "#-------------------\n";
        for(int i = 0; i<s.length ; ++i)
          bloc += "#      " + s[i]+"\n";
        bloc +="#-------------------\n";
        break;
    }
    return bloc;
  }

  protected void writeLine(RandomAccessFile out, String s) throws IOException{
    out.writeBytes(getLine(s));
  }

  protected void writeBloc(RandomAccessFile out, String [] s, int type) throws IOException{
    writeLine(out, getBloc(s, type));
  }

  protected void writeBloc(RandomAccessFile out, String s, int type)throws IOException{
    String [] s2 = {s};
    writeBloc(out, s2, type);
  }

  protected void writeLine(StringBuffer buf, String s){
    buf.append(getLine(s));
  }

  protected void writeBloc(StringBuffer buf, String [] s, int type){
    buf.append(getBloc(s,type));
  }

  protected void writeBloc(StringBuffer buf, String s, int type){
    writeBloc(buf, new String[]{s}, type);
  }
}