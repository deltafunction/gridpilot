package gridpilot;

/**
 * Debuging class : print debug information with the calling class name. <p>
 *
 *
 */

public class Debug{

  public static String toTrace = "all";

  private static Class [] excludeClasses = {/*DBPluginMgr.class*/};
//  static int debugLevel = 3; // 3 : only temporary debugs, 1 : all debugs, with fuctionNames
  private static boolean withClassName = true;
  private static boolean withPackage = false;
  private static boolean withMethodName = true;
  private static boolean withLineNumber = true;

  public synchronized static void debug(String msg, int level){
     //level : 1 : function name, 2 : 3: temporal debugs
    if(isExclude())
       return;

    if(level >= GridPilot.getClassMgr().getDebugLevel()){
      String className = "";
      Throwable t = new Throwable();
      if(withClassName)
        className += t.getStackTrace()[1].getClassName() ;
      if(!withPackage)
        className = className.substring(className.lastIndexOf(".")+1);
      if(withMethodName)
        className += "." + t.getStackTrace()[1].getMethodName();
      if(withLineNumber)
        className += ":" + t.getStackTrace()[1].getLineNumber();

      System.out.println(className +  " : " + msg);
    }
  }

  private static boolean isExclude(){
    String className = new Throwable().getStackTrace()[2].getClassName();
    for(int i=0; i<excludeClasses.length ; ++i)
      if(className.equals(excludeClasses[i].getName()))
        return true;


    return false;
  }

}
