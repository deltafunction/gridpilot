package gridpilot;

import gridpilot.Database.DBResult;
import gridpilot.Database.DBRecord;
import gridpilot.Database.JobTrans;

class MyThread extends Thread{

  public int getIntRes(){throw new UnsupportedOperationException("getIntRes not implemented !");}
  public boolean getBooleanRes(){throw new UnsupportedOperationException("getBooleanRes not implemented !"); }
  public String getStringRes(){throw new UnsupportedOperationException("getStringRes not implemented !");}
  public String[] getString2Res(){throw new UnsupportedOperationException("getString2Res not implemented !");}
  public DBRecord getDBRes(){throw new UnsupportedOperationException("getDBRes not implemented !");}
  public DBResult getDB2Res(){throw new UnsupportedOperationException("getDB2Res not implemented !");}
  public JobTrans[] getJTRes(){throw new UnsupportedOperationException("getJTRes not implemented !");}
  public boolean getBoolRes(){throw new UnsupportedOperationException("getBoolRes not implemented !");}
  
}