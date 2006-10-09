package gridpilot.dbplugins.atlas;

import java.util.Vector;

public class DQ2Locations{
  
  private String datasetName = null;
  private Vector incomplete = null;
  private Vector complete = null;
  
  public DQ2Locations(String _datasetName){
    datasetName = _datasetName;
    incomplete = new Vector();
    complete = new Vector();
  }
  
  public DQ2Locations(String _datasetName, String [] incompleteArr,
      String [] completeArr){
    datasetName = _datasetName;
    incomplete = new Vector();
    complete = new Vector();
    for(int i=0; i<incompleteArr.length; ++i){
      addIncomplete(incompleteArr[i]);
    }
    for(int i=0; i<completeArr.length; ++i){
      addComplete(completeArr[i]);
    }
  }
  
  public String getdatasetName(){
    return datasetName;
  }
  
  public String [] getIncomplete(){
    String [] tmpArr = new String[incomplete.size()];
    for(int i=0; i<tmpArr.length; ++i){
      tmpArr[i] = (String) incomplete.get(i);
    }
    return tmpArr;
  }
  
  public String [] getComplete(){
    String [] tmpArr = new String[complete.size()];
    for(int i=0; i<tmpArr.length; ++i){
      tmpArr[i] = (String) complete.get(i);
    }
    return tmpArr;
  }
  
  public void addIncomplete(String el){
    incomplete.add(el);
  }
  
  public void addComplete(String el){
    complete.add(el);
  }
}
