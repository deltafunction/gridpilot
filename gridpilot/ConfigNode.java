package gridpilot;

import java.util.Iterator;
import java.util.Vector;

public class ConfigNode{
  
  private Vector configNodes;
  private String name;
  private String value;
  private String description;
  private String section;

  public ConfigNode(String _name){
    name = _name;
    configNodes = new Vector();
  }
  
  public void setDescription(String _description){
    description = _description;
  }
  
  public void setValue(String _value){
    value = _value;
  }
  
  public void setSection(String _section){
    section = _section;
  }
  public void addNode(ConfigNode configNode){
    if(configNode==null){
      return;
    }
    configNodes.add(configNode);
  }

  public String getValue(){
    return value;
  }

  public String getSection(){
    return section;
  }

  public Vector getConfigNodes(){
    return configNodes;
  }
  
  public ConfigNode getConfigNode(String name){
    for(Iterator it=configNodes.iterator(); it.hasNext();){
      ConfigNode node = (ConfigNode) it.next();
      if(node.getName().equalsIgnoreCase(name)){
        return node;
      }
    }
    return null;
  }

  public String getName(){
    return name;
  }

  public String getDescription(){
    return description;
  }
  
  public String toString(){
    return name;
  }
  
  public void printAll(int level){
    ConfigNode node = null;
    for(Iterator it=configNodes.iterator(); it.hasNext();){
      node = (ConfigNode) it.next();
      String levStr = "";
      for(int i=0;i<level; ++i){
        levStr += "  ";
      }
      if(node.getConfigNodes()==null || node.getConfigNodes().size()==0){
        System.out.println(levStr+node.getName());
      }
      else{
        System.out.println(levStr+"***"+node.getName()+"***");
        node.printAll(level + 1);
      }
    }
  }

}
