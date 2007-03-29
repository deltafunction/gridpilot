package gridpilot;

import java.util.Vector;

public class ConfigNode{
  
  private Vector configNodes;
  private String name;
  private String value;
  private String description;

  public ConfigNode(String _name, String _value){
    name = _name;
    value = _value;
  }
  
  public void setDescription(String _description){
    description = _description;
  }
  
  public void addNode(ConfigNode configNode){
    configNodes.add(configNode);
  }

  public String getValue(){
    return value;
  }

  public ConfigNode [] getConfigNodes(){
    ConfigNode [] nodeArray = new ConfigNode[configNodes.size()];
    for(int i=0; i<nodeArray.length; ++i){
      nodeArray[i] = (ConfigNode) configNodes.get(i);
    }
    return nodeArray;
  }

  public String getName(){
    return name;
  }

  public String getDescription(){
    return description;
  }

}
