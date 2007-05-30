package gridpilot.wizards.beginning;

import gridpilot.ConfirmBox;

import javax.swing.JOptionPane;

public class BeginningWizard{

  public BeginningWizard(boolean firstRun){
    
    if(!welcome(firstRun)){
      return;
    }
    
    if(!checkDirs(firstRun)){
      return;
    }
    
    
    // Ask about certificate location. If not there, warn.

  }
  
  private boolean welcome(boolean firstRun){
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "\nWelcome!\n\n" +
            (firstRun?"This appears to be the first time you run GridPilot.\n":"") +
            "On the next windows you will be guided through some simple steps to setup GridPilot.\n" +
            "You will be asked to confirm the creation of a configuration file, etc.\n\n" +
            "Click \"Continue\" to move on or \"Cancel\" to exit this wizard.\n\n" +
            "Notice that you can always run this wizard again by choosing it from " +
            "the \"Help\" menu.";
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Starting with GridPilot",
          confirmString,
       new Object[] {"Continue",  "Cancel"});
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    if(choice!=0){
      return false;
    }
    return true;
  }
  
  private boolean endGreeting(boolean firstRun){
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "Please notice that only the most basic parameters,\n" +
            "necessary to get you up and running have been set.\n" +
            "You can modify these and set many others in \"Edit\" -> \"Preferences\"." +
            (firstRun?"\n\nThanks for using GridPilot and have fun!":"");
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Starting with GridPilot",
          confirmString,
       new Object[] {"Continue",  "Cancel"});
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    if(choice!=0){
      return false;
    }
    return true;
  }
  
  private boolean checkDirs(boolean firstRun){
    // If .gridpilot is there, get the various directory names, otherwise, just
    // check if ~/GridPilot is there
    if(firstRun){
    }
    else{
      // [GridPilot] pull cache directory = ~/GridPilot/files/cache
      // [My_DB_local] database = hsql://localhost/~/GridPilot/My_DB
      // [Fork] working directory = ~/GridPilot/jobs
      // [Fork] runtime directory = ~/GridPilot/runtimeEnvironments
      // [Fork] transformation directory = ~/GridPilot/transformations
    }
    return false;
  }
  
  
}
