package gridpilot.wizards.beginning;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import gridpilot.ConfigFile;
import gridpilot.ConfirmBox;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.LocalStaticShellMgr;
import gridpilot.Util;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

public class BeginningWizard{

  private ImageIcon icon = null;
  private static int TEXTFIELDWIDTH = 32;

  public BeginningWizard(boolean firstRun){
    
    URL imgURL = null;
    
    try{
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "aviateur.png");
      icon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.resourcesPath + "aviateur.png", 3);
      icon = null;
    }
    
    try{
      if(!welcome(firstRun)){
        return;
      }
      
      if(!checkDirs(firstRun)){
        return;
      }
      
      // TODO: ask about certificate location. If not there, warn.
      
      // TODO: set grid homedir
      
      // TODO: Set grid file catalog - fallback to homegrid.dyndns.org
      
      // TODO: Set grid job DB - fallback to homegrid.dyndns.org
      
      // TODO: set ATLAS home site - present list - ask for mysql alias
      
      // TODO: set pull and GPSS DB - present list, fallback to homegrid.dyndns.org
      
      // TODO: ask on which grids the user is allowed
      
      // TODO: ask if an ssh pool should be configured
      
      // TODO: give report on which directories were created, etc.
      
      endGreeting(firstRun);

    }
    catch(Exception e){
      e.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage("ERROR: could not run setup wizard", e);
      try{
        showError(e.getMessage());
      }
      catch(Exception e1){
        e1.printStackTrace();
      }
    }
    
  }
  
  private boolean welcome(boolean firstRun) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "Welcome!\n\n" +
            (firstRun?"This appears to be the first time you run GridPilot.\n":"") +
            "On the next windows you will be guided through some simple steps to setup GridPilot.\n" +
            "You will be asked to confirm the creation of a configuration file, etc.\n\n" +
            "Click \"Continue\" to move on or \"Cancel\" to exit this wizard.\n\n" +
            "Notice that you can always run this wizard again by choosing it from " +
            "the \"Help\" menu.";
    JLabel confirmLabel = new JLabel();
    confirmLabel.setPreferredSize(new Dimension(520, 400));
    confirmLabel.setOpaque(true);
    confirmLabel.setText(confirmString);
    int choice = -1;
    choice = confirmBox.getConfirm("Starting with GridPilot",
        confirmString, new Object[] {"Continue",  "Cancel"}, icon, Color.WHITE);
    
    if(choice!=0){
      return false;
    }
    return true;
  }
  
  private boolean endGreeting(boolean firstRun) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "Please notice that only the most basic parameters,\n" +
            "necessary to get you up and running have been set.\n" +
            "You can modify these and set many others in \"Edit\" -> \"Preferences\"." +
            (firstRun?"\n\nThanks for using GridPilot and have fun!":"");
    int choice = -1;
    choice = confirmBox.getConfirm("Starting with GridPilot",
        confirmString, new Object[] {"Continue",  "Cancel"}, icon, Color.WHITE);
    
    if(choice!=0){
      return false;
    }
    return true;
  }
  
  private void showError(String text) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "ERROR: could not set up GridPilot. "+text;
    confirmBox.getConfirm("Failed setting up GridPilot",
        confirmString, new Object[] {"OK"}, icon, Color.WHITE);
  }
  
  
  /**
   * Create the config file and some directories and set
   * config values.
   */
  private boolean checkDirs(boolean firstRun) throws IOException{
    JPanel jPanel = new JPanel(new GridBagLayout());
    File userConfDir = new File(System.getProperty("user.home") + File.separator +
      "GridPilot");
    File [] dirs = new File [] {
        userConfDir,
        new File(userConfDir, "cache"),
        new File(userConfDir, "jobs"),
        new File(userConfDir, "runtimeEnvironments"),
        new File(userConfDir, "transformations")
        };
    JTextField [] jtFields = new JTextField [dirs.length];
    if(firstRun){
      jPanel.add(new JLabel("A configuration file "+GridPilot.userConfFile.getCanonicalPath()+
          " will be created."),
          new GridBagConstraints(0, 0, 2, 1, 0.0, 0.0,
              GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
              new Insets(5, 5, 5, 5), 0, 0)) ;
    }
    jPanel.add(new JLabel("The following directories will be created if they don't already exist: "),
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    for(int i=0; i<dirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(dirs[i].getAbsolutePath());
      row = new JPanel(new BorderLayout());
      row.add(Util.createCheckPanel(
          (JFrame) SwingUtilities.getWindowAncestor(GridPilot.getClassMgr().getGlobalFrame().getRootPane()),
          dirs[i].getName(), jtFields[i]), BorderLayout.WEST);
      subRow = new JPanel(new BorderLayout());
      subRow.add(jtFields[i], BorderLayout.CENTER);
      subRow.add(new JLabel("   "), BorderLayout.SOUTH);
      subRow.add(new JLabel("   "), BorderLayout.NORTH);
      row.add(subRow, BorderLayout.EAST);
      jPanel.add(row, new GridBagConstraints(0, i+(firstRun?5:4), 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
    }
    jPanel.validate();
    
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Setting up GridPilot configuration",
          jPanel, new Object[] {"Continue",  "Cancel"}, icon, Color.WHITE);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    if(choice!=0){
      return false;
    }
    
    // Create missing directories
    for(int i=0; i<dirs.length; ++i){
      Debug.debug("Checking directory "+dirs[i], 2);
      dirs[i] = new File(jtFields[i].getText());
      if(dirs[i].exists()){
        if(!dirs[i].isDirectory()){
          throw new IOException("The directory "+userConfDir.getCanonicalPath()+" cannot be created.");
        }
      }
      else{
        Debug.debug("Creating directory "+dirs[i], 2);
        dirs[i].mkdir();
      }
    }

    ConfigFile tmpConfigFile = null;
    
    if(firstRun){
      tmpConfigFile = new ConfigFile(GridPilot.defaultConfFileName);
      // Make temporary config file
      File tmpConfFile = (File) GridPilot.tmpConfFile.get(GridPilot.defaultConfFileName);       
       // Copy over config file
       LocalStaticShellMgr.copyFile(tmpConfFile.getCanonicalPath(), GridPilot.userConfFile.getCanonicalPath());
       tmpConfFile.delete();
    }
    else{
      tmpConfigFile = GridPilot.getClassMgr().getConfigFile();
    }
    
    // Set config entries
    /* [GridPilot] pull cache directory = ~/GridPilot/cache
     * [My_DB_local] database = hsql://localhost/~/GridPilot/My_DB
     * [Fork] working directory = ~/GridPilot/jobs
     * [Fork] runtime directory = ~/GridPilot/runtimeEnvironments
     * [Fork] transformation directory = ~/GridPilot/transformations
     * */
     tmpConfigFile.setAttributes(
         new String [] {"GridPilot", "My_DB_local", "Fork", "Fork", "Fork"},
         new String [] {"pull cache directory", "database", "working directory", "runtime directory", "transformation directory"},
         new String [] {dirs[0].getCanonicalPath(), (new File(dirs[1], "My_DB_local")).getCanonicalPath(), dirs[2].getCanonicalPath(), dirs[3].getCanonicalPath(), dirs[4].getCanonicalPath()}
     );

    
    return true;
  }
  
  
}
