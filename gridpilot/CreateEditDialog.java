package gridpilot;

import gridfactory.common.Debug;

import java.awt.*;
import javax.swing.*;
import java.awt.event.*;

public class CreateEditDialog extends GPFrame /*implements ComponentListener*/{

  private static final long serialVersionUID = 1L;
  private static final int BCLOSE = 0;
  private static final int BCREATEUPDATE = 1;
  private static final int BCLEAR = 2;
  private static final int BSAVE_SETTINGS = 3;
  private JPanel pCreateEdit = new JPanel(new BorderLayout());
  private CreateEditPanel createEditPanel;
  private boolean editing;
  private JButton bClose = new JButton("Close");
  private JButton bCreateUpdate = null;
  private JButton bSaveSettings = new JButton("Save values");
  private JButton bClear = new JButton("Clear");
  private JCheckBox cbShowResults = new JCheckBox("Confirm before writing", true);
  private boolean showDetailsCheckBox = false;
  private boolean showButtons = false;
  private boolean showSaveSettings = false;
  private JCheckBox cbShowDetails = null;
  private JPanel buttonPanel = new JPanel();

  public CreateEditDialog(CreateEditPanel _panel, boolean _editing,
      boolean _showDetailsCheckBox, boolean _showButtons, boolean _showSaveSettings){
    
    super();
    
    _panel.statusBar = this.statusBar;
    
    showDetailsCheckBox = _showDetailsCheckBox;
    showButtons = _showButtons;
    createEditPanel = _panel;
    editing = _editing;
    showSaveSettings = _showSaveSettings;
    
    this.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
    this.addWindowListener(new WindowAdapter(){
      public void windowClosing(WindowEvent we){
        createEditPanel.windowClosing();
        we.getWindow().setVisible(false);
        Debug.debug("Thwarted user attempt to close window.", 3);
      }
    });
    
    if(editing){
      bCreateUpdate = new JButton("Update");
    }
    else{
      bCreateUpdate = new JButton("Create");
    }

    try{
      //setContentPane(pCreateEdit);
      this.getContentPane().add(pCreateEdit, BorderLayout.CENTER);
      initGUI();
      pack();
      requestFocusInWindow();
      // Doesn't seem to make any difference...
      //setAlwaysOnTop(false);
      this.setVisible(true);
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  
  /*public void componentResized(ComponentEvent e){
    pCreateEdit.remove(createEditPanel);
    pCreateEdit.add(createEditPanel, BorderLayout.CENTER);
    pCreateEdit.validate();
    pCreateEdit.setVisible(true);
    Debug.debug("componentResized event from "
         + e.getComponent().getClass().getName(), 3);
  }

  public void componentHidden(ComponentEvent e) {
  }
   
  public void componentMoved(ComponentEvent e) {    
  }
      
  public void componentShown(ComponentEvent e){ 
  }*/
  
  public void initGUI(){
    //buttonPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
    
    /*this.addComponentListener(this);
    createEditPanel.addComponentListener(this);*/

    // buttons initialisation
    bClose.setMnemonic(BCLOSE);
    bClose.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    bClear.setMnemonic(BCLEAR);
    bClear.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    bCreateUpdate.setMnemonic(BCREATEUPDATE);
    bCreateUpdate.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    bSaveSettings.setMnemonic(BSAVE_SETTINGS);
    bSaveSettings.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    pCreateEdit.add(createEditPanel, BorderLayout.CENTER);

    if(showButtons){
      if(showDetailsCheckBox){
        cbShowDetails = new JCheckBox("Show details", false);
        cbShowDetails.addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent e){
            try{
              createEditPanel.showDetails(cbShowDetails.isSelected());
              pack();
            }
            catch(Exception ex){
              Debug.debug("Could not show details", 2);
              ex.printStackTrace();
            }
          }
        });    
        buttonPanel.add(cbShowDetails);
      }
      buttonPanel.add(cbShowResults);
      buttonPanel.add(bClose);
      buttonPanel.add(bClear);
      buttonPanel.add(new JLabel("|"));
      buttonPanel.add(bCreateUpdate);
      if(showSaveSettings){
        buttonPanel.add(bSaveSettings);
      }

      pCreateEdit.add(buttonPanel, BorderLayout.SOUTH);
    }

    // center in window
    //
    //Dimension  scrnSize = Toolkit.getDefaultToolkit().getScreenSize();
    //setLocation((scrnSize.width / 2) - 150, (scrnSize.height / 2) - 50);
    //this.setSize(new Dimension(400, 0));

    // Initialize CreateEditPanel
    createEditPanel.initGUI();
    
    pack();
  }
  
  public void setBCreateUpdateEnabled(boolean ok){
    bCreateUpdate.setEnabled(ok);
  }

  /**
   * Called when a button is clicked
   */
  void button_actionPerformed(ActionEvent e){
    switch(((JButton)e.getSource()).getMnemonic()){
      case BCLOSE :
        createEditPanel.windowClosing();
        this.setVisible(false);
        break;

      case BCREATEUPDATE :
        new Thread(){
          public void run(){
            createEditPanel.create(cbShowResults.isSelected(), editing);
          }
        }.start();
        break;

      case BSAVE_SETTINGS :
        new Thread(){
          public void run(){
            createEditPanel.saveSettings();
          }
        }.start();
        break;

      case BCLEAR :
        createEditPanel.clearPanel();
        break;
    }
  }
}
