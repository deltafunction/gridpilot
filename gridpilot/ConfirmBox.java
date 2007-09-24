package gridpilot;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;

/**
 * Box showing about, help or error message.
 */
public class ConfirmBox extends JDialog implements ActionListener {

  private static final long serialVersionUID = 1L;
  private JButton bOk = new JButton();
  private JButton bCancel = new JButton();
  private JDialog dialog = null;

  public ConfirmBox(Frame parent){
    super(parent);
   enableEvents(AWTEvent.WINDOW_EVENT_MASK);
  }

  /**
   * Return the JDialog object created by getConfirm. 
   * 
   * @return a JDialog object
   */
  public JDialog getDialog(){
    return dialog;
  }
  
  /**
   * Show a dialog box containing a given text and a number of
   * buttons corresponding to choices.
   * 
   * @param title the title text displayed on the window frame
   * @param text the body text displayed in the window
   * @param showResultsOptions an array of Objects that can be either
   *                           strings or JComponents. If an object is a string,
   *                           a JButton is created with the string as button text.
   * @return an integer corresponding to the button clicked: 0, 1, ...
   * @throws Exception
   */
  public int getConfirm(String title, Object text, Object[] showResultsOptions) throws Exception {
    return getConfirm(title, text, showResultsOptions, null, null, true);
  }
  
  /**
   * Show a dialog box containing a given text and a number of
   * buttons corresponding to choices.
   * 
   * @param title the title text displayed on the window frame
   * @param text the body text displayed in the window
   * @param showResultsOptions an array of Objects that can be either
   *                           strings or JComponents. If an object is a string,
   *                           a JButton is created with the string as button text.
   * @param icon an Icon to be displayed on the window
   * @param bgColor the background color of the window
   * @param isResizable whether or not the window should be resizable
   * @return an integer corresponding to the button clicked: 0, 1, ...
   * @throws Exception
   */
  public int getConfirm(String title, Object text, Object[] showResultsOptions,
      Icon icon, Color bgColor, boolean isResizable) throws Exception {
    
    JComponent area = null;
    Object pane = null;
    boolean plainText = false;
    
    try{
      area = (JComponent) text;
      pane = new JScrollPane(area);
    }
    catch(ClassCastException e){
      plainText = true;
      pane = text;
    }
    
    //pane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
    
    JOptionPane op = new JOptionPane(
         pane,
         JOptionPane.QUESTION_MESSAGE,
         JOptionPane.YES_NO_CANCEL_OPTION,
         icon,
         showResultsOptions,
         showResultsOptions[0]);
    
    dialog = op.createDialog(JOptionPane.getRootFrame(), title);
    dialog.setResizable(isResizable);
    if(bgColor!=null){
      recolor(dialog.getContentPane(), bgColor);
    }
    
    if(!plainText){
      dialog.getContentPane().setMaximumSize(new Dimension(area.getPreferredSize().width+20,
          Toolkit.getDefaultToolkit().getScreenSize().height-200));
    }
    
    dialog.pack();
    dialog.setVisible(true);
    dialog.dispose();
    
    Object selectedValue = op.getValue();

    if (selectedValue==null){
      return JOptionPane.CLOSED_OPTION;
    }
    for(int i=0; i<showResultsOptions.length; ++i){
      if(showResultsOptions[i]==selectedValue){
        return i;
      }
    }
    return JOptionPane.CLOSED_OPTION;
  }

  private static void recolor(Component component, Color color) {
    component.setBackground(color);
    if(component instanceof Container){
        Container container = (Container) component;
        for(int i=0, ub=container.getComponentCount(); i<ub; ++i)
            recolor(container.getComponent(i), color);
    }
  }

  //Overridden so we can exit when window is closed
  protected void processWindowEvent(WindowEvent e){
    if(e.getID()==WindowEvent.WINDOW_CLOSING){
      cancel();
    }
    super.processWindowEvent(e);
  }

  //Close the dialog
  void cancel() {
    dispose();
  }
  
  //Close the dialog on a button event
  public void actionPerformed(ActionEvent e){
    if (e.getSource()==bOk){
      cancel();
    }
    if (e.getSource()==bCancel){
      cancel();
    }
  }
}