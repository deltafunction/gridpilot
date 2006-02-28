package gridpilot;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;

/**
 * Box showing about or help.
 * 
 */
public class ConfirmBox extends JDialog implements ActionListener {

  private JPanel panel = new JPanel(new BorderLayout());

  private JButton bOk = new JButton();
  private JButton bCancel = new JButton();
  private JPanel pButton = new JPanel(new FlowLayout());
  JEditorPane ep;
  Frame parent;

  public ConfirmBox(Frame parent/*, String title, String text*/) {
    super(parent);
    this.parent = parent;
   enableEvents(AWTEvent.WINDOW_EVENT_MASK);
   // try{
		//getConfirm(title, text);
    //}catch(Exception e){
     // e.printStackTrace();
    //}
  }

  public int getConfirm(String title, String text,Object[] showResultsOptions ) throws Exception  {
    
    JOptionPane op = new JOptionPane(
         //new JLabel(text),
         text,
         JOptionPane.QUESTION_MESSAGE,
         JOptionPane.YES_NO_CANCEL_OPTION,
         null,
         showResultsOptions,
         showResultsOptions[0]);

    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), title);
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();

    Object selectedValue = op.getValue();

    if (selectedValue == null)
    return JOptionPane.CLOSED_OPTION;
    for (int i = 0; i < showResultsOptions.length; ++i)
    if (showResultsOptions[i] == selectedValue)
    return i;
    return JOptionPane.CLOSED_OPTION;

  }


  public Dimension getPreferredSize(){
    return panel.getPreferredSize();
  }

  public Dimension getMinimumSize(){
    return panel.getPreferredSize();
  }

  //Overridden so we can exit when window is closed
  protected void processWindowEvent(WindowEvent e) {
    if (e.getID() == WindowEvent.WINDOW_CLOSING) {
      cancel();
    }
    super.processWindowEvent(e);
  }

  //Close the dialog
  void cancel() {
    dispose();
  }
  //Close the dialog on a button event
  public void actionPerformed(ActionEvent e) {
    if (e.getSource() == bOk) {
      cancel();
    }
	if (e.getSource() == bCancel) {
	  cancel();
	}
  }
}