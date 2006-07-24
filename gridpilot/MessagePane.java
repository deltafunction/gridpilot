package gridpilot;

import javax.swing.*;

/**
 * Shows a message.
 * (Try to) avoid(s) that with a big message, the box is greater than the screen.
 *
 */
public class MessagePane{

  public static void showMessage(String message, String title){
    JTextPane ta = new JTextPane();
    ta.setText(message);

    //ta.setLineWrap(true);
    //ta.setWrapStyleWord(true);
    ta.setOpaque(false);
    ta.setEditable(false);

    JOptionPane op = new JOptionPane(ta, JOptionPane.PLAIN_MESSAGE);

    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), title);
    dialog.setResizable(true);
    //ta.getPreferredSize(); // without this line, this dialog is too small !!! ???????
    dialog.pack();
    dialog.validate();
    dialog.setVisible(true);
    dialog.dispose();

  }
}
