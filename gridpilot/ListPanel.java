package gridpilot;

/**
 * Interface implemented by the main panels listing database results.
 *
 */

public interface ListPanel{
  public void panelShown();
  public void panelHidden();
  public String getTitle();
  /**
   * Copy selected database records to the clipboard
   * as a string.
   */
  public void copy();
  /**
   * Cut selected database records to the clipboard
   * as a string.
   */
  public void cut();
  /**
   * If possible: paste the database records currently on the clipboard
   * into the currently selected table.
   */
  public void paste();
}
