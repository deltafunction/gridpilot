package gridpilot;

/**
 * Syntax error in arithmetic expression.
 *
 */
class SyntaxException extends Exception{
  private static final long serialVersionUID = 1L;

  public SyntaxException(String msg){
    super(msg);
  }
}
