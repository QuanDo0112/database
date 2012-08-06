package simpledb;

/** Predicate compares tuples to a specified Field value.
 */
public class Predicate {
	
    /** Constants used for return codes in Field.compare */
    public enum Op {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
    }
    
    private int _field;
    private Op _op;
    private Field _operand;

    /**
     * Constructor.
     *
     * @param field field number of passed in tuples to compare against.
     * @param op operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        _field = field;
        _op = op;
        _operand = operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific
     * in the constructor.  The comparison can be made through Field's
     * compare method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
    	Field result = t.getField(_field);
    	return result.compare(_op, _operand);
    }

    /**
     * Returns something useful, like
     * "f = field_id op = op_string operand = operand_string
     */
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("Field = " + _field);
    	sb.append(" op = " + _op);
    	sb.append( " = " + _field);
    	return sb.toString();
    }
}
