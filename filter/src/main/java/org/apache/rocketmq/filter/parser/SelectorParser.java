

/* Generated By:JavaCC: Do not edit this line. SelectorParser.java */
package org.apache.rocketmq.filter.parser;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.rocketmq.filter.expression.BooleanConstantExpression;
import org.apache.rocketmq.filter.expression.BooleanExpression;
import org.apache.rocketmq.filter.expression.ComparisonExpression;
import org.apache.rocketmq.filter.expression.ConstantExpression;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.expression.LogicExpression;
import org.apache.rocketmq.filter.expression.MQFilterException;
import org.apache.rocketmq.filter.expression.PropertyExpression;
import org.apache.rocketmq.filter.expression.UnaryExpression;

import java.io.StringReader;
import java.util.ArrayList;

/**
 * JMS Selector Parser generated by JavaCC
 * <p/>
 * Do not edit this .java file directly - it is autogenerated from SelectorParser.jj
 */
public class SelectorParser implements SelectorParserConstants {

    private static final Cache<String, Object> PARSE_CACHE = CacheBuilder.newBuilder().maximumSize(100).build();
    //    private static final String CONVERT_STRING_EXPRESSIONS_PREFIX = "convert_string_expressions:";
    static private int[] jjLa10;
    static private int[] jjLa11;

    static {
        jj_la1_init_0();
        jj_la1_init_1();
    }

    final private int[] jjLa1 = new int[13];
    final private JJCalls[] jj2Rtns = new JJCalls[4];
    final private LookaheadSuccess jjLs = new LookaheadSuccess();
    /**
     * Generated Token Manager.
     */
    public SelectorParserTokenManager tokenSource;
    /**
     * Current token.
     */
    public Token token;
    /**
     * Next token.
     */
    public Token jjNt;
    SimpleCharStream jjInputStream;
    private String sql;
    private int jjNtk;
    private Token jjScanpos, jjLastpos;
    private int jjLa;
    private int jjGen;
    private boolean jjRescan = false;
    private int jjGc = 0;
    private java.util.List<int[]> jjExpentries = new java.util.ArrayList<int[]>();
    private int[] jjExpentry;
    private int jjKind = -1;
    private int[] jjLasttokens = new int[100];
    private int jjEndpos;

    protected SelectorParser(String sql) {
        this(new StringReader(sql));
        this.sql = sql;
    }

    /**
     * Constructor with InputStream.
     */
    public SelectorParser(java.io.InputStream stream) {
        this(stream, null);
    }

    /**
     * Constructor with InputStream and supplied encoding
     */
    public SelectorParser(java.io.InputStream stream, String encoding) {
        try {
            jjInputStream = new SimpleCharStream(stream, encoding, 1, 1);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        tokenSource = new SelectorParserTokenManager(jjInputStream);
        token = new Token();
        jjNtk = -1;
        jjGen = 0;
        for (int i = 0; i < 13; i++)
            jjLa1[i] = -1;
        for (int i = 0; i < jj2Rtns.length; i++)
            jj2Rtns[i] = new JJCalls();
    }

    /**
     * Constructor.
     */
    public SelectorParser(java.io.Reader stream) {
        jjInputStream = new SimpleCharStream(stream, 1, 1);
        tokenSource = new SelectorParserTokenManager(jjInputStream);
        token = new Token();
        jjNtk = -1;
        jjGen = 0;
        for (int i = 0; i < 13; i++)
            jjLa1[i] = -1;
        for (int i = 0; i < jj2Rtns.length; i++)
            jj2Rtns[i] = new JJCalls();
    }

    /**
     * Constructor with generated Token Manager.
     */
    public SelectorParser(SelectorParserTokenManager tm) {
        tokenSource = tm;
        token = new Token();
        jjNtk = -1;
        jjGen = 0;
        for (int i = 0; i < 13; i++)
            jjLa1[i] = -1;
        for (int i = 0; i < jj2Rtns.length; i++)
            jj2Rtns[i] = new JJCalls();
    }

    public static BooleanExpression parse(String sql) throws MQFilterException {
        //        sql = "("+sql+")";
        Object result = PARSE_CACHE.getIfPresent(sql);
        if (result instanceof MQFilterException) {
            throw (MQFilterException) result;
        } else if (result instanceof BooleanExpression) {
            return (BooleanExpression) result;
        } else {

            //            boolean convertStringExpressions = false;
            //            if( sql.startsWith(CONVERT_STRING_EXPRESSIONS_PREFIX)) {
            //                convertStringExpressions = true;
            //                sql = sql.substring(CONVERT_STRING_EXPRESSIONS_PREFIX.length());
            //            }
            //            if( convertStringExpressions ) {
            //                ComparisonExpression.CONVERT_STRING_EXPRESSIONS.set(true);
            //            }
            ComparisonExpression.CONVERT_STRING_EXPRESSIONS.set(true);
            try {

                BooleanExpression e = new SelectorParser(sql).parse();
                PARSE_CACHE.put(sql, e);
                return e;
            } catch (MQFilterException t) {
                PARSE_CACHE.put(sql, t);
                throw t;
            } finally {
                ComparisonExpression.CONVERT_STRING_EXPRESSIONS.remove();
                //                if( convertStringExpressions ) {
                //                    ComparisonExpression.CONVERT_STRING_EXPRESSIONS.remove();
                //                }
            }
        }
    }

    public static void clearCache() {
        PARSE_CACHE.cleanUp();
    }

    private static void jj_la1_init_0() {
        jjLa10 = new int[]{
                0x400, 0x200, 0xc10000, 0xc00000, 0x10000, 0xf001900, 0x20000000, 0x20000000, 0xf000800,
                0x1000, 0x1036e100, 0x1036e000, 0x16e000};
    }

    private static void jj_la1_init_1() {
        jjLa11 = new int[]{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0};
    }

    protected BooleanExpression parse() throws MQFilterException {
        try {
            return this.JmsSelector();
        } catch (Throwable e) {
            throw new MQFilterException("Invalid MessageSelector. ", e);
        }
    }

    private BooleanExpression asBooleanExpression(Expression value) throws ParseException {
        if (value instanceof BooleanExpression) {
            return (BooleanExpression) value;
        }
        if (value instanceof PropertyExpression) {
            return UnaryExpression.createBooleanCast(value);
        }
        throw new ParseException("Expression will not result in a boolean value: " + value);
    }

    // ----------------------------------------------------------------------------
    // Grammer
    // ----------------------------------------------------------------------------
    final public BooleanExpression JmsSelector() throws ParseException {
        Expression left = null;
        left = orExpression();
        {
            if (true)
                return asBooleanExpression(left);
        }
        throw new Error("Missing return statement in function");
    }

    final public Expression orExpression() throws ParseException {
        Expression left;
        Expression right;
        left = andExpression();
        label_1:
        while (true) {
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case OR:
                    break;
                default:
                    jjLa1[0] = jjGen;
                    break label_1;
            }
            jj_consume_token(OR);
            right = andExpression();
            left = LogicExpression.createOR(asBooleanExpression(left), asBooleanExpression(right));
        }
        {
            if (true)
                return left;
        }
        throw new Error("Missing return statement in function");
    }

    final public Expression andExpression() throws ParseException {
        Expression left;
        Expression right;
        left = equalityExpression();
        label_2:
        while (true) {
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case AND:
                    break;
                default:
                    jjLa1[1] = jjGen;
                    break label_2;
            }
            jj_consume_token(AND);
            right = equalityExpression();
            left = LogicExpression.createAND(asBooleanExpression(left), asBooleanExpression(right));
        }
        {
            if (true)
                return left;
        }
        throw new Error("Missing return statement in function");
    }

    final public Expression equalityExpression() throws ParseException {
        Expression left;
        Expression right;
        left = comparisonExpression();
        label_3:
        while (true) {
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case IS:
                case 22:
                case 23:
                    break;
                default:
                    jjLa1[2] = jjGen;
                    break label_3;
            }
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case 22:
                    jj_consume_token(22);
                    right = comparisonExpression();
                    left = ComparisonExpression.createEqual(left, right);
                    break;
                case 23:
                    jj_consume_token(23);
                    right = comparisonExpression();
                    left = ComparisonExpression.createNotEqual(left, right);
                    break;
                default:
                    jjLa1[3] = jjGen;
                    if (jj_2_1(2)) {
                        jj_consume_token(IS);
                        jj_consume_token(NULL);
                        left = ComparisonExpression.createIsNull(left);
                    } else {
                        switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                            case IS:
                                jj_consume_token(IS);
                                jj_consume_token(NOT);
                                jj_consume_token(NULL);
                                left = ComparisonExpression.createIsNotNull(left);
                                break;
                            default:
                                jjLa1[4] = jjGen;
                                jj_consume_token(-1);
                                throw new ParseException();
                        }
                    }
            }
        }
        {
            if (true)
                return left;
        }
        throw new Error("Missing return statement in function");
    }

    final public Expression comparisonExpression() throws ParseException {
        Expression left;
        Expression right;
        Expression low;
        Expression high;
        String t, u;
        boolean not;
        ArrayList list;
        left = unaryExpr();
        label_4:
        while (true) {
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case NOT:
                case BETWEEN:
                case IN:
                case 24:
                case 25:
                case 26:
                case 27:
                    break;
                default:
                    jjLa1[5] = jjGen;
                    break label_4;
            }
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case 24:
                    jj_consume_token(24);
                    right = unaryExpr();
                    left = ComparisonExpression.createGreaterThan(left, right);
                    break;
                case 25:
                    jj_consume_token(25);
                    right = unaryExpr();
                    left = ComparisonExpression.createGreaterThanEqual(left, right);
                    break;
                case 26:
                    jj_consume_token(26);
                    right = unaryExpr();
                    left = ComparisonExpression.createLessThan(left, right);
                    break;
                case 27:
                    jj_consume_token(27);
                    right = unaryExpr();
                    left = ComparisonExpression.createLessThanEqual(left, right);
                    break;
                case BETWEEN:
                    jj_consume_token(BETWEEN);
                    low = unaryExpr();
                    jj_consume_token(AND);
                    high = unaryExpr();
                    left = ComparisonExpression.createBetween(left, low, high);
                    break;
                default:
                    jjLa1[8] = jjGen;
                    if (jj_2_2(2)) {
                        jj_consume_token(NOT);
                        jj_consume_token(BETWEEN);
                        low = unaryExpr();
                        jj_consume_token(AND);
                        high = unaryExpr();
                        left = ComparisonExpression.createNotBetween(left, low, high);
                    } else {
                        switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                            case IN:
                                jj_consume_token(IN);
                                jj_consume_token(28);
                                t = stringLitteral();
                                list = new ArrayList();
                                list.add(t);
                                label_5:
                                while (true) {
                                    switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                                        case 29:
                                            break;
                                        default:
                                            jjLa1[6] = jjGen;
                                            break label_5;
                                    }
                                    jj_consume_token(29);
                                    t = stringLitteral();
                                    list.add(t);
                                }
                                jj_consume_token(30);
                                left = ComparisonExpression.createInFilter(left, list);
                                break;
                            default:
                                jjLa1[9] = jjGen;
                                if (jj_2_3(2)) {
                                    jj_consume_token(NOT);
                                    jj_consume_token(IN);
                                    jj_consume_token(28);
                                    t = stringLitteral();
                                    list = new ArrayList();
                                    list.add(t);
                                    label_6:
                                    while (true) {
                                        switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                                            case 29:
                                                break;
                                            default:
                                                jjLa1[7] = jjGen;
                                                break label_6;
                                        }
                                        jj_consume_token(29);
                                        t = stringLitteral();
                                        list.add(t);
                                    }
                                    jj_consume_token(30);
                                    left = ComparisonExpression.createNotInFilter(left, list);
                                } else {
                                    jj_consume_token(-1);
                                    throw new ParseException();
                                }
                        }
                    }
            }
        }
        {
            if (true)
                return left;
        }
        throw new Error("Missing return statement in function");
    }

    final public Expression unaryExpr() throws ParseException {
        String s = null;
        Expression left = null;
        if (jj_2_4(2147483647)) {
            jj_consume_token(31);
            left = unaryExpr();
        } else {
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case 32:
                    jj_consume_token(32);
                    left = unaryExpr();
                    left = UnaryExpression.createNegate(left);
                    break;
                case NOT:
                    jj_consume_token(NOT);
                    left = unaryExpr();
                    left = UnaryExpression.createNOT(asBooleanExpression(left));
                    break;
                case TRUE:
                case FALSE:
                case NULL:
                case DECIMAL_LITERAL:
                case FLOATING_POINT_LITERAL:
                case STRING_LITERAL:
                case ID:
                case 28:
                    left = primaryExpr();
                    break;
                default:
                    jjLa1[10] = jjGen;
                    jj_consume_token(-1);
                    throw new ParseException();
            }
        }
        {
            if (true)
                return left;
        }
        throw new Error("Missing return statement in function");
    }

    final public Expression primaryExpr() throws ParseException {
        Expression left = null;
        switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
            case TRUE:
            case FALSE:
            case NULL:
            case DECIMAL_LITERAL:
            case FLOATING_POINT_LITERAL:
            case STRING_LITERAL:
                left = literal();
                break;
            case ID:
                left = variable();
                break;
            case 28:
                jj_consume_token(28);
                left = orExpression();
                jj_consume_token(30);
                break;
            default:
                jjLa1[11] = jjGen;
                jj_consume_token(-1);
                throw new ParseException();
        }
        {
            if (true)
                return left;
        }
        throw new Error("Missing return statement in function");
    }

    final public ConstantExpression literal() throws ParseException {
        Token t;
        String s;
        ConstantExpression left = null;
        switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
            case STRING_LITERAL:
                s = stringLitteral();
                left = new ConstantExpression(s);
                break;
            case DECIMAL_LITERAL:
                t = jj_consume_token(DECIMAL_LITERAL);
                left = ConstantExpression.createFromDecimal(t.image);
                break;
            case FLOATING_POINT_LITERAL:
                t = jj_consume_token(FLOATING_POINT_LITERAL);
                left = ConstantExpression.createFloat(t.image);
                break;
            case TRUE:
                jj_consume_token(TRUE);
                left = BooleanConstantExpression.TRUE;
                break;
            case FALSE:
                jj_consume_token(FALSE);
                left = BooleanConstantExpression.FALSE;
                break;
            case NULL:
                jj_consume_token(NULL);
                left = BooleanConstantExpression.NULL;
                break;
            default:
                jjLa1[12] = jjGen;
                jj_consume_token(-1);
                throw new ParseException();
        }
        {
            if (true)
                return left;
        }
        throw new Error("Missing return statement in function");
    }

    final public String stringLitteral() throws ParseException {
        Token t;
        StringBuffer rc = new StringBuffer();
        boolean first = true;
        t = jj_consume_token(STRING_LITERAL);
        // Decode the sting value.
        String image = t.image;
        for (int i = 1; i < image.length() - 1; i++) {
            char c = image.charAt(i);
            if (c == '\'')
                i++;
            rc.append(c);
        }
        {
            if (true)
                return rc.toString();
        }
        throw new Error("Missing return statement in function");
    }

    final public PropertyExpression variable() throws ParseException {
        Token t;
        PropertyExpression left = null;
        t = jj_consume_token(ID);
        left = new PropertyExpression(t.image);
        {
            if (true)
                return left;
        }
        throw new Error("Missing return statement in function");
    }

    private boolean jj_2_1(int xla) {
        jjLa = xla;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_1();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(0, xla);
        }
    }

    private boolean jj_2_2(int xla) {
        jjLa = xla;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_2();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(1, xla);
        }
    }

    private boolean jj_2_3(int xla) {
        jjLa = xla;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_3();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(2, xla);
        }
    }

    private boolean jj_2_4(int xla) {
        jjLa = xla;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_4();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(3, xla);
        }
    }

    private boolean jj_3R_7() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_8()) {
            jjScanpos = xsp;
            if (jj_3R_9()) {
                jjScanpos = xsp;
                if (jj_3R_10()) {
                    jjScanpos = xsp;
                    if (jj_3R_11())
                        return true;
                }
            }
        }
        return false;
    }

    private boolean jj_3R_43() {
        if (jj_scan_token(29))
            return true;
        if (jj_3R_27())
            return true;
        return false;
    }

    private boolean jj_3R_24() {
        if (jj_scan_token(NULL))
            return true;
        return false;
    }

    private boolean jj_3R_35() {
        if (jj_scan_token(IS))
            return true;
        if (jj_scan_token(NOT))
            return true;
        if (jj_scan_token(NULL))
            return true;
        return false;
    }

    private boolean jj_3_1() {
        if (jj_scan_token(IS))
            return true;
        if (jj_scan_token(NULL))
            return true;
        return false;
    }

    private boolean jj_3R_23() {
        if (jj_scan_token(FALSE))
            return true;
        return false;
    }

    private boolean jj_3R_34() {
        if (jj_scan_token(23))
            return true;
        if (jj_3R_30())
            return true;
        return false;
    }

    private boolean jj_3R_22() {
        if (jj_scan_token(TRUE))
            return true;
        return false;
    }

    private boolean jj_3_3() {
        if (jj_scan_token(NOT))
            return true;
        if (jj_scan_token(IN))
            return true;
        if (jj_scan_token(28))
            return true;
        if (jj_3R_27())
            return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_43()) {
                jjScanpos = xsp;
                break;
            }
        }
        if (jj_scan_token(30))
            return true;
        return false;
    }

    private boolean jj_3R_31() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_33()) {
            jjScanpos = xsp;
            if (jj_3R_34()) {
                jjScanpos = xsp;
                if (jj_3_1()) {
                    jjScanpos = xsp;
                    if (jj_3R_35())
                        return true;
                }
            }
        }
        return false;
    }

    private boolean jj_3R_33() {
        if (jj_scan_token(22))
            return true;
        if (jj_3R_30())
            return true;
        return false;
    }

    private boolean jj_3R_42() {
        if (jj_scan_token(29))
            return true;
        if (jj_3R_27())
            return true;
        return false;
    }

    private boolean jj_3R_21() {
        if (jj_scan_token(FLOATING_POINT_LITERAL))
            return true;
        return false;
    }

    private boolean jj_3R_20() {
        if (jj_scan_token(DECIMAL_LITERAL))
            return true;
        return false;
    }

    private boolean jj_3R_28() {
        if (jj_3R_30())
            return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_31()) {
                jjScanpos = xsp;
                break;
            }
        }
        return false;
    }

    private boolean jj_3R_41() {
        if (jj_scan_token(IN))
            return true;
        if (jj_scan_token(28))
            return true;
        if (jj_3R_27())
            return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_42()) {
                jjScanpos = xsp;
                break;
            }
        }
        if (jj_scan_token(30))
            return true;
        return false;
    }

    private boolean jj_3R_19() {
        if (jj_3R_27())
            return true;
        return false;
    }

    private boolean jj_3R_29() {
        if (jj_scan_token(AND))
            return true;
        if (jj_3R_28())
            return true;
        return false;
    }

    private boolean jj_3R_16() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_19()) {
            jjScanpos = xsp;
            if (jj_3R_20()) {
                jjScanpos = xsp;
                if (jj_3R_21()) {
                    jjScanpos = xsp;
                    if (jj_3R_22()) {
                        jjScanpos = xsp;
                        if (jj_3R_23()) {
                            jjScanpos = xsp;
                            if (jj_3R_24())
                                return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean jj_3_2() {
        if (jj_scan_token(NOT))
            return true;
        if (jj_scan_token(BETWEEN))
            return true;
        if (jj_3R_7())
            return true;
        if (jj_scan_token(AND))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    private boolean jj_3R_40() {
        if (jj_scan_token(BETWEEN))
            return true;
        if (jj_3R_7())
            return true;
        if (jj_scan_token(AND))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    private boolean jj_3R_25() {
        if (jj_3R_28())
            return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_29()) {
                jjScanpos = xsp;
                break;
            }
        }
        return false;
    }

    private boolean jj_3R_39() {
        if (jj_scan_token(27))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    private boolean jj_3R_15() {
        if (jj_scan_token(28))
            return true;
        if (jj_3R_18())
            return true;
        if (jj_scan_token(30))
            return true;
        return false;
    }

    private boolean jj_3R_14() {
        if (jj_3R_17())
            return true;
        return false;
    }

    private boolean jj_3R_38() {
        if (jj_scan_token(26))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    private boolean jj_3R_13() {
        if (jj_3R_16())
            return true;
        return false;
    }

    private boolean jj_3R_26() {
        if (jj_scan_token(OR))
            return true;
        if (jj_3R_25())
            return true;
        return false;
    }

    private boolean jj_3R_17() {
        if (jj_scan_token(ID))
            return true;
        return false;
    }

    private boolean jj_3R_37() {
        if (jj_scan_token(25))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    private boolean jj_3R_12() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_13()) {
            jjScanpos = xsp;
            if (jj_3R_14()) {
                jjScanpos = xsp;
                if (jj_3R_15())
                    return true;
            }
        }
        return false;
    }

    private boolean jj_3R_32() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_36()) {
            jjScanpos = xsp;
            if (jj_3R_37()) {
                jjScanpos = xsp;
                if (jj_3R_38()) {
                    jjScanpos = xsp;
                    if (jj_3R_39()) {
                        jjScanpos = xsp;
                        if (jj_3R_40()) {
                            jjScanpos = xsp;
                            if (jj_3_2()) {
                                jjScanpos = xsp;
                                if (jj_3R_41()) {
                                    jjScanpos = xsp;
                                    if (jj_3_3())
                                        return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean jj_3R_36() {
        if (jj_scan_token(24))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    private boolean jj_3R_11() {
        if (jj_3R_12())
            return true;
        return false;
    }

    private boolean jj_3R_18() {
        if (jj_3R_25())
            return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_26()) {
                jjScanpos = xsp;
                break;
            }
        }
        return false;
    }

    private boolean jj_3_4() {
        if (jj_scan_token(31))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    private boolean jj_3R_10() {
        if (jj_scan_token(NOT))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    private boolean jj_3R_9() {
        if (jj_scan_token(32))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    private boolean jj_3R_27() {
        if (jj_scan_token(STRING_LITERAL))
            return true;
        return false;
    }

    private boolean jj_3R_30() {
        if (jj_3R_7())
            return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_32()) {
                jjScanpos = xsp;
                break;
            }
        }
        return false;
    }

    private boolean jj_3R_8() {
        if (jj_scan_token(31))
            return true;
        if (jj_3R_7())
            return true;
        return false;
    }

    /**
     * Reinitialise.
     */
    public void ReInit(java.io.InputStream stream) {
        ReInit(stream, null);
    }

    /**
     * Reinitialise.
     */
    public void ReInit(java.io.InputStream stream, String encoding) {
        try {
            jjInputStream.ReInit(stream, encoding, 1, 1);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        tokenSource.ReInit(jjInputStream);
        token = new Token();
        jjNtk = -1;
        jjGen = 0;
        for (int i = 0; i < 13; i++)
            jjLa1[i] = -1;
        for (int i = 0; i < jj2Rtns.length; i++)
            jj2Rtns[i] = new JJCalls();
    }

    /**
     * Reinitialise.
     */
    public void ReInit(java.io.Reader stream) {
        jjInputStream.ReInit(stream, 1, 1);
        tokenSource.ReInit(jjInputStream);
        token = new Token();
        jjNtk = -1;
        jjGen = 0;
        for (int i = 0; i < 13; i++)
            jjLa1[i] = -1;
        for (int i = 0; i < jj2Rtns.length; i++)
            jj2Rtns[i] = new JJCalls();
    }

    /**
     * Reinitialise.
     */
    public void ReInit(SelectorParserTokenManager tm) {
        tokenSource = tm;
        token = new Token();
        jjNtk = -1;
        jjGen = 0;
        for (int i = 0; i < 13; i++)
            jjLa1[i] = -1;
        for (int i = 0; i < jj2Rtns.length; i++)
            jj2Rtns[i] = new JJCalls();
    }

    private Token jj_consume_token(int kind) throws ParseException {
        Token oldToken;
        if ((oldToken = token).next != null)
            token = token.next;
        else
            token = token.next = tokenSource.getNextToken();
        jjNtk = -1;
        if (token.kind == kind) {
            jjGen++;
            if (++jjGc > 100) {
                jjGc = 0;
                for (int i = 0; i < jj2Rtns.length; i++) {
                    JJCalls c = jj2Rtns[i];
                    while (c != null) {
                        if (c.gen < jjGen)
                            c.first = null;
                        c = c.next;
                    }
                }
            }
            return token;
        }
        token = oldToken;
        jjKind = kind;
        throw generateParseException();
    }

    private boolean jj_scan_token(int kind) {
        if (jjScanpos == jjLastpos) {
            jjLa--;
            if (jjScanpos.next == null) {
                jjLastpos = jjScanpos = jjScanpos.next = tokenSource.getNextToken();
            } else {
                jjLastpos = jjScanpos = jjScanpos.next;
            }
        } else {
            jjScanpos = jjScanpos.next;
        }
        if (jjRescan) {
            int i = 0;
            Token tok = token;
            while (tok != null && tok != jjScanpos) {
                i++;
                tok = tok.next;
            }
            if (tok != null)
                jj_add_error_token(kind, i);
        }
        if (jjScanpos.kind != kind)
            return true;
        if (jjLa == 0 && jjScanpos == jjLastpos)
            throw jjLs;
        return false;
    }

    /**
     * Get the next Token.
     */
    final public Token getNextToken() {
        if (token.next != null)
            token = token.next;
        else
            token = token.next = tokenSource.getNextToken();
        jjNtk = -1;
        jjGen++;
        return token;
    }

    /**
     * Get the specific Token.
     */
    final public Token getToken(int index) {
        Token t = token;
        for (int i = 0; i < index; i++) {
            if (t.next != null)
                t = t.next;
            else
                t = t.next = tokenSource.getNextToken();
        }
        return t;
    }

    private int jj_ntk() {
        if ((jjNt = token.next) == null)
            return jjNtk = (token.next = tokenSource.getNextToken()).kind;
        else
            return jjNtk = jjNt.kind;
    }

    private void jj_add_error_token(int kind, int pos) {
        if (pos >= 100)
            return;
        if (pos == jjEndpos + 1) {
            jjLasttokens[jjEndpos++] = kind;
        } else if (jjEndpos != 0) {
            jjExpentry = new int[jjEndpos];
            for (int i = 0; i < jjEndpos; i++) {
                jjExpentry[i] = jjLasttokens[i];
            }
            jj_entries_loop:
            for (java.util.Iterator<?> it = jjExpentries.iterator(); it.hasNext(); ) {
                int[] oldentry = (int[]) (it.next());
                if (oldentry.length == jjExpentry.length) {
                    for (int i = 0; i < jjExpentry.length; i++) {
                        if (oldentry[i] != jjExpentry[i]) {
                            continue jj_entries_loop;
                        }
                    }
                    jjExpentries.add(jjExpentry);
                    break jj_entries_loop;
                }
            }
            if (pos != 0)
                jjLasttokens[(jjEndpos = pos) - 1] = kind;
        }
    }

    /**
     * Generate ParseException.
     */
    public ParseException generateParseException() {
        jjExpentries.clear();
        boolean[] la1tokens = new boolean[33];
        if (jjKind >= 0) {
            la1tokens[jjKind] = true;
            jjKind = -1;
        }
        for (int i = 0; i < 13; i++) {
            if (jjLa1[i] == jjGen) {
                for (int j = 0; j < 32; j++) {
                    if ((jjLa10[i] & (1 << j)) != 0) {
                        la1tokens[j] = true;
                    }
                    if ((jjLa11[i] & (1 << j)) != 0) {
                        la1tokens[32 + j] = true;
                    }
                }
            }
        }
        for (int i = 0; i < 33; i++) {
            if (la1tokens[i]) {
                jjExpentry = new int[1];
                jjExpentry[0] = i;
                jjExpentries.add(jjExpentry);
            }
        }
        jjEndpos = 0;
        jj_rescan_token();
        jj_add_error_token(0, 0);
        int[][] exptokseq = new int[jjExpentries.size()][];
        for (int i = 0; i < jjExpentries.size(); i++) {
            exptokseq[i] = jjExpentries.get(i);
        }
        return new ParseException(token, exptokseq, TOKEN_IMAGE);
    }

    /**
     * Enable tracing.
     */
    final public void enable_tracing() {
    }

    /**
     * Disable tracing.
     */
    final public void disable_tracing() {
    }

    private void jj_rescan_token() {
        jjRescan = true;
        for (int i = 0; i < 4; i++) {
            try {
                JJCalls p = jj2Rtns[i];
                do {
                    if (p.gen > jjGen) {
                        jjLa = p.arg;
                        jjLastpos = jjScanpos = p.first;
                        switch (i) {
                            case 0:
                                jj_3_1();
                                break;
                            case 1:
                                jj_3_2();
                                break;
                            case 2:
                                jj_3_3();
                                break;
                            case 3:
                                jj_3_4();
                                break;
                        }
                    }
                    p = p.next;
                }
                while (p != null);
            } catch (LookaheadSuccess ls) {
            }
        }
        jjRescan = false;
    }

    private void jj_save(int index, int xla) {
        JJCalls p = jj2Rtns[index];
        while (p.gen > jjGen) {
            if (p.next == null) {
                p = p.next = new JJCalls();
                break;
            }
            p = p.next;
        }
        p.gen = jjGen + xla - jjLa;
        p.first = token;
        p.arg = xla;
    }

    static private final class LookaheadSuccess extends java.lang.Error {
    }

    static final class JJCalls {
        int gen;
        Token first;
        int arg;
        JJCalls next;
    }

}
