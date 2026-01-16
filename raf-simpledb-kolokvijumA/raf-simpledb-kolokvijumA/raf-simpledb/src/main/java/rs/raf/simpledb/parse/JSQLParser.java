package rs.raf.simpledb.parse;

import java.util.*;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;

import rs.raf.simpledb.query.*;
import rs.raf.simpledb.query.aggregation.AggregationFn;
import rs.raf.simpledb.query.aggregation.CountFn;
import rs.raf.simpledb.record.Schema;

public class JSQLParser {

    /* ======================= SELECT ======================= */

    public QueryData query(String sql) throws JSQLParserException {

        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect ps = (PlainSelect) select.getSelectBody();

        /* SELECT */
        Collection<String> fields = new ArrayList<>();
        Collection<AggregationFn> aggfns = new ArrayList<>(); // dodano

        for (SelectItem<?> item : ps.getSelectItems()) {

            Expression expr = (Expression) item.getExpression();

            if (expr instanceof Column) {
                fields.add(((Column) expr).getColumnName());
            }
            else if (expr instanceof Function) {
                Function fn = (Function) expr;
                String fnName = fn.getName().toUpperCase(); // COUNT, SUM, AVG...
                Expression arg = fn.getParameters().getExpressions().get(0);

                String field;
                if (arg instanceof Column)
                    field = ((Column)arg).getColumnName();
                else
                    field = arg.toString();

                fields.add(fnName + "(" + field + ")");

                // kreiraj AggregationFn i dodaj u listu
                aggfns.add(new CountFn(field));
            }
            else {
                fields.add(expr.toString());
            }
        }

        /* FROM */
        Collection<String> tables = new ArrayList<>();
        tables.add(ps.getFromItem().toString());

        if (ps.getJoins() != null) {
            for (Join j : ps.getJoins()) {
                tables.add(j.getRightItem().toString());
            }
        }

        /* WHERE */
        Predicate pred = new Predicate();
        if (ps.getWhere() != null) {
            pred = parseWhere(ps.getWhere());
        }

        /* GROUP BY */
        Collection<String> groupBy = new ArrayList<>();

        if (ps.getGroupBy() != null && ps.getGroupBy().getGroupByExpressions() != null) {
            for (Object e : ps.getGroupBy().getGroupByExpressions()) {
                if (e instanceof Column)
                    groupBy.add(((Column) e).getColumnName());
                else
                    groupBy.add(e.toString());
            }
        }

        /* ORDER BY */
        Collection<String> orderBy = new ArrayList<>();
        if (ps.getOrderByElements() != null) {
            for (OrderByElement obe : ps.getOrderByElements()) {
                Expression e = obe.getExpression();
                String col;
                if (e instanceof Column)
                    col = ((Column) e).getColumnName();
                else
                    col = e.toString();

                if (obe.isAsc())
                    orderBy.add(col + " ASC");
                else
                    orderBy.add(col + " DESC");
            }
        }

        return new QueryData(
                fields,
                tables,
                groupBy,
                orderBy,
                pred,
                aggfns // prosleđuje agregacije
        );
    }


    /* ======================= WHERE ======================= */

    private Predicate parseWhere(Expression expr) {

        if (expr instanceof AndExpression) {

            AndExpression and = (AndExpression) expr;

            Predicate left = parseWhere(and.getLeftExpression());
            Predicate right = parseWhere(and.getRightExpression());

            left.conjoinWith(right);
            return left;
        }

        if (expr instanceof EqualsTo) {

            EqualsTo eq = (EqualsTo) expr;

            rs.raf.simpledb.query.Expression lhs =
                    toSimpleDBExpression(eq.getLeftExpression());

            rs.raf.simpledb.query.Expression rhs =
                    toSimpleDBExpression(eq.getRightExpression());

            return new Predicate(new Term(lhs, rhs));
        }

        throw new RuntimeException("Unsupported WHERE: " + expr);
    }

    /* ======================= EXPRESSIONS ======================= */

    private rs.raf.simpledb.query.Expression
    toSimpleDBExpression(Expression e) {

        if (e instanceof Column) {
            return new FieldNameExpression(
                    ((Column) e).getColumnName());
        }

        if (e instanceof LongValue) {
            return new ConstantExpression(
                    new IntConstant(
                            ((LongValue) e).getBigIntegerValue().intValue()
                    ));
        }

        if (e instanceof StringValue) {
            return new ConstantExpression(
                    new StringConstant(
                            ((StringValue) e).getValue()
                    ));
        }

        throw new RuntimeException("Unsupported expression: " + e);
    }

    /* ======================= UPDATE CMD ======================= */

    public Object updateCmd(String sql)
            throws JSQLParserException {

        Statement stmt =
                CCJSqlParserUtil.parse(sql);

        if (stmt instanceof Insert)
            return parseInsert((Insert) stmt);

        if (stmt instanceof Delete)
            return parseDelete((Delete) stmt);

        if (stmt instanceof Update)
            return parseUpdate((Update) stmt);

        if (stmt instanceof CreateTable)
            return parseCreateTable((CreateTable) stmt);

        if (stmt instanceof CreateView)
            return parseCreateView((CreateView) stmt);

        if (stmt instanceof CreateIndex)
            return parseCreateIndex((CreateIndex) stmt);

        throw new RuntimeException(
                "Unsupported command: " + stmt);
    }

    /* ======================= INSERT ======================= */

    private InsertData parseInsert(Insert ins) {

        String tbl = ins.getTable().getName();

        List<String> fields = new ArrayList<>();
        if (ins.getColumns() != null) {
            for (Column c : ins.getColumns()) {
                fields.add(c.getColumnName());
            }
        }

        List<Constant> values = new ArrayList<>();
        if (ins.getValues() != null) {
            for (Expression e :
                    ins.getValues().getExpressions()) {

                values.add(toConstant(e));
            }
        }

        return new InsertData(tbl, fields, values);
    }

    /* ======================= DELETE ======================= */

    private DeleteData parseDelete(Delete del) {

        String tbl = del.getTable().getName();

        Predicate pred = new Predicate();
        if (del.getWhere() != null) {
            pred = parseWhere(del.getWhere());
        }

        return new DeleteData(tbl, pred);
    }

    /* ======================= UPDATE ======================= */

    private ModifyData parseUpdate(Update upd) {

        String tbl = upd.getTable().getName();

        String fld =
                upd.getColumns().get(0).getColumnName();

        rs.raf.simpledb.query.Expression newval =
                toSimpleDBExpression(
                        upd.getExpressions().get(0));

        Predicate pred = new Predicate();
        if (upd.getWhere() != null) {
            pred = parseWhere(upd.getWhere());
        }

        return new ModifyData(tbl, fld, newval, pred);
    }

    /* ======================= CREATE TABLE ======================= */

    private CreateTableData
    parseCreateTable(CreateTable ct) {

        String tbl = ct.getTable().getName();
        Schema sch = new Schema();

        ct.getColumnDefinitions().forEach(cd -> {

            String name = cd.getColumnName();
            String type =
                    cd.getColDataType()
                            .getDataType()
                            .toLowerCase();

            if (type.equals("int")
                    || type.equals("integer")) {

                sch.addIntField(name);
            }

            else if (type.equals("varchar")
                    || type.equals("char")) {

                int len = 20;

                if (cd.getColDataType()
                        .getArgumentsStringList()
                        != null) {

                    len = Integer.parseInt(
                            cd.getColDataType()
                                    .getArgumentsStringList()
                                    .get(0));
                }

                sch.addStringField(name, len);
            }
        });

        return new CreateTableData(tbl, sch);
    }

    /* ======================= CREATE VIEW ======================= */

    private CreateViewData
    parseCreateView(CreateView cv) {

        String name = cv.getView().getName();
        String q = cv.getSelect().toString();

        try {
            return new CreateViewData(
                    name, query(q));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /* ======================= CREATE INDEX ======================= */

    private CreateIndexData
    parseCreateIndex(CreateIndex ci) {

        String idx = ci.getIndex().getName();
        String tbl = ci.getTable().getName();

        String fld =
                ci.getIndex()
                        .getColumnsNames()
                        .get(0);

        return new CreateIndexData(idx, tbl, fld);
    }

    /* ======================= CONSTANT ======================= */

    private Constant toConstant(Expression e) {

        if (e instanceof LongValue)
            return new IntConstant(
                    ((LongValue) e)
                            .getBigIntegerValue()
                            .intValue());

        if (e instanceof StringValue)
            return new StringConstant(
                    ((StringValue) e).getValue());

        throw new RuntimeException(
                "Unsupported constant: " + e);
    }
}
