package com.ximalaya.flink.dsl.stream.calcite.domain.node;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * 实现hive lateral view explode语义
 *
 * select myCol1,explodeMyCol2 from myTable lateral view explode(myCol2) explodeTable as explodeMyCol2 [where = condition] as myNewTable;
 *
 * myTable:
 *
 * | myCol1   | myCol2 |
 * | ------  | ------ |
 * |     a | [1,2,3   ] |
 *
 * myNewTable:
 *
 * | myCol1 | explodeMyCol2 |
 * | -------- | ------- |
 * |    a     |   1    |
 * |    a     |   2    |
 * |    a     |    3   |
 *
 * 注: myCol也可以是一个返回Array的函数调用
 */

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/22
 **/
public class DslStreamExplodeSqlSelect extends SqlCall {

    private SqlNodeList selectList;
    private SqlIdentifier from;
    private SqlNode explodeField;
    private SqlIdentifier explodeTable;
    private SqlIdentifier explodeAlias;
    private SqlNode where;
    private SqlIdentifier alias;


    public SqlNodeList getSelectList() {
        return selectList;
    }

    public SqlIdentifier getFrom() {
        return from;
    }

    public SqlNode getExplodeField() {
        return explodeField;
    }

    public SqlIdentifier getExplodeTable() {
        return explodeTable;
    }

    public SqlIdentifier getExplodeAlias() {
        return explodeAlias;
    }

    public SqlNode getWhere() {
        return where;
    }

    public SqlIdentifier getAlias() {
        return alias;
    }

    public DslStreamExplodeSqlSelect(SqlParserPos pos,
                                     SqlNodeList selectList,
                                     SqlIdentifier from,
                                     SqlNode explodeField,
                                     SqlIdentifier explodeTable,
                                     SqlIdentifier explodeAlias,
                                     SqlNode where, SqlIdentifier alias) {
        super(pos);
        this.selectList = selectList;
        this.from = from;
        this.explodeField = explodeField;
        this.explodeTable = explodeTable;
        this.explodeAlias = explodeAlias;
        this.where = where;
        this.alias = alias;
    }

    @Override
    public SqlOperator getOperator() {
        return null;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.SELECT;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SELECT");
        selectList.unparse(writer,leftPrec,rightPrec);
        writer.keyword("FROM");
        from.unparse(writer,leftPrec,rightPrec);
        writer.keyword("LATERAL");
        writer.keyword("VIEW");
        writer.keyword("EXPLODE");
        writer.keyword("(");
        explodeField.unparse(writer,leftPrec,rightPrec);
        writer.keyword(")");
        explodeTable.unparse(writer,leftPrec,rightPrec);
        writer.keyword("AS");
        explodeAlias.unparse(writer,leftPrec,rightPrec);
        if(where!=null) {
            where.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("AS");
        alias.unparse(writer,leftPrec,rightPrec);
    }
}
