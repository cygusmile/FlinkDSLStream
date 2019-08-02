package com.ximalaya.flink.dsl.stream.calcite.domain.node;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * 实现 hive explode语义
 *
 * select explode(myCol) [as myNewCol] from myTable [where condition] as newMyTable;
 *
 * myTable:
 *
 * | myCol   |
 * | ------  |
 * | [1,2,3] |
 *
 * newMyTable:
 *
 * | myNewCol |
 * | -------- |
 * |    1     |
 * |    2     |
 * |    3     |
 *
 * 注: myCol也可以是一个返回Array的函数调用
 */

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/29
 **/
public class DslStreamSimpleExplodeSqlSelect extends SqlCall {

    private SqlNode explodeField;
    private SqlIdentifier fieldAlias;
    private SqlIdentifier from;
    private SqlNode where;
    private SqlIdentifier alias;


    public SqlNode getExplodeField() {
        return explodeField;
    }

    public SqlIdentifier getFieldAlias() {
        return fieldAlias;
    }

    public SqlIdentifier getFrom() {
        return from;
    }

    public SqlNode getWhere() {
        return where;
    }

    public SqlIdentifier getAlias() {
        return alias;
    }

    public DslStreamSimpleExplodeSqlSelect(SqlParserPos pos,
                                           SqlNode explodeField,
                                           SqlIdentifier fieldAlias,
                                           SqlIdentifier from,
                                           SqlNode where,
                                           SqlIdentifier alias) {
        super(pos);
        this.explodeField = explodeField;
        this.fieldAlias = fieldAlias;
        this.from = from;
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
        writer.keyword("EXPLODE");
        writer.keyword("(");
        explodeField.unparse(writer,leftPrec,rightPrec);
        writer.keyword(")");
        if(fieldAlias!=null){
            fieldAlias.unparse(writer,leftPrec,rightPrec);
        }
        writer.keyword("FROM");
        from.unparse(writer,leftPrec,rightPrec);
        if(where!=null) {
            where.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("AS");
        alias.unparse(writer,leftPrec,rightPrec);
    }
}
