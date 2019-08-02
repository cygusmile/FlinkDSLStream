package com.ximalaya.flink.dsl.stream.calcite.domain.node;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * 实现维流表连接语义
 *
 * 实现维表和流表的内连接
 * select side stream a.* from streamTable a join dimensionTable b on a.id = b.id where a.name <> 'hello' as joinTable;
 *
 * 实现维表和流表的左外连接
 * select side stream a.* from streamTable a left join dimensionTable b on a.id = b.id where a.name <> 'hello' as joinTable;
 *
 * 实现多表关联
 * select side stream a.* from streamTable a join dimensionTableOne b join dimensionTableTwo c  on a.id = b.id and a.id = c.id as joinTable;
 */

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/11
 **/

public class DslStreamSideJoinSqlSelect extends SqlCall {

    private SqlNodeList selectList;
    private SqlNode from;
    private SqlNode where;
    private SqlIdentifier alias;

    public DslStreamSideJoinSqlSelect(SqlParserPos pos,
                                     SqlNodeList selectList,
                                     SqlNode from,
                                     SqlNode where,
                                      SqlIdentifier alias) {
        super(pos);
        this.selectList = selectList;
        this.from = from;
        this.where = where;
        this.alias = alias;
    }

    public SqlNodeList getSelectList() {
        return selectList;
    }

    public SqlNode getFrom() {
        return from;
    }

    public SqlNode getWhere() {
        return where;
    }

    public SqlIdentifier getAlias() {
        return alias;
    }

    @Override
    public SqlOperator getOperator() {
        return null;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.SELECT;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SELECT");
        selectList.unparse(writer,leftPrec,rightPrec);
        writer.keyword("FROM");
        from.unparse(writer,leftPrec,rightPrec);
        if(where!=null) {
            where.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("AS");
        alias.unparse(writer,leftPrec,rightPrec);
    }
}
