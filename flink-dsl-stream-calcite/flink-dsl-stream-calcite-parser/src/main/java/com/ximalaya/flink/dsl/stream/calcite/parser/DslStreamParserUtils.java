package com.ximalaya.flink.dsl.stream.calcite.parser;

import com.ximalaya.flink.dsl.stream.calcite.parser.impl.DslStreamSqlParser;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/28
 **/

public class DslStreamParserUtils {

    public static SqlNode parse(String sql) throws Exception {
            return SqlParser.create(sql,
                    SqlParser.configBuilder()
                            .setParserFactory(DslStreamSqlParser.FACTORY)
                            .setQuoting(Quoting.BACK_TICK)
                            .setUnquotedCasing(Casing.UNCHANGED)
                            .setQuotedCasing(Casing.UNCHANGED)
                            .setConformance(SqlConformanceEnum.DEFAULT)
                            .build())
                    .parseStmt();
    }
}
