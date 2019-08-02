SqlNode DslStreamSideJoinSqlSelect():
{
    final Span s = Span.of();
    final List<SqlNode> selectList;
    final SqlNode fromClause;
    final SqlNode where;
    final SqlIdentifier alias;
    }
    {
    <SELECT> <SIDE> <STREAM> selectList = SelectList()
     <FROM>  fromClause = FromClause()
     where = WhereOpt()
     <AS>
     alias = SimpleIdentifier()
     {
       return new DslStreamSideJoinSqlSelect(s.end(this),new SqlNodeList(selectList,Span.of(selectList).pos()),fromClause,where,alias);
     }
}
SqlNode DslStreamExplodeSqlSelect():
{
    final List<SqlNode> selectList;
    final SqlIdentifier from;
    final Span s = Span.of();
    final SqlNode explodeField;
    final SqlIdentifier explodeTable;
    final SqlIdentifier explodeAlias;
    final SqlNode where;
    final SqlIdentifier alias;
}
{
    <SELECT> selectList = SelectList()
    <FROM> from = SimpleIdentifier()
    <LATERAL> <VIEW> <EXPLODE> <LPAREN>  explodeField = SelectExpression() <RPAREN>
        explodeTable = SimpleIdentifier()
        <AS>
        explodeAlias = SimpleIdentifier()
        where = WhereOpt()
        <AS>
        alias = SimpleIdentifier()
    {
        return new DslStreamExplodeSqlSelect(s.end(this),new SqlNodeList(selectList,
            Span.of(selectList).pos()),from,explodeField,explodeTable,explodeAlias,where,alias);
    }
}
SqlNode DslStreamSimpleExplodeSqlSelect():
{
     final Span s = Span.of();
     final SqlNode explodeField;
     final SqlIdentifier aliasField;
     final SqlIdentifier from;
     final SqlNode where;
     final SqlIdentifier alias;
}
{
       <SELECT> <EXPLODE> <LPAREN>  explodeField = SelectExpression() <RPAREN>
        aliasField = asOpt()
       <FROM> from = SimpleIdentifier()
         where = WhereOpt()
        <AS>
         alias = SimpleIdentifier()
         {
            return new DslStreamSimpleExplodeSqlSelect(s.end(this),explodeField ,aliasField,from,where,alias);
          }
}
SqlIdentifier asOpt():
{
      SqlIdentifier as;
}
{
     <AS> as = SimpleIdentifier()
    {
      return as;
    }
     | {
            return null;
      }
}