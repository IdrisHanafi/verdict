package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.VerdictSQLParser.Group_by_itemContext;
import edu.umich.verdict.VerdictSQLParser.Join_partContext;
import edu.umich.verdict.VerdictSQLParser.Select_list_elemContext;
import edu.umich.verdict.VerdictSQLParser.Table_sourceContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.ResultSetConversion;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

import com.google.common.base.Optional;

/**
 * Base class for exact relations (and any relational operations on them).
 * @author Yongjoo Park
 *
 */
public abstract class ExactRelation extends Relation {

	public ExactRelation(VerdictContext vc) {
		super(vc);
	}
	
	public static ExactRelation from(VerdictContext vc, String sql) {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(sql));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		RelationGen g = new RelationGen(vc, sql);
		return g.visit(p.select_statement());
	}
	
	
//	/**
//	 * Returns an expression for a (possibly joined) table source.
//	 * SingleSourceRelation: a table name
//	 * JoinedRelation: a join expression
//	 * FilteredRelation: a full toSql()
//	 * ProjectedRelation: a full toSql()
//	 * AggregatedRelation: a full toSql()
//	 * GroupedRelation: a full toSql()
//	 * @return
//	 */
//	protected abstract String getSourceExpr();
	
	/**
	 * Returns a name for a (possibly joined) table source. It will be an alias name if the source is a derived table.
	 * @return
	 */
	protected abstract String getSourceName();
	
	/*
	 * Filtering
	 */
	
	/**
	 * Returns a relation with an extra filtering condition.
	 * The immediately following filter (or where) function on the joined relation will work as a join condition.
	 * @param cond
	 * @return
	 * @throws VerdictException
	 */
	public ExactRelation filter(Cond cond) throws VerdictException {
		return new FilteredRelation(vc, this, cond);
	}
	
	public ExactRelation filter(String cond) throws VerdictException {
		return filter(Cond.from(cond));
	}
	
	public ExactRelation where(String cond) throws VerdictException {
		return filter(cond);
	}
	
	public ExactRelation where(Cond cond) throws VerdictException {
		return filter(cond);
	}
	
	/*
	 * Aggregation
	 */
	
	public AggregatedRelation agg(Object... elems) {
		return agg(Arrays.asList(elems));
	}
	
	public AggregatedRelation agg(List<Object> elems) {
		List<SelectElem> se = new ArrayList<SelectElem>();
		for (Object e : elems) {
			se.add(SelectElem.from(e.toString()));
		}
		return new AggregatedRelation(vc, this, se);
	}
	
	@Override
	public AggregatedRelation count() throws VerdictException {
		return agg(FuncExpr.count());
	}

	@Override
	public AggregatedRelation sum(String expr) throws VerdictException {
		return agg(FuncExpr.sum(Expr.from(expr)));
	}

	@Override
	public AggregatedRelation avg(String expr) throws VerdictException {
		return agg(FuncExpr.avg(Expr.from(expr)));
	}

	@Override
	public AggregatedRelation countDistinct(String expr) throws VerdictException {
		return agg(FuncExpr.countDistinct(Expr.from(expr)));
	}
	
	public GroupedRelation groupby(String group) {
		String[] tokens = group.split(",");
		List<ColNameExpr> groups = new ArrayList<ColNameExpr>();
		for (String t : tokens) {
			groups.add(ColNameExpr.from(t));
		}
		return new GroupedRelation(vc, this, groups);
	}
	
	/*
	 * Approx Aggregation
	 */

	public ApproxRelation approxAgg(List<Object> elems) throws VerdictException {
		return agg(elems).approx();
	}
	
	public ApproxRelation approxAgg(Object... elems) throws VerdictException {
		return agg(elems).approx();
	}

	public long approxCount() throws VerdictException {
		return TypeCasting.toLong(approxAgg(FuncExpr.count()).collect().get(0).get(0));
	}

	public double approxSum(Expr expr) throws VerdictException {
		return TypeCasting.toDouble(approxAgg(FuncExpr.sum(expr)).collect().get(0).get(0));
	}

	public double approxAvg(Expr expr) throws VerdictException {
		return TypeCasting.toDouble(approxAgg(FuncExpr.avg(expr)).collect().get(0).get(0));
	}
	
	public long approxCountDistinct(Expr expr) throws VerdictException {
		return TypeCasting.toLong(approxAgg(FuncExpr.countDistinct(expr)).collect().get(0).get(0));
	}
	
	public long approxCountDistinct(String expr) throws VerdictException {
		return TypeCasting.toLong(approxAgg(FuncExpr.countDistinct(Expr.from(expr))).collect().get(0).get(0));
	}
	
	/*
	 * order by and limit
	 */
	
	public ExactRelation orderby(String orderby) {
		String[] tokens = orderby.split(",");
		List<ColNameExpr> cols = new ArrayList<ColNameExpr>();
		for (String t : tokens) {
			cols.add(ColNameExpr.from(t));
		}
		return new GroupedRelation(vc, this, cols);
	}
	
	public ExactRelation limit(long limit) {
		return new LimitedRelation(vc, this, limit);
	}
	
	public ExactRelation limit(String limit) {
		return limit(Integer.valueOf(limit));
	}
	
	/*
	 * Joins
	 */
	
	public JoinedRelation join(SingleRelation r, List<Pair<Expr, Expr>> joinColumns) {
		return JoinedRelation.from(vc, this, r, joinColumns);
	}
	
	public JoinedRelation join(SingleRelation r, Cond cond) throws VerdictException {
		return JoinedRelation.from(vc, this, r, cond);
	}
	
	public JoinedRelation join(SingleRelation r, String cond) throws VerdictException {
		return join(r, Cond.from(cond));
	}
	
	public JoinedRelation join(SingleRelation r) throws VerdictException {
		return join(r, (Cond) null);
	}	
	
	/*
	 * Transformation to ApproxRelation
	 */

	public abstract ApproxRelation approx() throws VerdictException;
	
	protected abstract ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace);

	/**
	 * Finds sets of samples that could be used for the table sources in a transformed approximate relation.
	 * Called on ProjectedRelation or AggregatedRelation, returns an empty set.
	 * Called on FilteredRelation, returns the result of its source.
	 * Called on JoinedRelation, combine the results of its two sources.
	 * Called on SingleRelation, finds a proper list of samples.
	 * Note that the return value's key (i.e., Set<ApproxSingleRelation>) holds a set of samples that point to all
	 * different relations. In other words, if this sql includes two tables, then the size of the set will be two, and
	 * the elements of the set will be the sample tables for those two tables. Multiple of such sets serve as condidates.
	 * @param functions
	 * @return A map from a candidate to score.
	 */
	protected Map<Set<SampleParam>, Double> findSample(List<Expr> functions) {
		return new HashMap<Set<SampleParam>, Double>();
	}
	
	protected Map<TableUniqueName, SampleParam> chooseBest(Map<Set<SampleParam>, Double> candidates) {
		Set<SampleParam> best = null;
		double max_score = Double.NEGATIVE_INFINITY;
		
		for (Map.Entry<Set<SampleParam>, Double> e : candidates.entrySet()) {
//			VerdictLogger.debug(this, String.format("candidate sample: %s, score: %f", e.getKey(), e.getValue()));
			if (e.getValue() > max_score) {
				best = e.getKey();
				max_score = e.getValue();
			}
		}
		
		Map<TableUniqueName, SampleParam> map = new HashMap<TableUniqueName, SampleParam>();
		if (best != null) {
			for (SampleParam s : best) {
				map.put(s.originalTable, s);
			}
		}
		
		return map;
	}
	
	/*
	 * Helpers
	 */

	/**
	 * 
	 * @param relation Starts to collect from this relation
	 * @return All found groupby expressions and the first relation that is not a GroupedRelation.
	 */
	protected Pair<List<Expr>, ExactRelation> allPrecedingGroupbys(ExactRelation r) {
		List<Expr> groupbys = new ArrayList<Expr>();
		ExactRelation t = r;
		while (true) {
			if (t instanceof GroupedRelation) {
				groupbys.addAll(((GroupedRelation) t).groupby);
				t = ((GroupedRelation) t).getSource();
			} else {
				break;
			}
		}
		return Pair.of(groupbys, t);
	}

	protected Pair<Optional<Cond>, ExactRelation> allPrecedingFilters(ExactRelation r) {
		Optional<Cond> c = Optional.absent();
		ExactRelation t = r;
		while (true) {
			if (t instanceof FilteredRelation) {
				if (c.isPresent()) {
					c = Optional.of((Cond) AndCond.from(c.get(), ((FilteredRelation) t).getFilter()));
				} else {
					c = Optional.of(((FilteredRelation) t).getFilter());
				}
				t = ((FilteredRelation) t).getSource();
			} else {
				break;
			}
		}
		return Pair.of(c, t);
	}

	protected String sourceExpr(ExactRelation source) {
		if (source instanceof SingleRelation) {
			return ((SingleRelation) source).getTableName().toString();
		} else if (source instanceof JoinedRelation) {
			return ((JoinedRelation) source).joinClause();
		} else {
			return String.format("(%s) AS %s", source.toSql(), source.getAliasName());
		}
	}
	
}


class RelationGen extends VerdictSQLBaseVisitor<ExactRelation> {
	
	private VerdictContext vc;
	
	private String sql;
	
	public RelationGen(VerdictContext vc, String sql) {
		this.vc = vc;
		this.sql = sql;
	}
	
	@Override
	public ExactRelation visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
		ExactRelation r = visit(ctx.query_expression());
		
		if (ctx.order_by_clause() != null) {
			r = r.orderby(getOriginalText(ctx.order_by_clause()));
		}
		
		if (ctx.limit_clause() != null) {
			r = r.limit(ctx.limit_clause().getText());
		}
		
		return r;
	}
	
	@Override
	public ExactRelation visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
		// parse the where clause
		Cond where = null;
		if (ctx.WHERE() != null) {
			where = Cond.from(getOriginalText(ctx.where));
		}
		
		// parse the from clause
		// if a subquery is found; another instance of this class will be created and be used.
		// we convert all the table sources into ExactRelation instances. Those ExactRelation instances will be joined
		// either using the join condition explicitly stated using the INNER JOIN ON statements or using the conditions
		// in the where clause.
		ExactRelation r = null;
		List<String> joinedTableName = new ArrayList<String>();
		
		for (Table_sourceContext s : ctx.table_source()) {
			TableSourceExtractor e = new TableSourceExtractor();
			ExactRelation r1 = e.visit(s);
			if (r == null) r = r1;
			else {
				JoinedRelation r2 = new JoinedRelation(vc, r, r1, null);
				Cond j = null;
			
				// search for join conditions
				if (r1 instanceof SingleRelation && where != null) {
					String n = ((SingleRelation) r1).getTableName().tableName;
					j = where.searchForJoinCondition(joinedTableName, n);
					if (j == null && r1.getAliasName() != null) {
						j = where.searchForJoinCondition(joinedTableName, r1.getAliasName());
					}
				} else if (r2.getAliasName() != null && where != null) {
					j = where.searchForJoinCondition(joinedTableName, r2.getAliasName());
				}
				
				if (j != null) {
					try {
						r2.setJoinCond(j);
					} catch (VerdictException e1) {
						VerdictLogger.error(StackTraceReader.stackTrace2String(e1));
					}
					where = where.remove(j);
				}
				
				r = r2;
			}
			
			// add both table names and the alias to joined table names
			if (r1 instanceof SingleRelation) {
				joinedTableName.add(((SingleRelation) r1).getTableName().tableName);
			}
			if (r1.getAliasName() != null) {
				joinedTableName.add(r1.getAliasName());
			}
		}
		
		if (where != null) {
			r = new FilteredRelation(vc, r, where);
		}
		
		if (ctx.GROUP() != null) {
			List<ColNameExpr> groupby = new ArrayList<ColNameExpr>();
			for (Group_by_itemContext g : ctx.group_by_item()) {
				groupby.add(ColNameExpr.from(g.getText()));
			}
			r = new GroupedRelation(vc, r, groupby);
		}
		
		SelectListExtractor select = new SelectListExtractor();
		Triple<List<SelectElem>, List<SelectElem>, List<SelectElem>> elems = select.visit(ctx.select_list());
		if (elems.getMiddle().size() > 0) {
			r = new AggregatedRelation(vc, r, elems.getMiddle());
			r.setAliasName(Relation.genAlias());
			r = new ProjectedRelation(vc, r, elems.getRight());
		} else {
			r = new ProjectedRelation(vc, r, elems.getRight());
		}
		
		return r;
	}
	
	// Returs a triple of
	// 1. non-aggregate select list elements
	// 2. aggregate select list elements.
	// 3. both of them in order.
	class SelectListExtractor extends VerdictSQLBaseVisitor<Triple<List<SelectElem>, List<SelectElem>, List<SelectElem>>> {
		@Override public Triple<List<SelectElem>, List<SelectElem>, List<SelectElem>> visitSelect_list(VerdictSQLParser.Select_listContext ctx) {
			List<SelectElem> nonagg = new ArrayList<SelectElem>();
			List<SelectElem> agg = new ArrayList<SelectElem>();
			List<SelectElem> both = new ArrayList<SelectElem>();
			for (Select_list_elemContext a : ctx.select_list_elem()) {
				SelectElem e = SelectElem.from(getOriginalText(a));
				if (e.isagg()) {
					agg.add(e);
				} else {
					nonagg.add(e);
				}
				both.add(e);
			}
			return Triple.of(nonagg, agg, both);
		}
	}
	
	// The tableSource returned from this class is supported to include all necessary join conditions; thus, we do not
	// need to search for their join conditions in the where clause.
	class TableSourceExtractor extends VerdictSQLBaseVisitor<ExactRelation> {
		public List<ExactRelation> relations = new ArrayList<ExactRelation>();
		
		private Cond joinCond = null;
		
		@Override
		public ExactRelation visitTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx) {
			ExactRelation r = visit(ctx.table_source_item());
			for (Join_partContext j : ctx.join_part()) {
				ExactRelation r2 = visit(j);
				r = new JoinedRelation(vc, r, r2, null);
				if (joinCond != null) {
					try {
						((JoinedRelation) r).setJoinCond(joinCond);
					} catch (VerdictException e) {
						VerdictLogger.error(StackTraceReader.stackTrace2String(e));
					}
					joinCond = null;
				}
			}
			return r;
		}
		
		@Override
		public ExactRelation visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
			String tableName = ctx.table_name_with_hint().table_name().getText();
			ExactRelation r = SingleRelation.from(vc, tableName);
			if (ctx.as_table_alias() != null) {
				r.setAliasName(ctx.as_table_alias().table_alias().getText());
			}
			return r;
		}
		
		@Override
		public ExactRelation visitJoin_part(VerdictSQLParser.Join_partContext ctx) {
			if (ctx.INNER() != null) {
				TableSourceExtractor ext = new TableSourceExtractor();
				ExactRelation r = ext.visit(ctx.table_source());
				joinCond = Cond.from(getOriginalText(ctx.search_condition()));
				return r;
			} else {
				VerdictLogger.error(this, "Unsupported join condition: " + getOriginalText(ctx));
				return null;
			}
		}
	}
	
	protected String getOriginalText(ParserRuleContext ctx) {
		int a = ctx.start.getStartIndex();
	    int b = ctx.stop.getStopIndex();
	    Interval interval = new Interval(a,b);
	    return CharStreams.fromString(sql).getText(interval);
	}
}

