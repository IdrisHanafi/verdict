package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.models.StratifiedSample;
import edu.umich.verdict.parser.TsqlBaseVisitor;
import edu.umich.verdict.parser.TsqlParser;
import edu.umich.verdict.processing.SelectStatement;
import org.antlr.v4.runtime.TokenStreamRewriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class QueryTransformer {
    protected static final List<String> supportedAggregates = Arrays.asList("avg sum count".split(" "));
    protected final MetaDataManager metaDataManager;
    protected final TokenStreamRewriter rewriter;
    protected SelectStatement q;
    protected TransformedQuery transformed;
    protected final int bootstrapTrials;
    protected final double confidence;
    protected final String sampleType;
    private final double preferredSample;
    //TODO: do we need this?
    private final boolean useConfIntUdf = true;

    protected TsqlParser.Select_listContext selectList = null;
    ArrayList<SelectListItem> selectItems = new ArrayList<>();

    public static QueryTransformer forConfig(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        if (!conf.getBoolean("bootstrap"))
            return new IdenticalTransformer(conf, metaDataManager, q);
        switch (conf.get("bootstrap.method")) {
            case "uda":
                return new UdaTransformer(conf, metaDataManager, q);
            case "on the fly":
                return new OnTheFlyTransformer(conf, metaDataManager, q);
            case "stored":
                return new StoredTransformer(conf, metaDataManager, q);
            default:
                return new IdenticalTransformer(conf, metaDataManager, q);
        }
    }

    public QueryTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        this.q = q;
        this.metaDataManager = metaDataManager;
        rewriter = q.getRewriter();
        confidence = conf.getPercent("bootstrap.confidence");
        bootstrapTrials = conf.getInt("bootstrap.trials");
        preferredSample = conf.getPercent("bootstrap.sample_size");
        sampleType = conf.get("bootstrap.sample_type").toLowerCase();
        transformed = new TransformedQuery(q, bootstrapTrials, confidence, conf.get("bootstrap.method").toLowerCase());
    }

    protected boolean replaceTableNames() {
        q.getParseTree().accept(new TsqlBaseVisitor<Void>() {
            public Void visitTable_source_item(TsqlParser.Table_source_itemContext ctx) {
                if (transformed.getSample() != null)
                    // already replaced a sample
                    return null;
                if (ctx.table_name_with_hint() == null)
                    // table_source_item is a sub-query or something else (not a table reference)
                    return null;
                TsqlParser.Table_nameContext nameCtx = ctx.table_name_with_hint().table_name();
                Sample sample = getSample(nameCtx.getText());
                if (sample != null) {
                    if (ctx.as_table_alias() == null)
                        // if there is no alias, we add an alias equal to the original table name to eliminate side-effects of this change in other parts of the query
                        rewriter.replace(nameCtx.start, nameCtx.stop, sample.getName() + " AS " + nameCtx.table.getText());
                    else
                        rewriter.replace(nameCtx.start, nameCtx.stop, sample.getName());
                    transformed.setSample(sample);
                }
                return null;
            }
        });
        return transformed.getSample() != null;
    }

    protected abstract boolean addBootstrapTrials();

    public TransformedQuery transform() {
        //TODO: make methods throw appropriate exceptions instead of returning false
        boolean changed = replaceTableNames() && findSelectList() && findAggregates() && scaleAggregates() && addBootstrapTrials();// && addSelectWrapper();
        return transformed;
    }

    protected boolean findSelectList() {
        q.getParseTree().accept(new TsqlBaseVisitor<Void>() {
            public Void visitSelect_list(TsqlParser.Select_listContext list) {
                if (selectList != null)
                    // already found
                    return null;
                selectList = list;
                return null;
            }
        });
        if (selectList == null)
            return false;
        transformed.setOriginalCols(selectList.select_list_elem().size());
        return true;
    }

    protected boolean findAggregates() {
        if (selectList == null)
            return false;
        for (TsqlParser.Select_list_elemContext item : selectList.select_list_elem()) {
            SelectListItem itemInfo;
            try {
                itemInfo = new SelectListItem(selectItems.size() + 1, item);
            } catch (Exception e) {
                transformed.getAggregates().clear();
                break;
            }
            selectItems.add(itemInfo);
            if (itemInfo.isSupportedAggregate())
                transformed.addAggregate(itemInfo.getAggregateType(), itemInfo.getExpression(), itemInfo.getIndex());
        }
        return transformed.isChanged();
    }

    protected boolean scaleAggregates() {
        for (SelectListItem item : selectItems)
            if (item.isSupportedAggregate())
                item.scale(rewriter);
        return true;
    }

    protected Sample getSample(String tableName) {
        double min = 1000;
        Sample best = null;
        for (Sample s : metaDataManager.getTableSamples(tableName)) {
            if ((s instanceof StratifiedSample && sampleType.equals("uniform")) || (!(s instanceof StratifiedSample) && sampleType.equals("stratified")))
                continue;
            double diff = Math.abs(s.getCompRatio() - preferredSample);
            if ((min > preferredSample * .2 && diff < min) || (diff <= preferredSample * .2 && best != null &&
                    getPreferred(s, best) == s)) {
                min = diff;
                best = s;
            }
        }
        return best;
    }

    protected Sample getPreferred(Sample first, Sample second) {
        return second.getPoissonColumns() > first.getPoissonColumns() ? first : second;
    }

    protected class SelectListItem {
        private int index;
        private String expr;
        private String aggr;
        private TransformedQuery.AggregateType aggregateType = TransformedQuery.AggregateType.NONE;
        private String alias = "";
        private boolean isSupportedAggregate = false;
        private TsqlParser.Select_list_elemContext ctx;

        public SelectListItem(int index, TsqlParser.Select_list_elemContext ctx) throws Exception {
            this.ctx = ctx;
            if (ctx.expression() == null)
                // probably item is *
                //TODO: better exception
                throw new Exception("In appropriate expression.");
            this.index = index;
            this.expr = ctx.expression().getText();
            if (ctx.column_alias() != null)
                alias = ctx.column_alias().getText();
            TsqlParser.ExpressionContext exprCtx = ctx.expression();
            if (exprCtx instanceof TsqlParser.Function_call_expressionContext) {
                // it's a function call
                TsqlParser.Aggregate_windowed_functionContext aggCtx = ((TsqlParser.Function_call_expressionContext) exprCtx).function_call().aggregate_windowed_function();
                if (aggCtx != null) {
                    // its aggregate function
                    if (aggCtx.over_clause() != null)
                        return;
                    if (aggCtx.all_distinct_expression() == null) {
                        // count(*)
                        expr = "1";
                    } else {
                        if (aggCtx.all_distinct_expression().DISTINCT() != null)
                            return;
                        expr = aggCtx.all_distinct_expression().expression().getText();
                    }
                    aggr = aggCtx.getChild(0).getText();
                    if (supportedAggregates.contains(aggr.toLowerCase())) {
                        isSupportedAggregate = true;
                        aggregateType = TransformedQuery.AggregateType.valueOf(aggr.toUpperCase());
                    } else
                        aggregateType = TransformedQuery.AggregateType.OTHER;
                }
            }
        }

        public void scale(TokenStreamRewriter rewriter) {
            double scale = getScale();
            if (scale == 1)
                return;
            if (getAlias().isEmpty()) {
                String expr = ctx.getText();
                rewriter.replace(ctx.start, ctx.stop, scale + "*" + expr + " AS " + metaDataManager.getAliasCharacter() + expr + metaDataManager.getAliasCharacter());
            } else
                rewriter.insertBefore(ctx.expression().start, scale + "*");

        }

        protected double getScale() {
            if (transformed.getSample() instanceof StratifiedSample)
                return 1;
            switch (getAggregateType()) {
                case SUM:
                case COUNT:
                    return 1 / transformed.getSample().getCompRatio();
                default:
                    return 1;
            }
        }

        public String getAlias() {
            return this.alias;
        }

        public int getIndex() {
            return index;
        }

        public String getExpression() {
            return expr;
        }

        public TransformedQuery.AggregateType getAggregateType() {
            return aggregateType;
        }

        public boolean isSupportedAggregate() {
            return isSupportedAggregate;
        }
    }
}