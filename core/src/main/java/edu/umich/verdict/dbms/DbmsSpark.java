package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsSpark extends Dbms {

    private static String DBNAME = "spark";

    protected SparkSession spark;

    protected Dataset<Row> df;

    protected Set<TableUniqueName> cachedTable;

    public DbmsSpark(VerdictContext vc, SparkSession spark) throws VerdictException {	
        super(vc, DBNAME);

        this.spark = spark;
        this.cachedTable = new HashSet<TableUniqueName>();
    }

    public Dataset<Row> getDatabaseNamesInDataFrame() throws VerdictException {
        Dataset<Row> df = executeSparkQuery("show databases");
        return df;
    }

    public Dataset<Row> getTablesInDataFrame(String schemaName) throws VerdictException {
        Dataset<Row> df = executeSparkQuery("show tables in " + schemaName);
        return df;
    }

    public Dataset<Row> describeTableInDataFrame(TableUniqueName tableUniqueName)  throws VerdictException {
        Dataset<Row> df = executeSparkQuery(String.format("describe %s", tableUniqueName));
        return df;
    }

    @Override
    public boolean execute(String sql) throws VerdictException {
        df = spark.sql(sql);
        return (df != null)? true : false;
        //return (df.count() > 0)? true : false;
    }

    @Override
    public void executeUpdate(String sql) throws VerdictException {
        execute(sql);
    }

    @Override
    public ResultSet getResultSet() {
        return null;
    }

    @Override
    public Dataset<Row> getDataFrame() {
        return df;
    }

    public Dataset<Row> emptyDataFrame() {
        return spark.emptyDataFrame();
    }

    @Override
    public Set<String> getDatabases() throws VerdictException {
        Set<String> databases = new HashSet<String>();
        List<Row> rows = getDatabaseNamesInDataFrame().collectAsList();
        for (Row row : rows) {
            String dbname = row.getString(0); 
            databases.add(dbname);
        }
        return databases;
    }

    @Override
    public List<String> getTables(String schema) throws VerdictException {
        List<String> tables = new ArrayList<String>();
        List<Row> rows = getTablesInDataFrame(schema).collectAsList();
        for (Row row : rows) {
            String table = row.getString(0);
            tables.add(table);
        }
        return tables;
    }

    @Override
    public long getTableSize(TableUniqueName tableName) throws VerdictException {
        String sql = String.format("select count(*) from %s", tableName);
        Dataset<Row> df = executeSparkQuery(sql);
        long size = df.collectAsList().get(0).getLong(0);
        return size;
    }

    @Override
    public Map<String, String> getColumns(TableUniqueName table) throws VerdictException {
        Map<String, String> col2type = new LinkedHashMap<String, String>();
        List<Row> rows = describeTableInDataFrame(table).collectAsList();
        for (Row row : rows) {
            String column = row.getString(0);
            String type = row.getString(1);
            col2type.put(column, type);
        }
        return col2type;
    }

    @Override
    public void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues)
            throws VerdictException {
        VerdictLogger.warn(this, "deleteEntry() not implemented for DbmsSpark");
    }

    @Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into %s ", tableName));
        sql.append("select t.* from (select ");
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(") t");
        executeUpdate(sql.toString());
    }

    @Override
    public void cacheTable(TableUniqueName tableName) {
        if (vc.getConf().cacheSparkSamples() && !cachedTable.contains(tableName)) {
            spark.catalog().cacheTable(tableName.toString());
            cachedTable.add(tableName);
        }
    }

    @Override
    public String modOfHash(String col, int mod) {
        return String.format("crc32(cast(%s as string)) %% %d", col, mod);
    }

    @Override
    protected String randomPartitionColumn() {
        int pcount = partitionCount();
        return String.format("round(rand(unix_timestamp())*%d) %% %d AS %s", pcount, pcount, partitionColumnName());
    }

    @Override
    protected String randomNumberExpression(SampleParam param) {
        String expr = "rand(unix_timestamp())";
        return expr;
    }

    @Override
    public boolean isSpark() {
        return true;
    }

    @Override
    public void close() throws VerdictException {
        // TODO Auto-generated method stub
    }

    @Override
    protected String modOfRand(int mod) {
        return String.format("pmod(abs(rand(unix_timestamp())), %d)", mod);
    }

}
