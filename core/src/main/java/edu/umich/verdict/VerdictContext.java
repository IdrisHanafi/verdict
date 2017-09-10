/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict;

import java.sql.ResultSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;

public abstract class VerdictContext {

    final protected VerdictConf conf;

    protected VerdictMeta meta;

    final static protected Set<String> JDBC_DBMS = Sets.newHashSet("mysql", "impala", "hive", "hive2");

    /*
     *  DBMS fields
     */
    private Dbms dbms;

    private Dbms metaDbms;		// contains persistent info of VerdictMeta


    // used for refreshing meta data.
    private long queryUid;

    final protected int contextId;


    public Dbms getDbms() {
        return dbms;
    }

    public void setDbms(Dbms dbms) {
        this.dbms = dbms;
        this.metaDbms = dbms;
    }

    public int getContextId() {
        return contextId;
    }

    public long getQid() {
        return queryUid;
    }

    public VerdictMeta getMeta() {
        return meta;
    }

    public void setMeta(VerdictMeta meta) {
        this.meta = meta;
    }

    public String getDefaultSchema() {
        return conf.getDbmsSchema();
    }

    public VerdictConf getConf() {
        return conf;
    }

    public Dbms getMetaDbms() {
        return metaDbms;
    }

    public Optional<String> getCurrentSchema() {
        return dbms.getCurrentSchema();
    }

    public void destroy() throws VerdictException {
        dbms.close();
    }

    public long getCurrentQid() {
        return queryUid;
    }

    public void incrementQid() {
        queryUid += 1;
    }

    protected VerdictContext(VerdictConf conf, int contextId) {
        this.conf = conf;
        this.contextId = contextId;
    }

    protected VerdictContext(VerdictConf conf) {
        this(conf, ThreadLocalRandom.current().nextInt(0, 10000));
    }

    private static VerdictContext dummyContext = null;

    /**
     * Singleton dummy VerdictContext. Used only by FuncExpr for setting inherited Expr's VerdictContext field.
     * @return
     */
    public static VerdictContext dummyContext() {
        if (dummyContext != null) return dummyContext;

        VerdictConf conf = new VerdictConf(false);
        conf.setDbms("dummy");
        try {
            dummyContext = VerdictJDBCContext.from(conf);
            return dummyContext;
        } catch (VerdictException e) {
            e.printStackTrace();
        }
        return null;
    }

    public abstract void execute(String sql) throws VerdictException;

    public abstract ResultSet getResultSet();

    public abstract Dataset<Row> getDataFrame();

    public ResultSet executeJdbcQuery(String sql) throws VerdictException {
        execute(sql);
        ResultSet rs = getResultSet();
        return rs;
    }

    public Dataset<Row> executeSparkQuery(String sql) throws VerdictException {
        execute(sql);
        Dataset<Row> df = getDataFrame();
        return df;
    }

    public Dataset<Row> sql(String sql) throws VerdictException {
        Dataset<Row> df = executeSparkQuery(sql);
        return df;
    }

}
