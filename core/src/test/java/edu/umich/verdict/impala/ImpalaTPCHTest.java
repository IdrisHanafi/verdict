/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.impala;

import java.io.FileNotFoundException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import edu.umich.verdict.TestBase;
import edu.umich.verdict.BasicTest;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ImpalaTPCHTest extends BasicTest{
    @BeforeClass
    public static void Setup() throws VerdictException, FileNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(TestBase.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        vc = VerdictJDBCContext.from(conf);
    }

    @Test
    public void TestTPCH1() throws VerdictException, FileNotFoundException {
        String sql = "select\n" + " l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,\n"
                + " sum(l_extendedprice) as sum_base_preice, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,"
                + " avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order\n"
                + "from\n" + " lineitem\n" + "where\n" + " l_shipdate <= '1998-10-03'\n"
                + "group by\n" + " l_returnflag, l_linestatus\n" + "order by\n" + " l_returnflag, l_linestatus;\n";
                vc.executeJdbcQuery(sql);
    }

    @Test
    public void TestTPCH2() throws VerdictException, FileNotFoundException { 

        String sql = "select\n" + "  s_acctbal, s_name, n_name, p_partkey, p_mfgr,\n"
                + " s_address, s_phone, s_comment\n" + "from\n" + " part, supplier, partsupp, nation, region\n"
                + "where\n" + " p_partkey = ps_partkey\n" + " and s_suppkey = ps_suppkey\n" + " and p_size = 15\n"
                + " and p_type like '%BRASS'\n" + " and s_nationkey = n_nationkey\n"
                + " and n_regionkey = r_regionkey\n" + " and r_name = 'EUROPE'\n" + " and ps_supplycost = (\n"
                + "  select\n" + "   min(ps_supplycost)\n" + "  from\n" + "   partsupp,\n" + "   supplier,\n"
                + "   nation,\n" + "   region,\n" + "   part\n" + "  where\n" + "   p_partkey = ps_partkey\n"
                + "   and s_suppkey = ps_suppkey\n" + "   and s_nationkey = n_nationkey\n"
                + "   and n_regionkey = r_regionkey\n" + "   and r_name = 'EUROPE'\n" + "  )\n" + "order by\n"
                + " s_acctbal desc,\n" + " n_name,\n" + " s_name,\n" + " p_partkey\n" + "limit 100;\n";
        vc.executeJdbcQuery(sql);
    }

    @Test
    public void TestTPCH3() throws VerdictException, FileNotFoundException {
        String sql = " select\n" + " l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate,\n"
                + " o_shippriority\n" + "from\n" + " customer, orders, lineitem\n" + "where\n" + "c_mktsegment='BUILDING'"
                + " and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15'"
                + " and l_shipdate > '1995-03-15'\n" + "group by\n" + " l_orderkey, o_orderdate, o_shippriority\n"
                + "order by\n" + " revenue desc, o_orderdate;\n";

                vc.executeJdbcQuery(sql);
    }



    //@Test		//THROWS ERROR
    public void TestTPCH4() throws VerdictException, FileNotFoundException, NullPointerException{
        String sql = " select\n" + " o_orderpriority, count(*) as order_count\n" + "from\n"
        + " orders\n" + "where\n" + "o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01'"
        + " and EXISTS(select * from lineitem where l_orderkey=o_orderkey and l_commitdate<l_receiptdate)\n"
        + " group by\n" + " o_orderpriority\n" + "order by\n" + " o_orderpriority;\n";

        //System.out.println("Before " + sql);
        vc.executeJdbcQuery(sql);
        //System.out.println("After " + sql);
    }

    @Test
    public void TestTPCH5() throws VerdictException, FileNotFoundException {
        String sql = "select\n" + " n_name, sum(l_extendedprice * (1-l_discount)) as revenue\n"
        + "from\n" + " customer, orders, lineitem, supplier, nation, region\n" + "where\n"
        + " c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey\n"
        + " and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey"
        + " and r_name = 'ASIA' and o_orderdate >= '1994-01-01' and o_orderdate < '1995-01-01'\n"
        + " group by\n" + " n_name\n" + "order by\n" + " revenue desc;\n";

        vc.executeJdbcQuery(sql);

    }

    //@Test
    public void TestTPCH6() throws VerdictException, FileNotFoundException {
        String sql = "select\n" + " sum(l_extendedprice * l_discount) as revenue\n"
        + "from\n" + " lineitem\n" + "where\n" + " l_shipdate >= '1994-01-01' and "
        + "l_shipdate < 1995-01-01 and l_discount between 0.05 and 0.07 and l_quantity < 24";
        vc.executeJdbcQuery(sql);
    }

    //@Test
    public void TestTPCH7() throws VerdictException, FileNotFoundException {
        String sql = "select\n" + " supp_nation, cust_nation, l_year, sum(volume) as revenue\n"
        + "from (\n" + " select\n" + " n1.n_name as supp_nation,n2.n_name as cust_nation, extract"
        + "(year from l_shipdate) as l_year, l_extendedprice * (1-l_discount as volume\n"
        + " from\n" + " supplier, lineitem, orders, customer, nation n1, nation n2\n"
        + " where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey"
        + " and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ("
        + "(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')"
        + ")\n" + " and l_shipdate between '1995-01-01' and '1996-12-31'\n" + ") as shipping\n"
        + "group by\n" + " supp_nation, cust_nation,l_year\n" + "order by\n" + " supp_nation, cust_nation, l_year;";
        vc.executeJdbcQuery(sql);
    }

    //@Test		//THROWS ERROR
    public void TestTPCH8() throws VerdictException, FileNotFoundException {
        String sql = "select\n" + 
        		"o_year,\n" + 
        		"sum(case\n" + 
        		"when nation = 'BRAZIL'\n" + 
        		"then volume\n" + 
        		"else 0\n" + 
        		"end) / sum(volume) as mkt_share\n" + 
        		"from (\n" + 
        		"select\n" + 
        		"extract(year from o_orderdate) as o_year,\n" + 
        		"l_extendedprice * (1-l_discount) as volume,\n" + 
        		"n2.n_name as nation\n" + 
        		"from\n" + 
        		"part,\n" + 
        		"supplier,\n" + 
        		"lineitem,\n" + 
        		"orders,\n" + 
        		"customer,\n" + 
        		"nation n1,\n" + 
        		"nation n2,\n" + 
        		"region\n" + 
        		"where\n" + 
        		"p_partkey = l_partkey\n" + 
        		"and s_suppkey = l_suppkey\n" + 
        		"and l_orderkey = o_orderkey\n" + 
        		"and o_custkey = c_custkey\n" + 
        		"and c_nationkey = n1.n_nationkey\n" + 
        		"and n1.n_regionkey = r_regionkey\n" + 
        		"and r_name = 'AMERICA'\n" + 
        		"and s_nationkey = n2.n_nationkey\n" + 
        		"and o_orderdate between date '1995-01-01' and date '1996-12-31'\n" + 
        		"and p_type = 'ECONOMY ANODIZED STEEL'\n" + 
        		") as all_nations\n" + 
        		"group by\n" + 
        		"o_year\n" + 
        		"order by\n" + 
        		"o_year;";
        vc.executeJdbcQuery(sql);
    }

    @Test
    public void TestTPCH9() throws VerdictException, FileNotFoundException {
        String sql = "select nation, o_year, sum(amount) as sum_profit\n" + "from (\n" + "  select\n"
                + "    n_name as nation,\n" + "    year(o_orderdate) as o_year,\n"
                + "    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n" + "  from\n"
                + "    lineitem\n" + "    inner join orders on o_orderkey = l_orderkey\n"
                + "    inner join partsupp on ps_suppkey = l_suppkey\n"
                + "    inner join part on p_partkey = ps_partkey\n"
                + "    inner join supplier on s_suppkey = ps_suppkey\n"
                + "    inner join nation on s_nationkey = n_nationkey\n" + "  where p_name like '%green%') as profit\n"
                + "group by nation, o_year\n" + "order by nation, o_year desc";
        vc.executeJdbcQuery(sql);
    }

    @Test
    public void TestTPCH10() throws VerdictException, FileNotFoundException {
        String sql = "select\n" + " c_custkey, c_name, sum(l_extendedprice * (1-l_discount)) as revenue,"
                + " c_acctbal, n_name, c_address, c_phone, c_comment\n" + "from\n" + " customer, "
                + " orders, lineitem, nation\n" + "where\n" + " c_custkey = o_custkey and "
                + " o_orderdate >= '1993-10-01' and o_orderdate < '1994-01-01' and l_returnflag='R'"
                + " and c_nationkey = n_nationkey\n" + "group by\n" + " c_custkey, c_name, c_acctbal,"
                + " c_phone, n_name, c_address, c_comment\n" + "order by\n" + " revenue_desc;";
    }

    @Test
    public void TestTPCH10_2() throws VerdictException, FileNotFoundException {
        String sql = "select\n" + 
        		"c_custkey,\n" + 
        		"c_name,\n" + 
        		"sum(l_extendedprice * (1 - l_discount)) as revenue,\n" + 
        		"c_acctbal,\n" + 
        		"n_name,\n" + 
        		"c_address,\n" + 
        		"c_phone,\n" + 
        		"c_comment\n" + 
        		"from\n" + 
        		"customer,\n" + 
        		"orders,\n" + 
        		"lineitem,\n" + 
        		"nation\n" + 
        		"where\n" + 
        		"c_custkey = o_custkey\n" + 
        		"and l_orderkey = o_orderkey\n" + 
        		"and o_orderdate >= '1993-10-01'\n" + 
        		"and o_orderdate < '1994-01-01'\n" + 
        		"and l_returnflag = 'R'\n" + 
        		"and c_nationkey = n_nationkey\n" + 
        		"group by\n" + 
        		"c_custkey,\n" + 
        		"c_name,\n" + 
        		"c_acctbal,\n" + 
        		"c_phone,\n" + 
        		"n_name,\n" + 
        		"c_address,\n" + 
        		"c_comment\n" + 
        		"order by\n" + 
        		"revenue desc;";
                vc.executeJdbcQuery(sql);
    }
    
    @Test
    public void TestTPCH11() throws VerdictException, FileNotFoundException {
    		String sql = "select\n" + 
    				"ps_partkey,\n" + 
    				"sum(ps_supplycost * ps_availqty) as value\n" + 
    				"from\n" + 
    				"partsupp,\n" + 
    				"supplier,\n" + 
    				"nation\n" + 
    				"where\n" + 
    				"ps_suppkey = s_suppkey\n" + 
    				"and s_nationkey = n_nationkey\n" + 
    				"and n_name = 'GERMANY'\n" + 
    				"group by\n" + 
    				"ps_partkey having\n" + 
    				"sum(ps_supplycost * ps_availqty) > (\n" + 
    				"select\n" + 
    				"sum(ps_supplycost * ps_availqty) * 0.0001\n" + 
    				"from\n" + 
    				"partsupp,\n" + 
    				"supplier,\n" + 
    				"nation\n" + 
    				"where\n" + 
    				"ps_suppkey = s_suppkey\n" + 
    				"and s_nationkey = n_nationkey\n" + 
    				"and n_name = 'GERMANY'\n" + 
    				")\n" + 
    				"order by\n" + 
    				"value desc;";
    		vc.executeJdbcQuery(sql);
    }

    @Test
    public void TestTPCH12() throws VerdictException, FileNotFoundException {
        String sql = "select\n" + " l_shipmode,\n" + " sum(case\n" + "  when o_orderpriority = '1-URGENT'\n"
                + "   or o_orderpriority = '2-HIGH'\n" + "   then 1\n" + "   else 0\n" + "  end) as high_line_count,\n"
                + "  sum(case\n" + "   when o_orderpriority <> '1-URGENT'\n" + "    and o_orderpriority <> '2-HIGH'\n"
                + "     then 1\n" + "   else 0\n" + "   end) as low_line_count\n" + "from\n" + " orders,\n"
                + " lineitem\n" + "where\n" + " o_orderkey = l_orderkey\n" + " and l_shipmode in ('MAIL', 'SHIP')\n"
                + " and l_commitdate < l_receiptdate\n" + " and l_shipdate < l_commitdate\n"
                + " and l_receiptdate >= '1994-01-01'\n" + " and l_receiptdate < '1995-01-01'\n" + "group by\n"
                + " l_shipmode\n" + "order by\n" + " l_shipmode;\n";
        vc.executeJdbcQuery(sql);
    }
    
    //@Test		//THROWS ERROR
    public void TestTPCH13() throws VerdictException, FileNotFoundException {
    		String sql = "select\n" + 
    				"c_count, count(*) as custdist\n" + 
    				"from (\n" + 
    				"select\n" + 
    				"c_custkey,\n" + 
    				"count(o_orderkey)\n" + 
    				"from\n" + 
    				"customer left outer join orders on\n" + 
    				"c_custkey = o_custkey\n" + 
    				"and o_comment not like ‘%special%requests%’\n" + 
    				"group by\n" + 
    				"c_custkey\n" + 
    				")as c_orders (c_custkey, c_count)\n" + 
    				"group by\n" + 
    				"c_count\n" + 
    				"order by\n" + 
    				"custdist desc,\n" + 
    				"c_count desc;";
    		vc.executeJdbcQuery(sql);
    }
    
    @Test
    public void TestTPCH14() throws VerdictException, FileNotFoundException {
    		String sql = "select\n" + 
    				"100.00 * sum(case\n" + 
    				"when p_type like 'PROMO%'\n" + 
    				"then l_extendedprice*(1-l_discount)\n" + 
    				"else 0\n" + 
    				"end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\n" + 
    				"from\n" + 
    				"lineitem,\n" + 
    				"part\n" + 
    				"where\n" + 
    				"l_partkey = p_partkey\n" + 
    				"and l_shipdate >= '1995-09-01'\n" + 
    				"and l_shipdate < '1995-10-01'";
    		vc.executeJdbcQuery(sql);
    }
    
    @Test
    public void TestTPCH15() throws VerdictException, FileNotFoundException {
        String sql0 = "select l_suppkey as supplier_no,\n"
                + "       sum(l_extendedprice * (1 - l_discount)) as total_revenue\n" + "from lineitem\n"
                + "where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01'\n" + "group by l_suppkey\n"
                + "limit 10;\n";

        String sql1 = "create view revenue_temp as\n" + "select l_suppkey as supplier_no,\n"
                + "       sum(l_extendedprice * (1 - l_discount)) as total_revenue\n" + "from lineitem\n"
                + "where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01'\n" + "group by l_suppkey;\n";

        String sql2 = "select s_suppkey, s_name, s_address, s_phone, total_revenue\n" + "from supplier, revenue_temp\n"
                + "where s_suppkey = supplier_no\n" + "  and total_revenue = (\n"
                + "        select max(total_revenue)\n" + "        from revenue_temp)\n" + "order by s_suppkey;\n";

        String sql3 = "drop view revenue_temp;\n";

        vc.executeJdbcQuery(sql0);
    }
    
    //@Test
    public void TestTPCH16() throws VerdictException, FileNotFoundException {
    		String sql = "select\n" + 
    				"p_brand,\n" + 
    				"p_type,\n" + 
    				"p_size,\n" + 
    				"count(distinct ps_suppkey) as supplier_cnt\n" + 
    				"from\n" + 
    				"partsupp,\n" + 
    				"part\n" + 
    				"where\n" + 
    				"p_partkey = ps_partkey\n" + 
    				"and p_brand <> 'Brand#45'\n" + 
    				"and p_type not like 'MEDIUM POLISHED%'\n" + 
    				"and p_size in (49, 14, 23, 45, 19, 3, 36, 9)\n" + 
    				"and ps_suppkey not in (\n" + 
    				"select\n" + 
    				"s_suppkey\n" + 
    				"from\n" + 
    				"supplier\n" + 
    				"where\n" + 
    				"s_comment like '%Customer%Complaints%'\n" + 
    				")\n" + 
    				"group by\n" + 
    				"p_brand,\n" + 
    				"p_type,\n" + 
    				"p_size\n" + 
    				"order by\n" + 
    				"supplier_cnt desc,\n" + 
    				"p_brand,\n" + 
    				"p_type,\n" + 
    				"p_size;";
    		vc.executeJdbcQuery(sql);
    }

    @Test
    public void TestTPCH17() throws VerdictException, FileNotFoundException {
        String sql = "select sum(l_extendedprice) / 7.0 as avg_yearly\n" + "from lineitem\n" + "     inner join (\n"
                + "             select l_partkey as partkey, 0.2 * avg(l_quantity) as small_quantity\n"
                + "             from lineitem inner join part on l_partkey = p_partkey\n"
                + "             group by l_partkey) t\n" + "       on l_partkey = partkey\n" + "     inner join part\n"
                + "       on l_partkey = p_partkey\n" + "where p_brand = 'Brand#23' and\n"
                + "      p_container = 'MED BOX' and\n" + "      l_quantity < small_quantity;\n";
        vc.executeJdbcQuery(sql);
    }
    
    //@Test		//THROWS ERROR
    public void TestTPCH18() throws VerdictException, FileNotFoundException {
    	String sql = "select\n" + 
    			"c_name,\n" + 
    			"c_custkey,\n" + 
    			"o_orderkey,\n" + 
    			"o_orderdate,\n" + 
    			"o_totalprice,\n" + 
    			"sum(l_quantity)\n" + 
    			"from\n" + 
    			"customer,\n" + 
    			"orders,\n" + 
    			"lineitem\n" + 
    			"where\n" + 
    			"o_orderkey in (\n" + 
    			"select\n" + 
    			"l_orderkey\n" + 
    			"from\n" + 
    			"lineitem\n" + 
    			"group by\n" + 
    			"l_orderkey having\n" + 
    			"sum(l_quantity) > 300\n" + 
    			")\n" + 
    			"and c_custkey = o_custkey\n" + 
    			"and o_orderkey = l_orderkey\n" + 
    			"group by\n" + 
    			"c_name,\n" + 
    			"c_custkey,\n" + 
    			"o_orderkey,\n" + 
    			"o_orderdate,\n" + 
    			"o_totalprice\n" + 
    			"order by\n" + 
    			"o_totalprice desc,\n" + 
    			"o_orderdate;";
    		vc.executeJdbcQuery(sql);
    }
    
    // @Test		//THROWS ERROR
    public void TestTPCH19() throws VerdictException, FileNotFoundException {
    	String sql = "select\n" + 
    			"sum(l_extendedprice * (1 - l_discount) ) as revenue\n" + 
    			"from\n" + 
    			"lineitem,\n" + 
    			"part\n" + 
    			"where\n" + 
    			"(\n" + 
    			"p_partkey = l_partkey\n" + 
    			"and p_brand = ‘Brand#12’\n" + 
    			"and p_container in ( ‘SM CASE’, ‘SM BOX’, ‘SM PACK’, ‘SM PKG’)\n" + 
    			"and l_quantity >= 1 and l_quantity <= 11\n" + 
    			"and p_size between 1 and 5\n" + 
    			"and l_shipmode in (‘AIR’, ‘AIR REG’)\n" + 
    			"and l_shipinstruct = ‘DELIVER IN PERSON’\n" + 
    			")\n" + 
    			"or\n" + 
    			"(\n" + 
    			"p_partkey = l_partkey\n" + 
    			"and p_brand = ‘Brand#23’\n" + 
    			"and p_container in (‘MED BAG’, ‘MED BOX’, ‘MED PKG’, ‘MED PACK’)\n" + 
    			"and l_quantity >= 10 and l_quantity <= 20\n" + 
    			"and p_size between 1 and 10\n" + 
    			"and l_shipmode in (‘AIR’, ‘AIR REG’)\n" + 
    			"and l_shipinstruct = ‘DELIVER IN PERSON’\n" + 
    			")\n" + 
    			"or\n" + 
    			"(\n" + 
    			"p_partkey = l_partkey\n" + 
    			"and p_brand = ‘Brand#34’\n" + 
    			"and p_container in ( ‘LG CASE’, ‘LG BOX’, ‘LG PACK’, ‘LG PKG’)\n" + 
    			"and l_quantity >= 20 and l_quantity <= 30\n" + 
    			"and p_size between 1 and 15\n" + 
    			"and l_shipmode in (‘AIR’, ‘AIR REG’)\n" + 
    			"and l_shipinstruct = ‘DELIVER IN PERSON’\n" + 
    			");";
    		vc.executeJdbcQuery(sql);
    }
    
    //@Test		//THROWS ERROR
    public void TestTPCH20() throws VerdictException, FileNotFoundException {
    		String sql = "select\n" + 
    				"s_name,\n" + 
    				"s_address\n" + 
    				"from\n" + 
    				"supplier, nation\n" + 
    				"where\n" + 
    				"s_suppkey in (\n" + 
    				"select\n" + 
    				"ps_suppkey\n" + 
    				"from\n" + 
    				"partsupp\n" + 
    				"where\n" + 
    				"ps_partkey in (\n" + 
    				"select\n" + 
    				"p_partkey\n" + 
    				"from\n" + 
    				"part\n" + 
    				"where\n" + 
    				"p_name like 'forest%'\n" + 
    				")\n" + 
    				"and ps_availqty > (\n" + 
    				"select\n" + 
    				"0.5 * sum(l_quantity)\n" + 
    				"from\n" + 
    				"lineitem\n" + 
    				"where\n" + 
    				"l_partkey = ps_partkey\n" + 
    				"and l_suppkey = ps_suppkey\n" + 
    				"and l_shipdate >= '1994-01-01’\n" + 
    				"and l_shipdate < '1995-01-01’\n" + 
    				")\n" + 
    				")\n" + 
    				"and s_nationkey = n_nationkey\n" + 
    				"and n_name = 'CANADA'\n" + 
    				"order by\n" + 
    				"s_name;";
    		vc.executeJdbcQuery(sql);
    }
    
    

}
