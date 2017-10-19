/*
 *  * Copyright 2017 University of Michigan
 *   *
 *    * Licensed under the Apache License, Version 2.0 (the "License");
 *     * you may not use this file except in compliance with the License.
 *      * You may obtain a copy of the License at
 *       *
 *        *     http://www.apache.org/licenses/LICENSE-2.0
 *         *
 *          * Unless required by applicable law or agreed to in writing, software
 *           * distributed under the License is distributed on an "AS IS" BASIS,
 *            * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *             * See the License for the specific language governing permissions and
 *              * limitations under the License.
 *               */

package edu.umich.verdict.hive;

import java.io.FileNotFoundException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.BeforeClass;

import edu.umich.verdict.BasicTest;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

import java.sql.Connection;
import java.sql.Statement;

public class HiveBasicTest extends BasicTest {
    
    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {

    final String host = readHost();
    final String port = "10000";
    final String schema = "instacart1g";

	VerdictConf conf = new VerdictConf();
        conf.setDbms("hive2");
        conf.setHost(host);
        conf.setPort(port);
        //conf.setDbmsSchema(schema);
        //conf.set("no_user_password","true");
        vc = VerdictJDBCContext.from(conf);

    }

}
