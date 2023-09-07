// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

/**
 * Create debug point statement.
 * syntax:
 * CREATE DEBUG POINT debugPointName
 * [ PROPERTIES ( ... ) ]
 */
public class CreateDebugPointStmt extends DdlStmt {
    private static String EXECUTE = "execute";
    private static String TIMEOUT = "timeout";

    private String debugPointName;
    private Map<String, String> properties;
    private int executeLimit = -1;
    private long timeoutSeconds = -1;

    public CreateDebugPointStmt(String debugPointName, Map<String, String> properties) {
        this.debugPointName = debugPointName;
        this.properties = properties;
    }

    public String getDebugPointName() {
        return debugPointName;
    }

    public int getExcuteLimit() {
        return executeLimit;
    }

    public long getTimeoutSeconds() {
        return timeoutSeconds;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        checkProperties();
    }

    private void checkProperties() throws AnalysisException {
        if (properties == null) {
            return;
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();

            if (key.equalsIgnoreCase(EXECUTE)) {
                try {
                    executeLimit = Integer.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid execute format: " + val);
                }
            } else if (key.equalsIgnoreCase(TIMEOUT)) {
                try {
                    timeoutSeconds = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid timeout format: " + val);
                }
            } else {
                throw new AnalysisException("Unknown property: " + key);
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DEBUG POINT `").append(debugPointName).append("`");
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, " = ", true, true, true));
            sb.append(")");
        }

        return sb.toString();
    }
}
