/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.store.amazondynamodb;

import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.SortedMap;

/**
 * Implementation class of {@link CompiledCondition} corresponding to the Amazon DynamoDB Event Table.
 * Maintains the condition string returned by the ConditionVisitor as well as a map of parameters to be used at runtime.
 */
public class AmazonDynamoDBCompiledCondition implements CompiledCondition {

    private String compiledQuery;
    private SortedMap<Integer, Object> parameters;
    private SortedMap<Integer, Object> parametersConst;
    private StringBuilder keyCondition;
    private StringBuilder nonKeyCondition;

    public AmazonDynamoDBCompiledCondition(String compiledQuery, SortedMap<Integer, Object> parameters,
                                           SortedMap<Integer, Object> parametersConst, StringBuilder keyCondition,
                                           StringBuilder nonKeyCondition) {
        this.compiledQuery = compiledQuery;
        this.parameters = parameters;
        this.parametersConst = parametersConst;
        this.keyCondition = keyCondition;
        this.nonKeyCondition = nonKeyCondition;
    }

    public String getCompiledQuery() {
        return compiledQuery;
    }

    public SortedMap<Integer, Object> getParametersConst() {
        return parametersConst;
    }

    public String toString() {
        return getCompiledQuery();
    }

    public SortedMap<Integer, Object> getParameters() {
        return parameters;
    }

    public StringBuilder getKeyCondition() {
        return keyCondition;
    }

    public StringBuilder getNonKeyCondition() {
        return nonKeyCondition;
    }

    @Override
    public CompiledCondition cloneCompilation(String s) {
        return new AmazonDynamoDBCompiledCondition(this.compiledQuery, this.parameters, this.parametersConst,
                this.keyCondition, this.nonKeyCondition);
    }
}
