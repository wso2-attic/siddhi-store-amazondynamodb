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

import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.table.record.BaseExpressionVisitor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.DYNAMODB_AND;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.EXCEPTION_MATH_OPERATOR;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.SEPARATOR;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.SIMPLE_PRIMARY_KEY;

/**
 * Class which is used by the Siddhi runtime for instructions on converting the SiddhiQL condition to the condition
 * format understood by the underlying Amazon DynamoDB data store.
 */
public class AmazonDynamoDBConditionVisitor extends BaseExpressionVisitor {

    private StringBuilder condition;
    private String finalCompiledCondition;
    private StringBuilder keyConditionExpression;
    private StringBuilder nonKeyCondition;
    private String partitionKey;
    private String sortKey;
    private String primaryKeyType;

    private Map<String, Object> placeholders;
    private Map<String, Object> placeholdersConstants;
    private SortedMap<Integer, Object> parameters;
    private SortedMap<Integer, Object> parametersConst;


    private int streamVarCount;
    private int constantCount;

    public AmazonDynamoDBConditionVisitor(String partitionKey,
                                          String sortKey, String primaryKeyType) {
        this.condition = new StringBuilder();
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
        this.primaryKeyType = primaryKeyType;
        this.placeholders = new HashMap<>();
        this.placeholdersConstants = new HashMap<>();
        this.parameters = new TreeMap<>();
        this.parametersConst = new TreeMap<>();
        this.keyConditionExpression = new StringBuilder();
        this.nonKeyCondition = new StringBuilder();
    }

    public String returnCondition() {
        this.parametrizeCondition();
        this.generateKeyCondition();
        this.generateNonKeyCondition();
        return this.finalCompiledCondition.trim();
    }

    public SortedMap<Integer, Object> getParameters() {
        return this.parameters;
    }

    public SortedMap<Integer, Object> getParametersConst() {
        return this.parametersConst;
    }

    public StringBuilder getKeyCondition() {
        return this.keyConditionExpression;
    }

    public StringBuilder getNonKeyCondition() {
        return this.nonKeyCondition;
    }

    @Override
    public void beginVisitAnd() {
    }

    @Override
    public void endVisitAnd() {
    }

    @Override
    public void beginVisitAndLeftOperand() {
    }

    @Override
    public void endVisitAndLeftOperand() {
        condition.append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void beginVisitAndRightOperand() {
        condition.append(AmazonDynamoDBTableConstants.DYNAMODB_AND).append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitAndRightOperand() {
    }

    @Override
    public void beginVisitOr() {
        condition.append(AmazonDynamoDBTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitOr() {
        condition.append(AmazonDynamoDBTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitOrLeftOperand() {
    }

    @Override
    public void endVisitOrLeftOperand() {
        condition.append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void beginVisitOrRightOperand() {
        condition.append(AmazonDynamoDBTableConstants.DYNAMODB_OR).append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitOrRightOperand() {
    }

    @Override
    public void beginVisitNot() {
        condition.append(AmazonDynamoDBTableConstants.DYNAMODB_NOT).append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitNot() {
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        switch (operator) {
            case EQUAL:
                condition.append(AmazonDynamoDBTableConstants.DYNAMODB_COMPARE_EQUAL);
                break;
            case GREATER_THAN:
                condition.append(AmazonDynamoDBTableConstants.DYNAMODB_COMPARE_GREATER_THAN);
                break;
            case GREATER_THAN_EQUAL:
                condition.append(AmazonDynamoDBTableConstants.DYNAMODB_COMPARE_GREATER_THAN_EQUAL);
                break;
            case LESS_THAN:
                condition.append(AmazonDynamoDBTableConstants.DYNAMODB_COMPARE_LESS_THAN);
                break;
            case LESS_THAN_EQUAL:
                condition.append(AmazonDynamoDBTableConstants.DYNAMODB_COMPARE_LESS_THAN_EQUAL);
                break;
            case NOT_EQUAL:
                condition.append(AmazonDynamoDBTableConstants.DYNAMODB_COMPARE_NOT_EQUAL);
                break;
        }
        condition.append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
    }

    @Override
    public void beginVisitIsNull(String streamId) {
    }

    @Override
    public void endVisitIsNull(String streamId) {
        condition.append(AmazonDynamoDBTableConstants.DYNAMODB_IS_NULL).append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void beginVisitIn(String storeId) {
        condition.append(AmazonDynamoDBTableConstants.DYNAMODB_IN).append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitIn(String storeId) {
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        String name = this.generateConstantName();
        this.placeholdersConstants.put(name, value);
        condition.append("[").append(name).append("]").append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        throw new OperationNotSupportedException(EXCEPTION_MATH_OPERATOR);
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        throw new OperationNotSupportedException(EXCEPTION_MATH_OPERATOR);
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
        throw new OperationNotSupportedException(EXCEPTION_MATH_OPERATOR);
    }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) {
        throw new OperationNotSupportedException(EXCEPTION_MATH_OPERATOR);
    }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {
        throw new OperationNotSupportedException(EXCEPTION_MATH_OPERATOR);
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) {
        throw new OperationNotSupportedException(EXCEPTION_MATH_OPERATOR);
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {
    }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
    }

    @Override
    public void beginVisitParameterAttributeFunction(int index) {
    }

    @Override
    public void endVisitParameterAttributeFunction(int index) {
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        String name = this.generateStreamVarName();
        this.placeholders.put(name, new Attribute(id, type));
        condition.append("[").append(name).append("]").append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        condition.append(attributeName).append(AmazonDynamoDBTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {

    }

    /**
     * Util method for walking through the generated condition string and isolating the parameters which will be filled
     * in later as part of building the SQL statement. This method will:
     * (a) eliminate all temporary placeholders and put "?" in their places.
     * (b) build and maintain a sorted map of ordinals and the corresponding parameters which will fit into the above
     * places in the PreparedStatement.
     */
    private void parametrizeCondition() {
        String query = this.condition.toString();
        String[] tokens = query.split("\\[");
        int ordinal = 1;
        int ordinalCon = 1;
        for (String token : tokens) {
            if (token.contains("]")) {
                String candidate = token.substring(0, token.indexOf("]"));
                if (this.placeholders.containsKey(candidate)) {
                    candidate = ":" + candidate;
                    this.parameters.put(ordinal, candidate);
                    ordinal++;
                } else if (this.placeholdersConstants.containsKey(candidate)) {
                    this.parametersConst.put(ordinalCon, this.placeholdersConstants.get(candidate));
                    candidate = ":" + candidate;
                    this.parameters.put(ordinal, candidate);
                    ordinalCon++;
                    ordinal++;
                }
            }
        }
        for (String placeholder : this.placeholders.keySet()) {
            query = query.replace("[" + placeholder + "]", ":" + placeholder);
        }

        for (String placeholder : this.placeholdersConstants.keySet()) {
            query = query.replace("[" + placeholder + "]", ":" + placeholder);
        }
        this.finalCompiledCondition = query;
    }

    /*
     * This method is used to seperate key conditions from the query condition.
     */
    private void generateKeyCondition() {
        String query = this.finalCompiledCondition;
        StringBuilder keyCondition = new StringBuilder();
        if (query.contains(DYNAMODB_AND)) {
            String[] conditionArray = query.split(DYNAMODB_AND);
            if (this.primaryKeyType.equals(SIMPLE_PRIMARY_KEY)) {
                for (String con : conditionArray) {
                    if (con.contains(this.partitionKey)) {
                        keyCondition.append(con);
                    }
                }
            } else {
                StringBuilder partitionKeyCondition = new StringBuilder();
                StringBuilder sortKeyCondition = new StringBuilder();
                for (String con : conditionArray) {
                    if (con.contains(this.partitionKey)) {
                        partitionKeyCondition.append(con);
                    } else if (con.contains(this.sortKey)) {
                        sortKeyCondition.append(con);
                    }
                }
                keyCondition.append(partitionKeyCondition).append(SEPARATOR).append(sortKeyCondition);
            }
        } else if (query.contains(this.partitionKey)) {
            keyCondition.append(query);
        }
        this.keyConditionExpression = keyCondition;
    }

    /*
     * This method is used to seperate non key conditions from the query condition.
     */
    private void generateNonKeyCondition() {
        String query = this.finalCompiledCondition;
        query = query.replace("[", ":");
        String[] conditionArray = query.split(DYNAMODB_AND);
        if (query.contains(DYNAMODB_AND)) {
            for (String con : conditionArray) {
                if (primaryKeyType.equals(SIMPLE_PRIMARY_KEY)) {
                    if (!con.contains(this.partitionKey)) {
                        nonKeyCondition.append(String.join(DYNAMODB_AND, con));
                    }
                } else {
                    if (!con.contains(this.partitionKey) && !con.contains(this.sortKey)) {
                        nonKeyCondition.append(String.join(DYNAMODB_AND, con));
                    }
                }
            }
        } else if (!query.contains(DYNAMODB_AND)) {
            if (!query.contains(this.partitionKey)) {
                nonKeyCondition.append(query);
            } else {
                nonKeyCondition = null;
            }
        }
    }

    /**
     * Method for generating a temporary placeholder for stream variables.
     *
     * @return a placeholder string of known format.
     */
    private String generateStreamVarName() {
        String name = "strVar" + this.streamVarCount;
        this.streamVarCount++;
        return name;
    }

    /**
     * Method for generating a temporary placeholder for constants.
     *
     * @return a placeholder string of known format.
     */
    private String generateConstantName() {
        String name = "const" + this.constantCount;
        this.constantCount++;
        return name;
    }
}
