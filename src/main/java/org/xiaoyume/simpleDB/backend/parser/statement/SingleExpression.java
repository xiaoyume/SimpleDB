package org.xiaoyume.simpleDB.backend.parser.statement;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 单表达式，包含字段名，比较符，值
 * 字段 < = > value   比如查   age < 20
 * @date 2024/3/1 15:47
 */
public class SingleExpression {
    public String field;
    public String compareOp;
    public String value;
}
