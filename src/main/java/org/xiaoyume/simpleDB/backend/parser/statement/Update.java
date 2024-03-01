package org.xiaoyume.simpleDB.backend.parser.statement;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/3/1 15:48
 */
public class Update {
    public String tableName;
    public String fieldName;
    public String value;
    public Where where;
}
