package org.xiaoyume.simpleDB.backend.common;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/29 20:18
 */
public class Error {
    //common
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write!");

    //dm
    public static final Exception BadLogFileException = new RuntimeException("Bad log file!");
    public static final Exception MemTooSmallException = new RuntimeException("Memory too small!");
    public static final Exception DataTooLargeException = new RuntimeException("Data too large!");
    public static final Exception DatabaseBusyException = new RuntimeException("Database is busy!");

    // tm
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

    // vm
    public static final Exception DeadlockException = new RuntimeException("Deadlock!");
    public static final Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");//并发更新异常
    public static final Exception NullEntryException = new RuntimeException("Null entry!");

    //tbm
    public static final Exception InvalidFieldException = new RuntimeException("Invalid field type!");
    public static final Exception FieldNotFoundException = new RuntimeException("Field not found!");
    public static final Exception FieldNotIndexedException = new RuntimeException("Field not indexed!");
    public static final Exception InvalidLogOpException = new RuntimeException("Invalid logic operation!");
    public static final Exception InvalidValuesException = new RuntimeException("Invalid values!");
    public static final Exception DuplicatedTableException = new RuntimeException("Duplicated table!");
    public static final Exception TableNotFoundException = new RuntimeException("Table not found!");
}
