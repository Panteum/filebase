/**
 * An enum for data operations.
 * 
 */
const DATA_OPS = {
    INSERT: `insert`,
    UPDATE: `update`,
    DELETE: `delete`,
    SELECT: `select`,
    CLEANUP: `cleanup`
}

/**
 * An enum for errors in filebases.
 * 
 */
const FILEBASE_ERRS = {
    NONE: 0,
    CORRUPTION: 1,
    WRITE: 2,
    READ: 3,
    COMMON: 4,
    UNLOADED: 5
}

/**
 * Delay in execution of data operations, in milliseconds.
 * 
 */
const OPS_EXEC_DELAY = 10

/**
 * Name of the error event.
 * 
 */
const ERROR_EVENT = `error`

/**
 * Default limit in size for a record cache.
 * 
 */
const CACHE_LIMIT = 1000

/**
 * Function that just returns anything that is passed.
 * 
 * @param {*} value - Value to just return.
 */
const HOLLOW_FUNC = function (value) {
    return value
}

/**
 * A data operation to be performed in a Slotbase or a Heapbase.
 * 
 * @typedef DataOp
 * @property {string} type - Type of the operation to be performed.
 * @property {any} id - Value used to identify the operation.
 * @property {any[]} args - Arguments to the operation.
 */

module.exports = {
    DATA_OPS,
    FILEBASE_ERRS,
    OPS_EXEC_DELAY,
    ERROR_EVENT,
    CACHE_LIMIT,
    HOLLOW_FUNC
}