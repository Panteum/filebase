const fs = require(`fs`)
const fsprom = fs.promises

const path = require(`path`)
const EventEmitter = require(`events`)

const OP_DELAY_DEFAULT = 5000

// to accommodate BigInt values
BigInt.prototype.toJSON = function () {
    return this.toString()
}

/**
 * A data operation to be performed in a JSONHeapbase.
 * 
 * @typedef JSONDataOp
 * @property {string} type - Type of the operation to be performed.
 * @property {(value) => void} resolve - Resolving function of the data operation.
 * @property {(err) => void} reject - Rejecting function of the data operation.
 * @property {any[]} args - Arguments to the operation.
 */

/**
 * Builds an entry in a JSONHeapbase's position exchange.
 * 
 * @callback ExchangeBuilder
 * @param {string} recordJSON - Record serialized in JSON.
 * @param {number} position - Position of the serialized record in the file.
 * @param {number} size - Size of the serialized record, in bytes.
 */

/**
 * Database that serializes records as JSON in a file. Allows optimized data write and read.
 * 
 */
class JSONHeapbase extends EventEmitter {
    /**
     * Absolute path of the file used by the JSONHeapbase as persistence medium.
     * 
     * @type {string}
     */
    filepath

    /**
     * Exchange map of a record's id and the record's position in the persistence file.
     * 
     * @type {Map<*, [number, number]>}
     */
    positionExchange
    
    /**
     * Last position in the persistence file.
     * 
     * @type {number}
     */
    lastPosition

    /**
     * Number of milliseconds to lapse before emptying the operation queue.
     * 
     * @type {number}
     */
    opDelay

    /**
     * Queue of pending operations to be performed after the number of milliseconds indicated
     * by opDelay lapse.
     * 
     * @type {JSONDataOp[]}
     */
    opQueue

    /**
     * Id of the timeout used to flush the operation queue.
     * 
     * @type {NodeJS.Timeout}
     */
    timeout

    /**
     * Flag signifying readiness of the JSONHeapbase. If set to true, the JSONHeapbase has already
     * loaded its metadata and is ready for operations.
     * 
     * @type {boolean}
     */
    isLoaded

    /**
     * Creates a JSONHeapbase. The constructor only creates an instance in memory; to load the
     * JSONHeapbase's data, {@link JSONHeapbase#load} should be called.
     * 
     * @param {string} filepath - Path to the persistence file for the JSONHeapbase.
     * @param {number} [opDelay=OP_DELAY_DEFAULT] - Number of milliseconds to lapse before emptying the operation queue.
     */
    constructor (filepath, opDelay) {
        if (!path.isAbsolute(filepath)) {
            throw new Error("Cannot accept a relative filepath.")
        }

        super()

        // load the important values
        this.filepath = filepath
        this.lastPosition = 0

        // load operation-based values
        this.opDelay = opDelay || OP_DELAY_DEFAULT
        this.opQueue = []

        this.isLoaded = false
    }

    
}