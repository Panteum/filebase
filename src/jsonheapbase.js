const fs = require(`fs`)
const fsprom = fs.promises

const path = require(`path`)
const EventEmitter = require(`events`)

const { DATA_OPS } = require(`./constants`)

const OP_DELAY_DEFAULT = 5000

const RECORD_SIZE_SIZE = 4

const RECORD_LENGTH_LIMIT = 2147483647

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
 * Function passed with a record during a filebase's load event.
 * 
 * @callback RecordIterator
 * @param {*} record - Record to be passed.
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
     * Property name of the record's exchange id.
     * 
     * @type {string}
     */
    exchangeIdName

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
     * @param {string} exchangeIdName - Property name of the record's exchange id.
     */
    constructor (filepath, exchangeIdName, opDelay) {
        if (!path.isAbsolute(filepath)) {
            throw new Error(`Cannot accept a relative filepath.`)
        }

        if (!exchangeIdName) {
            throw new Error(`Cannot create the position exchange with the exchangeIdName.`)
        }

        super()

        // load the important values
        this.filepath = filepath
        this.lastPosition = 0
        this.positionExchange = new Map()
        this.exchangeIdName = exchangeIdName

        // load operation-based values
        this.opDelay = opDelay || OP_DELAY_DEFAULT
        this.opQueue = []
        
        this.isLoaded = false
    }

    /**
     * Loads the Heapbase. During loading, occupied and free segment is recorded using a list of indexes.
     * 
     * @param {RecordIterator} iterate - Iterates the records loaded.
     */
    async load(iterate) {
        // get the file stats
        const handle = await fsprom.open(this.filepath, `a+`)
        const stats = await handle.stat()

        try {
            let offset = 0
            while (offset < stats.size) {
                // get the size first
                const sizeBuffer = Buffer.alloc(RECORD_SIZE_SIZE)
                const sizeReadOutput = await handle.read(sizeBuffer, 0, RECORD_SIZE_SIZE, offset)
                const recordSize = sizeReadOutput.buffer.readInt32BE()

                // if recordSize is negative, this means it was deleted
                // don't push it to the position exchange
                if (recordSize > 0) {
                    // if not deleted add to the position exchange
                    const recordPosition = offset

                    // update the offset and read the record
                    offset += RECORD_SIZE_SIZE
                    const recordBuffer = Buffer.alloc(recordSize)
                    const recordReadOutput = await handle.read(recordBuffer, 0, recordSize, offset)
                    const recordJSON = recordReadOutput.buffer.toString()
                    const record = JSON.parse(recordJSON)

                    const exchangeId = record[this.exchangeIdName]
                    if (exchangeId === undefined) {
                        throw new Error(`Record at position: ${recordPosition} doesn't have exchange id property of ${this.exchangeIdName}!`)
                    }

                    iterate(record)

                    this.positionExchange.set(exchangeId, [recordPosition, recordSize])

                    offset += recordSize
                }
            }
        } catch (e) {
            console.debug(`Reading persistence file ${this.filepath} threw error:`, e)
            throw e 
        } finally {
            await handle.close()
        }

        // save last position
        this.lastPosition = stats.size
        
        // ready for action
        this.isLoaded = true
    }
    
    /**
     * Sets the timeout for data operation queue flushing.
     * 
     */
    setFlushTimeout() {
        var self = this
        setTimeout(async function () {
            // perform operations then set another timeout after
            await self.flushOpQueue(null, true)
            self.timeout = null
        }, this.opDelay)
    }

    /**
     * Performs the data operations queued in opQueue.
     * 
     * @param {fs.promises.FileHandle} [handle] - Handle of the Heapbase's persistence file. Can be passed from a parent operation.
     * @param {boolean} [inTimeout] Flag signifying if flush operation is executed in a timeout.
     */
    async flushOpQueue(handle, inTimeout = false) {
        if (this.opQueue.length === 0) {
            return handle
        }

        // open the persistence file if not passed
        var returnHandle = true
        if (!handle) {
            try {
                handle = await fsprom.open(this.filepath, `r+`)
            } catch (e) {
                if (inTimeout) {
                    this.emit(ERROR_EVENT, { err: e })
                    return
                } else {
                    throw e
                }
            }

            returnHandle = false
        }

        while (this.opQueue.length !== 0) {
            // always do FIFO
            const dataOp = this.opQueue.shift()

            // perform the data operation
            // no need to catch outside, errors are caught inside
            if (dataOp.type === DATA_OPS.INSERT) {
                await this._insert(handle, dataOp)
            } else if (dataOp.type === DATA_OPS.DELETE) {
                await this._delete(handle, dataOp)
            }
        }

        // close the persistence file if not passed
        if (!returnHandle) {
            try {
                await handle.close()
            } catch (e) {
                if (inTimeout) {
                    this.emit(ERROR_EVENT, { err: e })
                } else {
                    throw e
                }
            }

            return
        }

        return handle
    }

    /**
     * Inserts a record into the JSONHeapbase through the operation queue.
     * 
     * @param {*} record - Record to be inserted.
     */
    add(record) {
        if (!this.isLoaded) {
            throw new Error(`JSONHeapbase '${this.filepath}' is not loaded.`)
        }

        const self = this
        return new Promise (function (resolve, reject) {
            // create the operation entry
            const dataOp = {
                type: DATA_OPS.INSERT,
                resolve,
                reject,
                args: [record]
            }

            // add to the queue
            self.opQueue.push(dataOp)

            // start the queue timeout, if not started
            if (!self.timeout) {
                self.setFlushTimeout()
            }
        })
    }

    /**
     * Takes a insert data operation and performs it against the JSONHeapbase.
     * 
     * @param {fs.promises.FileHandle} handle - Handle of the JSONHeapbase's persistence file.
     * @param {JSONDataOp} op - Operation to be performed.
     */
    async _insert(handle, op) {
        try {
            const exchangeId = op.args[0][this.exchangeIdName]
            if (exchangeId === undefined) {
                throw new Error(`Cannot insert without an exchange id.`)
            }

            // get the record and form it into JSON.
            const recordJSON = JSON.stringify(op.args[0])

            // form the buffer to insert to the file
            const recordBuffer = Buffer.from(recordJSON)

            if (recordBuffer.length > RECORD_LENGTH_LIMIT) {
                throw new Error(`Record length cannot go over limit of ${RECORD_LENGTH_LIMIT} bytes.`)
            }

            const allBuffer = Buffer.alloc(RECORD_SIZE_SIZE + recordBuffer.length)
            allBuffer.writeInt32BE(recordBuffer.length, 0)
            allBuffer.set(recordBuffer, RECORD_SIZE_SIZE)

            await handle.write(allBuffer, 0, allBuffer.length, this.lastPosition)

            this.positionExchange.set(exchangeId, [this.lastPosition, allBuffer.length])

            this.lastPosition += allBuffer.length

            op.resolve()
        } catch (e) {
            op.reject(e)
        }
    }

    /**
     * Removes a heap segment's contents through the operation queue.
     * 
     * @param {*} id - Id of the record to be deleted.
     */
    remove(id) {
        if (!this.isLoaded) {
            throw new Error(`JSONHeapbase '${this.filepath}' is not loaded.`)
        }

        const posAndSize = this.positionExchange.get(id)
        if (posAndSize === undefined) {
            throw new Error(`Cannot delete non-existent record: ${id}`)
        }

        const self = this
        return new Promise (function (resolve, reject) {
            // create the operation entry
            const dataOp = {
                type: DATA_OPS.DELETE,
                resolve,
                reject,
                args: [...posAndSize, id]
            }

            // add to the queue
            self.opQueue.push(dataOp)

            // start the queue timeout, if not started
            if (!self.timeout) {
                self.setFlushTimeout()
            }
        })
    }

    /**
     * Takes a delete data operation and performs it against the JSONHeapbase.
     * 
     * @param {fs.promises.FileHandle} handle - Handle of the JSONHeapbase's persistence file.
     * @param {JSONDataOp} op - Operation to be performed.
     */
    async _delete(handle, op) {
        // get the position and size
        const position = op.args[0]
        const size = op.args[1]
        const id = op.args[2]

        const recordSizeBuffer = Buffer.alloc(RECORD_SIZE_SIZE)
        recordSizeBuffer.writeInt32BE(size * -1)
        
        try {
            await handle.write(recordSizeBuffer, 0, RECORD_SIZE_SIZE, position)
        } catch (e) {
            op.reject(e)
            return
        }
        
        this.positionExchange.delete(id)

        op.resolve()
    }

    /**
     * Retrieves a record from the JSONHeapbase.
     * 
     * @param {*} id - Id of the record to be retrieved.
     */
    async get(id) {
        if (!this.isLoaded) {
            throw new Error(`JSONHeapbase '${this.filepath}' is not loaded.`)
        }

        const posAndSize = this.positionExchange.get(id)
        if (posAndSize === undefined) {
            throw new Error(`Cannot retrieve non-existent record: ${id}`)
        }

        // prepare buffer to contain data
        const buffer = Buffer.alloc(posAndSize[1])

        // open the handle first
        var handle = await fsprom.open(this.filepath, `r+`)

        // perform all pending operations first
        handle = await this.flushOpQueue(handle)

        // get the serialized record
        const readOutput = await handle.read(buffer, 0, buffer.length, posAndSize[0] + RECORD_SIZE_SIZE)

        // close the file handle for no leaks
        await handle.close()

        // return the record unserialized
        return JSON.parse(readOutput.buffer.toString())
    }
}

module.exports = JSONHeapbase