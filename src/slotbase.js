const fs = require("fs")
const fsprom = fs.promises

const path = require("path")
const EventEmitter = require("events")

const { DATA_OPS, OPS_EXEC_DELAY, ERROR_EVENT, CACHE_LIMIT, HOLLOW_FUNC } = require("./constants")

/**
 * @typedef {import('./constants').DataOp} DataOp
 */

/**
 * Event emitted after the conclusion of a slotbase data operation.
 * 
 * @typedef SlotbaseDataEvent
 * @property {number} slotindex - Index of the slot that was operated on.
 * @property {*} id - Id to identify the slot.
 * @property {Error} err - Error object, if the data operation throws.
 */

/**
 * Event emitted if an operation which is asynchronous by virtue of timeout fails.
 * 
 * @typedef SlotbaseErrorEvent
 * @property {Error} err - Error object, if the data operation throws.
 */

/**
 * A set of options to be passed to a Slotbase.
 * 
 * @typedef SlotbaseOptions
 * @property {boolean} [isWasteful=false] - Flag signifying no reuse of removed slots. Default value is false.
 * @property {boolean} [isUnsafe=false] - Flag signifying unsafe handling of slots. Default value is false.
 * @property {number} [opDelay=10] - Number of milliseconds to lapse before emptying the operation queue. Default value is 10. Cannot be zero.
 */

/**
 * A file-based, delayed-op database that only holds fixed-size buffers.
 * 
 * @todo Catch errors properly on file operations, make it readable for user
 * @todo Do a backup system so that we can mitigate write crashes, and corruption from external factors
 * 
 * @extends {EventEmitter}
 */
class Slotbase extends EventEmitter {
    /**
     * Absolute path of the file used by the Slotbase as persistence medium.
     * 
     * @type {string}
     */
    filepath

    /**
     * List of slots that are free.
     * 
     * @type {number[]}
     */
    freeSlots

    /**
     * Size of a slot, in bytes.
     * 
     * @type {number}
     */
    slotSize

    /**
     * Count of slots, both free and occupied.
     * 
     * @type {number}
     */
    slotCount

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
     * @type {DataOp[]}
     */
    opQueue

    /**
     * Id of the timeout used to flush the operation queue.
     * 
     * @type {NodeJS.Timeout}
     */
    timeout

    /**
     * Flag signifying no reuse of space from removed slots. If set to true, the Slotbase will
     * not track slots that are free and will always allocate a new slot at the end.
     * 
     * @type {boolean}
     */
    isWasteful

    /**
     * Flag signifying unsafe handling of free slots. Free slots are normally zero-filled before
     * allocation. If set to true, the Slotbase will ignore this default behavior.
     * 
     * @type {boolean}
     */
    isUnsafe

    /**
     * Flag signifying readiness of the Slotbase. If set to true, the Slotbase has already
     * loaded its metadata and is ready for operations.
     * 
     * @type {boolean}
     */
    isLoaded

    /**
     * Creates a Slotbase. The constructor only creates an instance in memory; to load the
     * Slotbase's data, {@link Slotbase#load} should be called.
     * 
     * @param {string} filepath - Path to the persistence file for the Slotbase.
     * @param {number} slotSize - Size for a slot, in bytes.
     * @param {SlotbaseOptions} [options={}] - Options to customize the operations of the Slotbase.
     */
    constructor (filepath, slotSize, options = {}) {
        if (!path.isAbsolute(filepath)) {
            throw new Error("Cannot accept a relative filepath.")
        }

        super()

        // load the important values
        this.filepath = filepath
        this.slotSize = slotSize
        this.slotCount = 0

        // load operation-based values
        this.opDelay = options.opDelay || OPS_EXEC_DELAY
        this.opQueue = []

        // load the boolean values
        this.isWasteful = options.isWasteful || false
        this.isUnsafe = options.isUnsafe || false
        this.isLoaded = false

        // load the free slots queue
        this.freeSlots = []
    }

    /**
     * Loads pertinent information from persistence file, and sets up the
     * operation queue timeouts.
     * 
     */
    async load() {
        // open the file for slot count and free slot checking
        const handle = await fsprom.open(this.filepath, "a+")
        const stats = await handle.stat()

        // file size should be a multiple of slot size
        if (stats.size % this.slotSize !== 0) {
            throw new Error(`Slotbase '${this.filepath}' is corrupted.`)
        }

        // update slotcount
        this.slotCount = stats.size / this.slotSize

        // check out free slots, but only if not marked as unsafe
        if (!this.isUnsafe) {
            const zerofillBuffer = Buffer.alloc(this.slotSize)
            for (let x = 0; x < this.slotCount; x++) {
                const buffer = Buffer.alloc(this.slotSize)
                const slotpos = x * this.slotSize
                const output = await handle.read(buffer, 0, this.slotSize, slotpos)

                // if slot is zerofilled it is a free slot
                if (zerofillBuffer.equals(output.buffer)) {
                    this.freeSlots.push(x)
                }
            }
        }

        // close after everything is done
        await handle.close()

        // ready for action
        this.isLoaded = true
    }

    /**
     * Cleans the persistence file from all free slots.
     * 
     */
    async cleanup() {
        if (!this.isLoaded) {
            throw new Error(`Slotbase '${this.filepath}' is not loaded.`)
        }

        // cant clean if there's nothing to clean
        if (this.freeSlots.length === 0) {
            return
        }
        
        const handle = await fsprom.open(this.filepath, "r+")

        /** 
         * Map of previous slotindex to new slotindex 
         * 
         * @type {Map<number, number>}
         */
        const movedIndexes = new Map()

        // container for slots ready to be written to
        const readySlots = []

        // try to find all legit entries and collect them
        // try not to load all buffers into memory as this will take a toll on the computer
        const freeSlotsSet = new Set(this.freeSlots)
        for (let x = 0; x < this.slotCount; x++) {
            if (freeSlotsSet.has(x)) {
                readySlots.push(x)
            } else {
                if (readySlots.length > 0) {
                    // read the slot to be moved
                    const slotpos = x * this.slotSize
                    const buffer = Buffer.alloc(this.slotSize)
                    const slotToMove = await handle.read(buffer, 0, this.slotSize, slotpos)

                    // write it to a ready slot
                    const readySlotIndex = readySlots.shift()
                    const readySlotPos = readySlotIndex * this.slotSize
                    await handle.write(slotToMove.buffer, 0, this.slotSize, readySlotPos)

                    // note the moved index's new index
                    movedIndexes.set(x, readySlotIndex)

                    // push the recently moved slot's previous index to readySlots
                    readySlots.push(x)
                }
            }
        }

        // calculate new slot count
        this.slotCount = this.slotCount - this.freeSlots.length

        // clean the file for reals
        const newSize = this.slotCount * this.slotSize
        await handle.truncate(newSize)

        await handle.close()

        // empty free slots list
        this.freeSlots = []

        return movedIndexes
    }

    /**
     * Sets the timeout for data operation queue flushing.
     * 
     */
    setFlushTimeout() {
        var self = this
        this.timeout = setTimeout(async function () {
            // perform operations then set another timeout after
            await self.flushOpQueue(null, true)
            self.timeout = null
        }, this.opDelay)
    }

    /**
     * Performs the data operations queued in opQueue.
     * 
     * @param {fs.promises.FileHandle} [handle] - Handle of the Slotbase's persistence file. Can be passed from a parent operation.
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
                handle = await fsprom.open(this.filepath, "r+")
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

            let err, eventName, eventObj, opId
            try {
                // perform the data operation
                if (dataOp.type === DATA_OPS.INSERT) {
                    opId = dataOp.id
                    eventName = dataOp.type
                    eventObj = await this._insert(handle, dataOp)
                } else if (dataOp.type === DATA_OPS.DELETE) {
                    opId = dataOp.id
                    eventName = dataOp.type
                    eventObj = await this._delete(handle, dataOp)
                } else if (dataOp.type === DATA_OPS.UPDATE) {
                    opId = dataOp.id
                    eventName = dataOp.type
                    eventObj = await this._update(handle, dataOp)
                }
            } catch (e) {
                // catch error to emit
                err = e

                // NOTE: should check if error is something about the file being inaccessible
                // to avoid executing all data ops, just emit errors for all of them in this case
            } finally {
                if (eventName) {
                    // emit the event
                    eventObj = eventObj || {}
                    eventObj.id = opId
                    eventObj.err = err
                    this.emit(eventName, eventObj)
                }
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
     * Inserts a data buffer into the Slotbase through the operation queue.
     * 
     * @param {Buffer} slotdata - Data to be inserted.
     * @param {*} id - Value to identify the slotdata on insert event.
     */
    add(slotdata, id) {
        if (!this.isLoaded) {
            throw new Error(`Slotbase '${this.filepath}' is not loaded.`)
        }

        if (!(slotdata instanceof Buffer)) {
            throw new Error(`Cannot insert data types other than type Buffer.`)
        }

        if (slotdata.byteLength !== this.slotSize) {
            throw new Error(`Cannot insert data bigger than the slot size '${this.slotSize}'`)
        }

        // create the operation entry
        const dataOp = {
            id,
            type: DATA_OPS.INSERT,
            args: [slotdata]
        }

        // add to the queue
        this.opQueue.push(dataOp)

        // start the queue timeout, if not started
        if (!this.timeout) {
            this.setFlushTimeout()
        }
    }

    /**
     * Takes a insert data operation and performs it against the Slotbase.
     * 
     * @param {fs.promises.FileHandle} handle - Handle of the Slotbase's persistence file.
     * @param {DataOp} op - Operation to be performed.
     * 
     * @returns {SlotbaseDataEvent}
     */
    async _insert(handle, op) {
        if (op.type !== DATA_OPS.INSERT) {
            throw new Error(`Cannot perform a data operation other than INSERT.`)
        }

        // get free slotindex
        var slotindex, didNotReuse
        if (this.isWasteful || (this.freeSlots.length === 0)) {
            slotindex = this.slotCount
            didNotReuse = true
        } else {
            slotindex = this.freeSlots.pop()
        }

        // get the slot position
        const slotpos = slotindex * this.slotSize

        // get the slotdata
        const slotdata = op.args[0]

        // perform the insertion
        await handle.write(slotdata, 0, slotdata.byteLength, slotpos)

        // increment the slot count if we did not reuse slots
        if (didNotReuse) {
            this.slotCount++
        }

        // return the event
        return { slotindex }
    }

    /**
     * Removes a slot's contents through the operation queue.
     * 
     * @param {number} slotindex - Index of the slot to be deleted.
     * @param {*} id - Value to identify the slot on delete event.
     */
    remove(slotindex, id) {
        if (!this.isLoaded) {
            throw new Error(`Slotbase '${this.filepath}' is not loaded.`)
        }

        const cannotRemove = this.slotCount <= slotindex || this.freeSlots.includes(slotindex)
        if (cannotRemove) {
            throw new Error(`Cannot remove a slot that doesn't have data.`)
        }

        // create the operation entry
        const dataOp = {
            id,
            type: DATA_OPS.DELETE,
            args: [slotindex]
        }

        // add to the queue
        this.opQueue.push(dataOp)

        // start the queue timeout, if not started
        if (!this.timeout) {
            this.setFlushTimeout()
        }
    }

    /**
     * Takes a delete data operation and performs it against the Slotbase.
     * 
     * @param {fs.promises.FileHandle} handle - Handle of the Slotbase's persistence file.
     * @param {DataOp} op - Operation to be performed.
     * 
     * @returns {SlotbaseDataEvent}
     */
    async _delete(handle, op) {
        if (op.type !== DATA_OPS.DELETE) {
            throw new Error(`Cannot perform a data operation other than DELETE.`)
        }

        // get the slot position
        const slotpos = op.args[0] * this.slotSize

        // zero-fill the buffer if not unsafe
        if (!this.isUnsafe) {
            // prepare the buffer
            const zerofillBuffer = Buffer.alloc(this.slotSize)

            // perform the zero-filling
            await handle.write(zerofillBuffer, 0, zerofillBuffer.byteLength, slotpos)
        }

        // add the deleted slot's index to freeSlots
        this.freeSlots.push(slotindex)

        // return the event
        return { slotindex }
    }

    /**
     * Updates a slot's contents through the operations queue.
     * 
     * @param {number} slotindex - Index of the slot to be updated.
     * @param {Buffer} slotdata - Data used to update the slot.
     * @param {number} offset - Index of slotdata from where to start writing.
     * @param {number} length - Length of slotdata to write to the slot.
     * @param {*} id - Value to identify the slot on update event.
     */
    put(slotindex, slotdata, offset, length, id) {
        if (!this.isLoaded) {
            throw new Error(`Slotbase '${this.filepath}' is not loaded.`)
        }

        const cannotUpdate = this.slotCount <= slotindex || this.freeSlots.includes(slotindex)
        if (cannotUpdate) {
            throw new Error(`Cannot update a slot that doesn't have data.`)
        }

        if (!(slotdata instanceof Buffer)) {
            throw new Error(`Cannot update using data types other than type Buffer.`)
        }

        const writeSize = offset + length
        if (writeSize > this.slotSize) {
            throw new Error(`Cannot update using data larger than the slot size '${this.slotSize}'`)
        }

        // create the operation entry
        const dataOp = {
            id,
            type: DATA_OPS.UPDATE,
            args: [slotindex, slotdata, offset, length]
        }

        // add to the queue
        this.opQueue.push(dataOp)

        // start the queue timeout, if not started
        if (!this.timeout) {
            this.setFlushTimeout()
        }
    }

    /**
     * Takes an update data operation and performs it against the Slotbase.
     * 
     * @param {fs.promises.FileHandle} handle - Handle of the Slotbase's persistence file.
     * @param {DataOp} op - Operation to be performed.
     * 
     * @returns {SlotbaseDataEvent}
     */
    async _update(handle, op) {
        if (!this.isLoaded) {
            throw new Error(`Slotbase '${this.filepath}' is not loaded.`)
        }

        if (op.type !== DATA_OPS.UPDATE) {
            throw new Error(`Cannot perform a data operation other than UPDATE.`)
        }

        // get the slot position
        const slotpos = op.args[0] * this.slotSize

        // get the slotdata
        const slotdata = op.args[1]

        // get the write position
        const writepos = slotpos + op.args[2]

        // get the write length
        const writelength = op.args[3]

        // perform the update
        await handle.write(slotdata, 0, writelength, writepos)

        // return the event
        return { slotindex }
    }

    /**
     * Selects a slot by its index and returns its data. Empties the operation queue
     * before getting the data. 
     * 
     * @param {number} slotindex - Index of the slot to be selected.
     */
    async get(slotindex) {
        if (!this.isLoaded) {
            throw new Error(`Slotbase '${this.filepath}' is not loaded.`)
        }

        // prepare buffer to contain data
        const buffer = Buffer.alloc(this.slotSize)

        // get the slot position
        const slotpos = slotindex * this.slotSize

        // open the handle first
        var handle = await fsprom.open(this.filepath, "r+")

        // perform all pending operations first
        handle = await this.flushOpQueue(handle)

        // get the slotdata
        const data = await handle.read(buffer, 0, this.slotSize, slotpos)

        // close the file handle for no leaks
        await handle.close()

        // just return the buffer, no need to truncate
        return data.buffer
    }

    /**
     * Selects a slot by its index and returns its data. The difference from {@link Slotbase#get}
     * is that this method doesn't empty the operation queue before getting the data.
     * 
     * @param {number} slotindex - Index of the slot to be selected.
     */
    async getNow(slotindex) {
        if (!this.isLoaded) {
            throw new Error(`Slotbase '${this.filepath}' is not loaded.`)
        }

        // prepare buffer to contain data
        const buffer = Buffer.alloc(this.slotSize)

        // get the slot position
        const slotpos = slotindex * this.slotSize

        // get the slotdata
        const handle = await fsprom.open(this.filepath, "r")
        const data = await handle.read(buffer, 0, this.slotSize, slotpos)
        await handle.close()

        // just return the buffer, no need to truncate
        return data.buffer
    }
    
    /**
     * Returns an iterator that fetches all or some slots of a Slotbase.
     * 
     * @param {number[]} [slotindexes] - Indexes to fetch. If not set, the iterator fetches all.
     */
    async iterate(slotindexes) {
        var handle = await fsprom.open(this.filepath, "r+")

        // perform all pending operations first
        handle = await this.flushOpQueue(handle)

        const self = this
        const iterator = {
            handle,
            currIndex: 0,
            next: async function () {
                const slotindex = slotindexes ? slotindexes[this.currIndex] : this.currIndex
                const isDone = slotindex === undefined || self.slotCount <= slotindex
                const canOutput = isDone && !self.freeSlots.includes(slotindex)

                var value
                if (canOutput) {
                    const buffer = Buffer.alloc(self.slotSize)
                    const slotpos = slotindex * self.slotSize
                    value = (await this.handle.read(buffer, 0, self.slotSize, slotpos)).buffer
                }

                this.currIndex++

                return {
                    done: isDone,
                    index: slotindex,
                    value,
                }
            },
            return: async function () {
                await this.handle.close()
            }
        }

        return iterator
    }

    /**
     * Check if certain slot is occupied through its slotindex.
     * 
     */
    isOccupied(slotindex) {
        return this.freeSlots.includes(slotindex)
    }

    /**
     * Flag that signifies if the Slotbase has any operations to perform or not.
     * 
     */
    get isIdle() {
        return this.opQueue.length === 0
    }
}

/**
 * An enum of data types for slot segments.
 * 
 * @readonly
 * @enum {string}
 */
const SLOT_SEGMENT_TYPES = {
    NUMBER: `NUMBER`,
    STRING: `STRING`,
    BUFFER: `BUFFER`
}

/**
 * Entry to the rollback map, contains pertinent info used for the rollback
 * 
 * @typedef RollbackEntry
 * @property {*} id - Id of the data to rollback.
 * @property {SlotRecord} record - Record used for the rollback.
 * @property {number} index - Index used for the rollback.
 */

/**
 * Record that is convertible to a slot.
 * 
 * @typedef {Object.<string, *>} SlotRecord
 */

/**
 * Function that serializes a record field into a slot segment of type Buffer.
 * 
 * @callback SegmentSerializer
 * @param {*} field - Field data to serialize.
 * @returns {Buffer}
 */

/**
 * Function that deserializes a segment buffer into a record field.
 * 
 * @callback SegmentDeserializer
 * @param {Buffer} segment - Segment to deserialize.
 */

/**
 * Object that describes the schema of a segment in a slot.
 * 
 * @typedef SlotSegmentSchema
 * @property {string} name - Name of the segment.
 * @property {number} pos - Position of the segment in the slot.
 * @property {number} size - Size of the segment.
 * @property {boolean} isId - Flag signifying that the segment of the schema is treated as the id of the record.
 * @property {SegmentSerializer} serialize - Serializes a field into a segment buffer.
 * @property {SegmentDeserializer} deserialize - Deserializes a segment buffer into a field.
 */

/**
 * Wrapper that provides basic schema capabilities to a Slotbase.
 * 
 */
class SlotbaseManager extends EventEmitter {
    /**
     * Slotbase to be managed.
     * 
     * @type {Slotbase}
     */
    slotbase

    /**
     * Map of slot segment schemas.
     * 
     * @type {Map<string, SlotSegmentSchema>}
     */
    segmentSchemas

    /**
     * Schema of the segment that identifies a slot record in the Slotbase.
     * 
     * @type {SlotSegmentSchema}
     */
    idSchema

    /**
     * Map used to exchange a record id for its equivalent slotindex.
     * 
     * @type {Map<any, number>}
     */
    idExchange

    /**
     * Cache map of slot records, operating in an LRU fashion.
     * 
     * @type {Map<any, { record: SlotRecord, isGuarded: boolean }>}
     */
    recordCache

    /**
     * Limit of the record cache.
     * 
     * @type {number}
     */
    cacheLimit

    /**
     * Map of records to be rolled back in case of a write error.
     * 
     * @type {Map<any, RollbackEntry[]>}
     */
    rollbackMap

    /**
     * Map of resolve functions from data operation calls keyed by their corresponding record ids. Organized per data operation type.
     * 
     * @type {Map<string, Map<any, ((value:any) => void)[]>>}
     */
    resolveMap

    /**
     * Map of reject functions from data operation calls keyed by their corresponding record ids. Organized per data operation type.
     * 
     * @type {Map<string, Map<any, ((err: any) => void)[]>>}
     */
    rejectMap

    /**
     * Creates a SlotbaseManager.
     * 
     * @param {Slotbase} slotbase - Slotbase to be managed.
     * @param {SlotSegmentSchema[]} schemas - List of segment schemas to be used by the manager.
     * @param {number} [limit] - Limit of records that can be stored in the cache. Default is 1000.
     */
    constructor (slotbase, schemas, limit = CACHE_LIMIT) {
        super()

        this.slotbase = slotbase
        this.segmentSchemas = schemas
        this.recordCache = new Map()
        this.rollbackMap = new Map()
        this.idExchange = new Map()
        this.cacheLimit = limit
        
        this.resolveMap = new Map()
        this.resolveMap.set(DATA_OPS.INSERT, new Map())
        this.resolveMap.set(DATA_OPS.DELETE, new Map())
        this.resolveMap.set(DATA_OPS.UPDATE, new Map())

        this.rejectMap = new Map()
        this.rejectMap.set(DATA_OPS.INSERT, new Map())
        this.rejectMap.set(DATA_OPS.DELETE, new Map())
        this.rejectMap.set(DATA_OPS.UPDATE, new Map()) 
    }

    /**
     * Loads the manager, and the Slotbase if it is not yet loaded.
     * 
     */
    async load() {
        if (!(this.slotbase instanceof Slotbase)) {
            throw new Error(`Passed slotbase is not valid.`)
        }

        if (this.segmentSchemas.size === 0) {
            throw new Error(`Cannot load a SlotbaseManager without schemas.`)
        }

        // load slotbase first
        if (!this.slotbase.isLoaded) {
            await this.slotbase.load()
        }

        // validate schemas
        var totalSchemaSize = 0
        const schemaIterator = this.segmentSchemas.values()
        while (true) {
            const { done, value } = schemaIterator.next()

            if (done) {
                break
            }

            // a field's size should not be zero
            if (value.size === 0) {
                throw new Error(`Segment schemas cannot have their sizes set to zero: ${value.name}`)
            }

            // a field's position must be within the slot size
            if (value.pos < 0 || value.pos > (this.slotbase.slotSize - 1)) {
                throw new Error(`Segment schemas must have their poses within the Slotbase's slot size: ${value.name}`)
            }

            // set hollow functions for serialize and deserialize
            if (value.deserialize === undefined) {
                value.deserialize = HOLLOW_FUNC
            }

            if (value.serialize === undefined) {
                value.serialize = HOLLOW_FUNC
            }

            // compute total schema size
            totalSchemaSize += value.size

            if (value.isId) {
                this.idSchema = value
            }
        }

        // total schema size should equal slot size
        if (this.slotbase.slotSize !== totalSchemaSize) {
            throw new Error(`Schemas must match the Slotbase's slot size.`)
        }

        if (this.idSchema === undefined) {
            throw new Error(`SlotbaseManager requires an id segment schema.`)
        }

        // load record ids from slotbase here
        const slotIterator = await this.slotbase.iterate()
        while (true) {
            const { done, value, index } = await slotIterator.next()

            if (done) {
                await slotIterator.return()
                break
            }

            if (value === undefined) {
                continue
            }

            // populate the id exchange
            const idSegment = value.slice(this.idSchema.pos, this.idSchema.size)
            const id = this.idSchema.deserialize(idSegment)
            this.idExchange.set(id, index)
        }
        
        // catch events emitted by the slotbase, for data integrity
        const self = this
        this.slotbase.on(DATA_OPS.INSERT, function (event) {
            if (event.err) {
                // rollback in case of an error
                if (self.recordCache.has(event.id)) {
                    self.recordCache.delete(event.id)
                }

                // let user know also by calling the appropriate reject
                self.rejectOp(event.id, DATA_OPS.INSERT, event.err)
                return
            }

            // set up the unguarding of cache records
            // also save the slotindex into the id exchange
            self.idExchange.set(event.id, event.slotindex)
            self.unguardCacheRecord(event.id)
            
            // resolve the corresponding data operation
            self.resolveOp(event.id, DATA_OPS.INSERT, event.slotindex)
        })

        this.slotbase.on(DATA_OPS.DELETE, function (event) {
            if (event.err) {
                // rollback in case of an error, if there's anything to rollback
                if (self.rollbackMap.has(event.id)) {
                    const rollbackEntry = self.rollbackMap.get(event.id).shift()

                    if (rollbackEntry.record !== undefined) {
                        // push to cache instead, we already removed it from the cache
                        self.pushToCache(event.id, rollbackEntry.record, false)
                    }

                    if (self.rollbackMap.get(event.id).length === 0) {
                        self.rollbackMap.delete(event.id)
                    }

                    // rollback in the id exchange
                    self.idExchange.set(event.id, rollbackEntry.index)

                    // let the user know about the error
                    self.rejectOp(event.id, DATA_OPS.DELETE, event.err)

                    return
                } else {
                    throw new Error(`Did not rollback on error, DELETE data op, data id ${event.id}`)
                }
            }

            // resolve the corresponding data operation
            self.resolveOp(event.id, DATA_OPS.DELETE, event.slotindex)
        })

        this.slotbase.on(DATA_OPS.UPDATE, function (event) {
            if (event.err) {
                // rollback in case of an error
                if (self.rollbackMap.has(event.id)) {
                    const rollbackEntry = self.rollbackMap.get(event.id).shift()

                    if (rollbackEntry.record !== undefined) {
                        self.recordCache.set(event.id, rollbackEntry.record)
                    }

                    if (self.rollbackMap.get(event.id).length === 0) {
                        self.rollbackMap.delete(event.id)
                    }

                    // rollback in the id exchange
                    self.idExchange.set(event.id, rollbackEntry.index)

                    // let the user know about the error
                    self.rejectOp(event.id, DATA_OPS.DELETE, event.err)

                    return
                } else {
                    throw new Error(`Did not rollback on error, UPDATE data op, data id ${event.id}`)
                }
            }

            // resolve the corresponding data operation
            self.resolveOp(event.id, DATA_OPS.UPDATE, event.slotindex)
        })
        
        this.slotbase.on(ERROR_EVENT, function (event) {
            self.emit(ERROR_EVENT, event)
        })
    }

    /**
     * Prepare for the eventual resolution of a data operation by returning a promise and
     * saving its resolve and reject functions for later when the operation has concluded.
     * 
     * @param {*} id - Id used to identify the data operation.
     * @param {string} opType - Type of the data operation.
     */
    prepareOpResolution(id, opType) {
        const self = this
        return new Promise (function (resolve, reject) {
            // save the resolve function
            if (self.resolveMap.get(opType).has(id)) {
                self.resolveMap.get(opType).get(id).push(resolve)
            } else {
                self.resolveMap.get(opType).set(id, [resolve])
            }

            // save the reject function
            if (self.rejectMap.get(opType).has(id)) {
                self.rejectMap.get(opType).get(id).push(reject)
            } else {
                self.rejectMap.get(opType).set(id, [reject])
            }
        })
    }

    /**
     * Resolve a pending data operation call.
     * 
     * @param {*} id - Id used to identify the data operation.
     * @param {string} opType - Type of the data operation.
     * @param  {...any} args - Additional arguments to pass to the resolve function.
     */
    resolveOp(id, opType, ...args) {
        // remove the counterpart reject function first
        const rejectQueue = this.rejectMap.get(opType).get(id)
        rejectQueue.shift()
        if (rejectQueue.length === 0) {
            this.rejectMap.get(opType).delete(id)
        }

        const resolveQueue = this.resolveMap.get(opType).get(id)
        const resolve = resolveQueue.shift()
        if (resolveQueue.length === 0) {
            this.resolveMap.get(opType).delete(id)
        }

        resolve(args)
    }

    /**
     * Reject a pending data operation call.
     * 
     * @param {*} id - Id used to identify the data operation.
     * @param {string} opType - Type of the data operation.
     * @param {*} err - Error argument to pass to the reject function.
     */
    rejectOp(id, opType, err) {
        // remove the counterpart resolve function first
        const resolveQueue = this.resolveMap.get(opType).get(id)
        resolveQueue.shift()
        if (resolveQueue.length === 0) {
            this.resolveMap.get(opType).delete(id)
        }

        const rejectQueue = this.rejectMap.get(opType).get(id)
        const reject = rejectQueue.shift()
        if (rejectQueue.length === 0) {
            this.rejectMap.get(opType).delete(id)
        }

        reject(err)
    }

    /**
     * Serializes a slot record. Returns the slot buffer if successfully built;
     * otherwise, returns undefined.
     * 
     * @param {SlotRecord} record - Record to serialize.
     */
    serializeRecord(record) {
        const slotdata = Buffer.alloc(this.slotbase.slotSize)

        // iterate record schema, to write to slotdata
        const schemaIterator = this.segmentSchemas.values()
        while (true) {
            const { done, value } = schemaIterator.next()

            if (done) {
                break
            }

            // get the data from the record
            const field = record[value.name]

            // serialize the data according to schema
            const segment = value.serialize(field)

            // write to buffer
            slotdata.set(segment, value.pos)
        }

        return slotdata
    }

    /**
     * Deserializes a slot buffer. Returns the slot record parsed from
     * the buffer.
     * 
     * @param {Buffer} slot - Slot to be deserialized.
     * 
     * @returns {SlotRecord}
     */
    deserializeSlot(slot) {
        const record = new Map()

        const schemaIterator = this.segmentSchemas.values()
        while (true) {
            const { done, value } = schemaIterator.next()

            if (done) {
                break
            }

            // slice the segment buffer off the slot
            const segment = slot.slice(value.pos, value.size)

            // deserialize the segment
            const field = value.deserialize(segment)

            record[value.name] = field
        }

        return record
    }

    /**
     * Shifts the record cache to make space. Returns true if was able to shift.
     * 
     */
    shiftCache() {
        // prepare record key holder
        let keyToShift

        // check what entry can be shifted
        const cacheIterator = this.recordCache.keys()
        while (true) {
            const { done, value } = cacheIterator.next()

            if (done) {
                break
            }

            const cacheEntry = this.recordCache.get(value)
            if (!cacheEntry.isGuarded) {
                keyToShift = value
                break
            }
        }

        if (keyToShift) {
            this.recordCache.delete(keyToShift)
            return true
        }

        return false
    }

    /**
     * Pushes a record to the cache, applying a guard so that it won't be shifted out. Returns true
     * if the record was pushed successfully. Otherwise, returns false.
     * 
     * @param {*} id - Id of the cache entry to be pushed.
     * @param {SlotRecord} record - Record to be pushed to the cache.
     * @param {boolean} [shouldGuard=true] - Flag signifying if record should be guarded. Default is true.
     */
    pushToCache(id, record, shouldGuard = true) {
        let isSuccess = true

        // shift the cache if full
        if (this.recordCache.size <= this.cacheLimit) {
            isSuccess = this.shiftCache()
        }

        // if was able to shift the cache if full
        if (isSuccess) {
            this.recordCache.set(id, { isGuarded: shouldGuard, record })
        }

        return isSuccess
    }

    /**
     * Gets a record from the cache.
     * 
     * @param {*} id - Id of the cache entry to be retrieved.
     */
    getFromCache(id) {
        const output = this.recordCache.get(id)

        // inserts back to recordCache to set it as newest
        this.recordCache.delete(output)
        this.recordCache.set(id, output)

        return output.record
    }

    /**
     * Unguards a cache entry so that it can be shifted out of the cache.
     * 
     * @param {*} Id - Id of the cache entry to be unguarded.
     */
    unguardCacheRecord(id) {
        this.recordCache.has(id) && (this.recordCache.get(id).isGuarded = false)
    }

    /**
     * Adds a record to the Slotbase.
     * 
     * @param {SlotRecord} record - Record to be added.
     */
    add(record) {
        // get the id first and verify if existing
        const id = record[this.idSchema.name]
        if (this.idExchange.has(id)) {
            throw new Error(`Record of id ${id} is already found in the Slotbase.`)
        }

        // serialize into buffer first
        const slotdata = this.serializeRecord(record)

        // now add to slotbase
        this.slotbase.add(slotdata, id)

        // save to cache while insert is ongoing
        this.pushToCache(id, record)

        return this.prepareOpResolution(id, DATA_OPS.INSERT)
    }

    /**
     * Removes a record from the Slotbase.
     * 
     * @param {*} id - Id of the record to be removed.
     */
    remove(id) {
        const slotindex = this.idExchange.get(id)
        if (slotindex === undefined) {
            throw new Error(`Record of id ${id} cannot be found in the Slotbase.`)
        }

        // prepare the rollback entry
        const rollbackEntry = {
            record: undefined, 
            id, 
            index: slotindex
        }

        // delete in the cache, if there is
        if (this.recordCache.has(id)) {
            // make sure to prepare for rollback
            const cacheRecord = this.recordCache.get(id).record
            rollbackEntry.record = cacheRecord

            this.recordCache.delete(id)
        }

        // save rollback entry
        if (this.rollbackMap.has(id)) {
            this.rollbackMap.get(id).push(rollbackEntry)
        } else {
            this.rollbackMap.set(id, [rollbackEntry])
        }

        // remove now from the slotbase
        this.slotbase.remove(slotindex, id)

        this.idExchange.delete(id)

        return this.prepareOpResolution(id, DATA_OPS.DELETE)
    }

    /**
     * Updates a record in the Slotbase.
     * 
     * @param {*} id - Id of the record to be updated.
     * @param {SlotRecord} record - Record which is the basis for the update.
     */
    update(id, record) {
        const slotindex = this.idExchange.get(id)
        if (slotindex === undefined) {
            throw new Error(`Record of id ${id} cannot be found in the Slotbase.`)
        }

        // prepare the rollback entry
        const rollbackEntry = {
            record: undefined, 
            id, 
            index: slotindex
        }

        // get cache entry if any, and prepare for rollback
        if (this.recordCache.has(id)) {
            const cacheRecord = this.recordCache.get(id).record
            rollbackEntry.record = cacheRecord

            this.recordCache.set(id, record)
        }

        // save rollback entry
        if (this.rollbackMap.has(id)) {
            this.rollbackMap.get(id).push(rollbackEntry)
        } else {
            this.rollbackMap.set(id, [rollbackEntry])
        }

        // serialize into buffer first
        const slotdata = this.serializeRecord(record)
        
        // update the slotbase
        this.slotbase.put(slotindex, slotdata, 0, slotdata.length, id)

        return this.prepareOpResolution(id, DATA_OPS.DELETE)
    }

    /**
     * Gets a record from the Slotbase.
     * 
     * @param {*} id - Id of the record to be fetched.
     */
    async get(id) {
        // look in the cache first
        if (this.recordCache.has(id)) {
            return this.getFromCache(id)
        }

        const slotindex = this.idExchange.get(id)
        if (slotindex !== undefined) {
            // get the slot from the slotbase
            const slot = await this.slotbase.get(slotindex)

            // deserialize slot into record and save to cache
            const record = this.deserializeSlot(slot)
            this.pushToCache(id, record, false)

            return record
        }
    }
}

module.exports = {
    Slotbase,
    SlotbaseManager
}