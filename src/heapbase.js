const fs = require("fs")
const fsprom = fs.promises

const path = require("path")
const EventEmitter = require("events")

const { DATA_OPS, FILEBASE_ERRS, OPS_EXEC_DELAY, CACHE_LIMIT, HOLLOW_FUNC, ERROR_EVENT } = require("./constants")

/**
 * Maximum number of bytes to handle during heap cleanup.
 * 
 */
const HEAP_CLEANUP_MEM_MAX = 1024 * 100

/**
 * @typedef {import('./constants').DataOp} DataOp
 */

/**
 * Event emitted after the conclusion of a heapbase data operation.
 * 
 * @typedef HeapbaseDataEvent
 * @property {number} heapindex - Index of the heap segment that was operated on.
 * @property {*} id - Id to identify the heap segment.
 * @property {Error} err - Error object, if the data operation throws.
 */

/**
 * Event emitted if an operation which is asynchronous by virtue of timeout fails.
 * 
 * @typedef HeapbaseErrorEvent
 * @property {Error} err - Error object, if the data operation throws.
 */

/**
 * A set of options to be passed to a Heapbase.
 * 
 * @typedef HeapbaseOptions
 * @property {boolean} [isWasteful=false] - Flag signifying no reuse of removed segments. Default value is false.
 * @property {boolean} [isUnsafe=false] - Flag signifying unsafe handling of segments. Default value is false.
 * @property {number} [opDelay=10] - Number of milliseconds to lapse before emptying the operation queue. Default value is 10. Cannot be zero.
 */

/**
 * Information about a certain segment in a Heapbase.
 * 
 * @typedef HeapIndex
 * @property {number} pos - Position of the segment in the Heapbase.
 * @property {number} size - Size of the segment in bytes.
 */

/**
 * A file-based, delayed-op database that holds dynamic-size buffers.
 * 
 * @extends {EventEmitter}
 */
class Heapbase extends EventEmitter {
    /**
     * Absolute path of the file used by the Heapbase as persistence medium.
     * 
     * @type {string}
     */
    filepath

    /**
     * Mapped queues of the poses of segments that are free, keyed by the segment's length.
     * 
     * @type {Map<number, Set<number>>}
     */
    freeSegmentQueues
    
    /**
     * Map of segments that are free, keyed by the segment's pos.
     * 
     * @type {Map<number, HeapIndex>}
     */
    freeSegments

    /**
     * Mapped list of segments that are occupied, keyed by the segment's pos.
     * 
     * @type {Map<number, HeapIndex>}
     */
    occupiedSegments

    /**
     * Size of the largest available segment in Heapbase, in bytes.
     * 
     * @type {number}
     */
    ceilSegmentSize

    /**
     * Current size of the heap, including both free and occupied segments.
     * 
     * @type {number}
     */
    heapSize

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
     * Flag signifying no reuse of segments from removed buffers. If set to true, the Heapbase will
     * not track segments that are free and will always allocate a new segment at the end. This might
     * increase write speed, but will mean poorly for disk space.
     * 
     * @type {boolean}
     */
    isWasteful

    /**
     * Flag signifying unsafe handling of free segments. Free segments are normally zero-filled before
     * allocation. If set to true, the Heapbase will ignore this default behavior.
     * 
     * @type {boolean}
     */
    isUnsafe

    /**
     * Flag signifying readiness of the Heapbase. If set to true, the Heapbase has already
     * loaded its metadata and is ready for operations.
     * 
     * @type {boolean}
     */
    isLoaded

    /**
     * Error status of the Heapbase.
     * 
     * @type {number}
     */
    errorStatus

    /**
     * Creates a Heapbase. The constructor only creates an instance in memory; to load the
     * Heapbase's data, {@link Heapbase#load} should be called.
     * 
     * @param {string} filepath - Path to the persistence file for the Heapbase.
     * @param {HeapbaseOptions} [options={}] - Options to customize the operations of the Heapbase.
     */
    constructor (filepath, options = {}) {
        if (!path.isAbsolute(filepath)) {
            throw new Error("Cannot accept a relative filepath.")
        }

        super()

        // load the important values
        this.filepath = filepath
        this.heapSize = 0
        this.ceilSegmentSize = 0

        // load operation-based values
        this.opDelay = options.opDelay || OPS_EXEC_DELAY
        this.opQueue = []

        // load the boolean values
        this.isWasteful = options.isWasteful || false
        this.isUnsafe = options.isUnsafe || false
        this.isLoaded = false

        // load the free segments and occupied segments list
        this.freeSegmentQueues = new Map()
        this.freeSegments = new Map()
        this.occupiedSegments = new Map()

        // record error status
        this.errorStatus = FILEBASE_ERRS.NONE
    }

    /**
     * Loads the Heapbase. During loading, occupied and free segment is recorded using a list of indexes.
     * 
     * @param {HeapIndex[]} occupiedSegments - List of HeapIndexes indicating which segments are occupied.
     */
    async load(occupiedSegments) {
        const isIndexlistFilled = Array.isArray(occupiedSegments) && occupiedSegments.length !== 0

        // get the file stats
        const handle = await fsprom.open(this.filepath, "a+")
        const stats = await handle.stat()
        await handle.close()

        // no point in loading any further without the indexes in a non-empty file
        if (!isIndexlistFilled && stats.size !== 0) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot load the Heapbase without a heapindex list if persistence file has contents. File size: ${stats.size}`)
        }

        // only load occupied segments if the file has contents
        if (stats.size !== 0) {
            // sort heapindex list first, so that we have the first positions first
            // NOTE: do a quicksort here if we have time
            const sortedOccupiedSegments = occupiedSegments.sort((a, b) => a.pos - b.pos)

            // evaluate heapindex list        
            var loadPos = 0
            for (let x = 0; x < sortedOccupiedSegments.length; x++) {
                const heapindex = sortedOccupiedSegments[x]
                
                // if heapindex size is not less than file size, the file is corrupted
                if (stats.size < (heapindex.pos + heapindex.size)) {
                    this.errorStatus = FILEBASE_ERRS.CORRUPTION
                    throw new Error(`Heapbase '${this.filepath}' is corrupted. Heapindex list did not match the file size.`)
                }

                // if current heapindex pos is not the current load position
                // we have found a free segment, save it
                if (heapindex.pos !== loadPos) {
                    const freeSegmentIndex = {
                        pos: loadPos,
                        size: heapindex.pos - loadPos
                    }
                    this.trackFreeSegment(freeSegmentIndex)
                }

                // update load position
                loadPos = heapindex.pos + heapindex.size
            }

            // check if we have empty segment after, save if there is
            if (loadPos < stats.size) {
                const freeSegmentIndex = {
                    pos: loadPos,
                    size: stats.size - loadPos
                }
                this.trackFreeSegment(freeSegmentIndex)
            }

            // save to occupiedSegments
            this.occupiedSegments = sortedOccupiedSegments
        }

        // save heap size
        this.heapSize = stats.size
        
        // ready for action
        this.isLoaded = true
    }

    /**
     * Cleans the persistence file from all free segments.
     * 
     */
    async cleanup() {
        if (!this.isLoaded) {
            this.errorStatus = FILEBASE_ERRS.UNLOADED
            throw new Error(`Heapbase '${this.filepath}' is not loaded.`)
        }

        // cant clean if there's nothing to clean
        if (this.freeSegmentQueues.size === 0) {
            return
        }
        
        // map of previous heapindex pos to new heapindex
        const movedIndexes = new Map()

        // map of new heapindex pos to new heapindex
        const newOccupiedSegments = new Map()

        /**
         * Container for segments ready to be transferred.
         * 
         * @type {[HeapIndex, Buffer][]}
         */
        const bufferedSegments = []

        var newTotalSize = 0
        var bufferedSize = 0
        var nextWritePos = 0

        const handle = await fsprom.open(this.filepath, "a+")

        // try not to load all buffers into memory as this will take a toll on the computer
        const occupiedSegmentIterator = this.occupiedSegments.values()
        while (true) {
            const { done, value } = occupiedSegmentIterator.next()

            if (done) {
                break
            }

            // check if we can still put into memory
            const canStillBuffer = (bufferedSize + value.size) <= HEAP_CLEANUP_MEM_MAX
            if (canStillBuffer) {
                // read the segment to a buffer
                const buffer = Buffer.alloc(value.size)
                const readOutput = await handle.read(buffer, 0, value.size, value.pos)

                // record to buffered segments list
                bufferedSegments.push([value, readOutput.buffer])
                bufferedSize += value.size
            } else {
                // write buffered segments to file
                for (let x = 0; x < bufferedSegments.length; x++) {
                    const [heapindex, heapdata] = bufferedSegments.shift()
                    const writeOutput = await handle.write(heapdata, 0, heapdata.length, nextWritePos)

                    // record index movement
                    const newHeapindex = {
                        pos: nextWritePos,
                        size: writeOutput.bytesWritten
                    }
                    movedIndexes.set(heapindex.pos, newHeapindex)

                    // set new occupiedSegments list
                    newOccupiedSegments.set(newHeapindex.pos, newHeapindex)

                    // update next writing position
                    nextWritePos += writeOutput.bytesWritten
                }

                // record total size
                newTotalSize += bufferedSize

                // reset buffered size and ready segments
                bufferedSize = 0
                bufferedSegments = []
            }
        }

        // save new occupiedSegments list
        this.occupiedSegments = newOccupiedSegments

        // clean the file for reals
        await handle.truncate(newTotalSize)
        await handle.close()

        // update heap size
        this.heapSize = newTotalSize

        // empty free segments list
        this.freeSegmentQueues = new Map()
        this.freeSegments = new Map()

        return movedIndexes
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
                handle = await fsprom.open(this.filepath, "a+")
            } catch (e) {
                if (inTimeout) {
                    this.emit(ERROR_EVENT, { err: e })
                } else {
                    throw e
                }
            }

            returnHandle = false
        }

        while (this.opQueue.length !== 0) {
            // always do FIFO
            const dataOp = this.opQueue.shift()

            let err, eventName, eventObj
            try {
                // perform the data operation
                if (dataOp.type === DATA_OPS.INSERT) {
                    eventName = dataOp.type
                    eventObj = await this._insert(handle, dataOp)
                } else if (dataOp.type === DATA_OPS.DELETE) {
                    eventName = dataOp.type
                    eventObj = await this._delete(handle, dataOp)
                } else if (dataOp.type === DATA_OPS.UPDATE) {
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
     * Seeks a free heap segment that best fits the desired size. Returns the HeapIndex if
     * such segment is found; otherwise, undefined is returned.
     * 
     * @param {number} desiredSize - Desired size to seek.
     */
    seekFreeSegment(desiredSize) {
        // try to get exact size if there is
        const freeSegmentQueue = this.freeSegmentQueues.get(desiredSize)
        if (freeSegmentQueue !== undefined && freeSegmentQueue.size > 0) {
            const heappos =  freeSegmentQueue.values().next().value
            const output = this.freeSegments.get(heappos)

            // remove from queue and segment map
            freeSegmentQueue.delete(heappos)
            this.freeSegments.delete(heappos)

            return output
        }

        // if exact size not found, iterate the map
        var output, returnedSize
        const queueIterator = this.freeSegmentQueues.keys()
        while (true) {
            const { done, value } = queueIterator.next()

            if (done) {
                break
            }

            if (value <= desiredSize) {
                // save for use later
                returnedSize = value

                // pop a value out of the segment queue
                const heappos = this.freeSegmentQueues.get(value).values().next().value
                if (heappos !== undefined) {
                    output = this.freeSegments.get(heappos)
                    
                    // remove from the queue
                    this.freeSegmentQueues.get(heappos).delete(heappos)

                    // make free segment unavailable
                    this.freeSegments.delete(heappos)
                    break
                }

                // somehow the queue is empty, delete it and continue
                this.freeSegmentQueues.delete(returnedSize)
            }
        }

        // clean up free segment entry if no more
        if (returnedSize && this.freeSegmentQueues.get(returnedSize).length === 0) {
            this.freeSegmentQueues.delete(returnedSize)
        }

        return output
    }

    /**
     * Saves info about a free heap segment in the heap.
     * 
     * @param {HeapIndex} heapindex - Metadata about the free segment.
     */
    trackFreeSegment(heapindex) {
        // if bigger than ceiling size, replace
        this.ceilSegmentSize = this.ceilSegmentSize < heapindex.size ? heapindex.size : this.ceilSegmentSize

        // add to freeSegmentQueues map
        if (this.freeSegmentQueues.has(heapindex.size)) {
            this.freeSegmentQueues.get(heapindex.size).add(heapindex.pos)
        } else {
            this.freeSegmentQueues.set(heapindex.size, new Set([heapindex.pos]))
        }

        // add to freeSegments map
        this.freeSegments.set(heapindex.pos, heapindex)
    }

    /**
     * Inserts a data buffer into the Heapbase through the operation queue.
     * 
     * @param {Buffer} heapdata - Data to be inserted.
     * @param {*} id - Value to identify the heapdata on insert event.
     */
    add(heapdata, id) {
        if (!this.isLoaded) {
            this.errorStatus = FILEBASE_ERRS.UNLOADED
            throw new Error(`Heapbase '${this.filepath}' is not loaded.`)
        }

        if (!(heapdata instanceof Buffer)) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot insert data types other than type Buffer.`)
        }

        // create the operation entry
        const dataOp = {
            type: DATA_OPS.INSERT,
            args: [heapdata, id]
        }

        // add to the queue
        this.opQueue.push(dataOp)

        // start the queue timeout, if not started
        if (!this.timeout) {
            this.setFlushTimeout()
        }
    }

    /**
     * Takes a insert data operation and performs it against the Heapbase.
     * 
     * @param {fs.promises.FileHandle} handle - Handle of the Heapbase's persistence file.
     * @param {DataOp} op - Operation to be performed.
     * 
     * @returns {HeapbaseDataEvent}
     */
    async _insert(handle, op) {
        if (op.type !== DATA_OPS.INSERT) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot perform a data operation other than INSERT.`)
        }

        // get the heapdata
        const heapdata = op.args[0]

        // create the heapindex
        var insertPos, heapindex, allocatedSize = heapdata.byteLength
        var didNotReuse = this.isWasteful || (this.freeSegmentQueues.size === 0) || this.ceilSegmentSize < heapdata.byteLength
        if (didNotReuse) {
            insertPos = this.heapSize
        } else {
            heapindex = this.seekFreeSegment(heapdata.byteLength)
            if (heapindex === undefined) {
                insertPos = this.heapSize
                didNotReuse = true
            } else {
                insertPos = heapindex.pos
                allocatedSize = heapindex.size
            }
        }

        // perform the insertion
        try {
            await handle.write(heapdata, 0, heapdata.byteLength, insertPos)
        } catch (e) {
            // rollback the allocation of free segment first, if allocated
            if (heapindex !== undefined) {
                this.trackFreeSegment(heapindex)
            }

            throw e
        }

        // if allocated size is bigger than write size, reuse extra segment
        const extraSize = allocatedSize - heapdata.byteLength
        if (extraSize > 0) {
            const extraPos = insertPos + heapdata.byteLength
            const extraHeapindex = {
                pos: extraPos,
                size: extraSize
            }
            this.trackFreeSegment(extraHeapindex)
        }

        // increment the heap size if we did not reuse segment
        if (didNotReuse) {
            this.heapSize += heapdata.byteLength
        }

        // create heapindex to return
        const returnHeapindex = {
            pos: insertPos,
            size: heapdata.byteLength
        }

        // add to occupied segments
        this.occupiedSegments.set(returnHeapindex.pos, returnHeapindex)

        // return the event
        return { heapindex: returnHeapindex, id: op.args[1] }
    }

    /**
     * Removes a heap segment's contents through the operation queue.
     * 
     * @param {HeapIndex} heapindex - Index of the heap segment to be deleted.
     * @param {*} id - Value to identify the heap segment on delete event.
     */
    remove(heapindex, id) {
        if (!this.isLoaded) {
            this.errorStatus = FILEBASE_ERRS.UNLOADED
            throw new Error(`Heapbase '${this.filepath}' is not loaded.`)
        }

        const indexIsValid = heapindex && typeof heapindex.pos === "number" && typeof heapindex.size === "number"
        if (!indexIsValid) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot remove using an invalid index.`)
        }

        const isOccupiedPos = this.occupiedSegments.has(heapindex.pos)
        if (isOccupiedPos) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot remove a heap segment that doesn't have data.`)
        }

        // create the operation entry
        const dataOp = {
            type: DATA_OPS.DELETE,
            args: [heapindex, id]
        }

        // add to the queue
        this.opQueue.push(dataOp)

        // start the queue timeout, if not started
        if (!this.timeout) {
            this.setFlushTimeout()
        }
    }

    /**
     * Takes a delete data operation and performs it against the Heapbase.
     * 
     * @param {fs.promises.FileHandle} handle - Handle of the Heapbase's persistence file.
     * @param {DataOp} op - Operation to be performed.
     * 
     * @returns {HeapbaseDataEvent}
     */
    async _delete(handle, op) {
        if (op.type !== DATA_OPS.DELETE) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot perform a data operation other than DELETE.`)
        }

        // get the heapindex
        const heapindex = op.args[0]

        // zero-fill the buffer if not unsafe
        if (!this.isUnsafe) {
            // prepare the buffer
            const zerofillBuffer = Buffer.alloc(heapindex.size)

            // perform the zero-filling
            await handle.write(zerofillBuffer, 0, zerofillBuffer.byteLength, heapindex.pos)
        }

        // remove from occupiedSegments list
        this.occupiedSegments.delete(heapindex.pos)

        // add to free segment list
        this.trackFreeSegment(heapindex)

        // emit the delete event
        this.emit("delete", { heapindex, id: op.args[1] })
    }

    /**
     * Updates a heap segment's contents through the operations queue.
     * 
     * @param {HeapIndex} heapindex - Index of the heap segment to be updated.
     * @param {Buffer} heapdata - Data used to update the heap segment.
     * @param {*} id - Value to identify the heap data on update event.
     */
    put(heapindex, heapdata, id) {
        if (!this.isLoaded) {
            this.errorStatus = FILEBASE_ERRS.UNLOADED
            throw new Error(`Heapbase '${this.filepath}' is not loaded.`)
        }

        const indexIsValid = heapindex && typeof heapindex.pos === "number" && typeof heapindex.size === "number"
        if (!indexIsValid) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot update using an invalid index.`)
        }

        const isOccupiedPos = this.occupiedSegments.has(heapindex.pos)
        if (!isOccupiedPos) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot update a heap segment that doesn't have data.`)
        }

        if (!(heapdata instanceof Buffer)) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot update using data types other than type Buffer.`)
        }

        // create the operation entry
        const dataOp = {
            type: DATA_OPS.UPDATE,
            args: [heapindex, heapdata, id]
        }

        // add to the queue
        this.opQueue.push(dataOp)
        
        // start the queue timeout, if not started
        if (!this.timeout) {
            this.setFlushTimeout()
        }
    }

    /**
     * Takes an update data operation and performs it against the Heapbase.
     * 
     * @param {fs.promises.FileHandle} handle - Handle of the Heapbase's persistence file.
     * @param {DataOp} op - Operation to be performed.
     * 
     * @returns {HeapbaseDataEvent}
     */
    async _update(handle, op) {
        if (!this.isLoaded) {
            this.errorStatus = FILEBASE_ERRS.UNLOADED
            throw new Error(`Heapbase '${this.filepath}' is not loaded.`)
        }

        if (op.type !== DATA_OPS.UPDATE) {
            this.errorStatus = FILEBASE_ERRS.COMMON
            throw new Error(`Cannot perform a data operation other than UPDATE.`)
        }

        // get the heapindex from occupiedSpaces, to make sure it is valid
        const heapindex = this.occupiedSegments.get(op.args[0].pos)

        // get the heapdata
        const heapdata = op.args[1]

        // check if data size for updating is bigger than current segment space
        // we need to increase if there is a free adjacent space, or find a bigger space altogether
        if (heapindex.size < heapdata.length) {
            const adjacentPos = heapindex.size + heapindex.pos

            // check if we can use adjacent segment
            let canUseAdjacentSegment = this.freeSegments.has(adjacentPos)
            if (canUseAdjacentSegment) {
                const adjacentSegmentIndex = this.freeSegments.get(adjacentPos)

                // overwrite this flag, no problem, we're just cascading if's
                canUseAdjacentSegment = heapdata.length <= (adjacentSegmentIndex.size + heapindex.size)
                if (canUseAdjacentSegment) {
                    // manipulate freeSegmentsQueue and freeSegments to pop the adjacent segment
                    this.freeSegmentQueues.get(adjacentSegmentIndex.size).delete(adjacentPos)
                    this.freeSegments.delete(adjacentPos)

                    // merge adjacent index to current index
                    // using JS's awesome powers of mutability
                    heapindex.size += adjacentSegmentIndex.size
                }
            }

            // cannot use adjacent segment, seek out another free segment or add to last
            if (!canUseAdjacentSegment) {
                // reuse the _insert function, it literally does what we want right now
                // just bamboozle it with the INSERT data op type
                const output = await this._insert(handle, { type: DATA_OPS.INSERT, args: [heapdata] })

                // should set our previous segment as free
                this.occupiedSegments.delete(heapindex.pos)
                this.trackFreeSegment(heapindex)

                return { heapindex: output.heapindex, id: op.args[2] }
            }
        }

        // perform the update
        await handle.write(heapdata, 0, heapdata.length, heapindex.pos)

        // TODO: do rollback here for the adjacent segment if error is thrown

        // if there is extra space after update, set it as a free segment
        if (heapdata.length < heapindex.size) {
            const newFreePos = heapdata.length + 1
            const newFreeSize = heapindex.size - heapdata.length

            // using JS's awesome powers of mutability
            heapindex.size = heapdata.length

            // add to free segments list
            this.trackFreeSegment({
                pos: newFreePos,
                size: newFreeSize
            })
        }

        // emit the event
        return { heapindex, id: op.args[2] }
    }

    /**
     * Selects a heap segment by its index and returns its data. Empties the operation queue
     * before getting the data. 
     * 
     * @param {HeapIndex} heapindex - Index of the heap segment to be selected.
     */
    async get(heapindex) {
        if (!this.isLoaded) {
            this.errorStatus = FILEBASE_ERRS.UNLOADED
            throw new Error(`Heapbase '${this.filepath}' is not loaded.`)
        }

        // prepare buffer to contain data
        const buffer = Buffer.alloc(heapindex.size)

        // open the handle first
        var handle = await fsprom.open(this.filepath, "a+")

        // perform all pending operations first
        handle = await this.flushOpQueue(handle)

        // get the heapdata
        const data = await handle.read(buffer, 0, heapindex.size, heapindex.pos)

        // close the file handle for no leaks
        await handle.close()

        // just return the buffer, no need to truncate
        return data.buffer
    }

    /**
     * Selects a heap segment by its index and returns its data. The difference from {@link Heapbase#get}
     * is that this method doesn't empty the operation queue before getting the data.
     * 
     * @param {HeapIndex} heapindex - Index of the heap segment to be selected.
     */
    async getNow(heapindex) {
        if (!this.isLoaded) {
            this.errorStatus = FILEBASE_ERRS.UNLOADED
            throw new Error(`Heapbase '${this.filepath}' is not loaded.`)
        }

        // prepare buffer to contain data
        const buffer = Buffer.alloc(heapindex.size)

        // get the heapdata
        const handle = await fsprom.open(this.filepath, "a+")
        const data = await handle.read(buffer, 0, heapindex.size, heapindex.pos)
        await handle.close()

        // just return the buffer, no need to truncate
        return data.buffer
    }

    /**
     * Check if certain heap segment is occupied through its heapindex.
     * 
     * @param {HeapIndex|number} heapindex - Heapindex to check. Can be the pos number.
     */
    isOccupied(heapindex) {
        return typeof heapindex === "number" ? this.occupiedSegments.has(heapindex) : this.occupiedSegments.has(heapindex.pos)
    }

    /**
     * Flag that signifies if the Heapbase has any operations to perform or not.
     * 
     */
    get isIdle() {
        return this.opQueue.length === 0
    }
}

/**
 * Function that serializes a record field into a segment of type Buffer.
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
 * Schema that describes and manages the data in a heap segment.
 * 
 * @typedef HeapSegmentSchema
 * @property {SegmentSerializer} serialize - Serializes a field into a segment buffer.
 * @property {SegmentDeserializer} deserialize - Deserializes a segment buffer into a field.
 */

/**
 * Wrapper that provides basic schema capabilities to a Heapbase.
 * 
 */
class HeapbaseManager extends EventEmitter {
    /**
     * Heapbase to be managed.
     * 
     * @type {Heapbase}
     */
    heapbase

    /**
     * Schema for all heap segments contained in the Heapbase.
     * 
     * @type {HeapSegmentSchema}
     */
    segmentSchema

    /**
     * Map used to exchange a segment id for its equivalent heapindex.
     * 
     * @type {Map<any, HeapIndex>}
     */
    idExchange

    /**
     * Cache map of segment data, operating in an LRU fashion.
     * 
     * @type {Map<any, { data: any, isGuarded: boolean }>}
     */
    dataCache

    /**
     * Limit of the data cache.
     * 
     * @type {number}
     */
    cacheLimit

    /**
     * Map of segment data to be rolled back in case of a write error.
     * 
     * @todo Each rollback should be saved in a queue manner, if an operation is successful the
     * corresponding rollback is shifted out. When an error happens, we can be sure that the rollback
     * is then accurate.
     * @type {Map<any, any[]>}
     */
    rollbackMap

    /**
     * Creates a HeapbaseManager.
     * 
     * @param {Heapbase} heapbase - Heapbase to be managed.
     * @param {HeapSegmentSchema} schema - Segment schemas to be used by the manager.
     * @param {number} [limit] - Limit of records that can be stored in the cache. Default is 1000.
     */
    constructor (heapbase, schema, limit = CACHE_LIMIT) {
        super()

        this.heapbase = heapbase
        this.segmentSchema = schema
        this.dataCache = new Map()
        this.rollbackMap = new Map()
        this.idExchange = new Map()
        this.cacheLimit = limit
    }

    /**
     * Loads the manager, and the Heapbase if it is not yet loaded.
     * 
     * @param {Map<any, HeapIndex>} idExchange - Map of segment ids to heap indexes.
     */
    async load(idExchange) {
        if (!(this.heapbase instanceof Heapbase)) {
            throw new Error(`Passed heapbase is not valid.`)
        }

        if (!this.segmentSchema) {
            throw new Error(`Cannot load a HeapbaseManager without a schema.`)
        }

        // load heapbase first
        if (!this.heapbase.isLoaded) {
            await this.heapbase.load(Array.from(idExchange.values()))
        }

        // save id exchange only if heapbase has contents
        // even if we load the id exchange to heapbase,
        // it will get ignored if the persistence file's size is 0
        if (this.heapbase.occupiedSegments.size !== 0) {
            this.idExchange = idExchange
        }

        // validate schema
        // set hollow functions for serialize and deserialize
        if (this.segmentSchema.deserialize === undefined) {
            this.segmentSchema.deserialize = HOLLOW_FUNC
        }

        if (this.segmentSchema.serialize === undefined) {
            this.segmentSchema.serialize = HOLLOW_FUNC
        }

        // catch events emitted by the heapbase, for data integrity
        const self = this
        this.heapbase.on(DATA_OPS.INSERT, function (event) {
            if (event.err) {
                // rollback in case of an error
                if (self.dataCache.has(event.id)) {
                    self.dataCache.delete(event.id)
                }

                // let user know also
                self.emit(DATA_OPS.INSERT, event)

                return
            }

            // set up the unguarding of cache data
            // also save the heapindex into the id exchange
            self.idExchange.set(event.id, event.heapindex)
            self.unguardCacheRecord(event.id)
            self.emit(DATA_OPS.INSERT, event)
        })

        this.heapbase.on(DATA_OPS.DELETE, function (event) {
            if (event.err) {
                // rollback in case of an error, if there's anything to rollback
                if (self.rollbackMap.has(event.id)) {
                    const rollbackData = self.rollbackMap.get(event.id).shift()
                    if (rollbackData !== undefined) {
                        // push to cache instead, we already removed it from the cache
                        self.pushToCache(event.id, rollbackData, false)
                    }

                    if (self.rollbackMap.get(event.id).length === 0) {
                        self.rollbackMap.delete(event.id)
                    }
                }

                // rollback in the id exchange
                self.idExchange.set(event.id, event.heapindex)
            }

            // let user know also
            self.emit(DATA_OPS.DELETE, event)
        })

        this.heapbase.on(DATA_OPS.UPDATE, function (event) {
            if (event.err) {
                // rollback in case of an error
                if (self.rollbackMap.has(event.id)) {
                    const rollbackData = self.rollbackMap.get(event.id).shift()
                    if (rollbackData !== undefined) {
                        self.dataCache.set(event.id, rollbackData)
                    }

                    if (self.rollbackMap.get(event.id).length === 0) {
                        self.rollbackMap.delete(event.id)
                    }
                }
            }

            // let user know also
            self.emit(DATA_OPS.UPDATE, event)
        })

        this.heapbase.on(ERROR_EVENT, function (event) {
            self.emit(ERROR_EVENT, event)
        })
    }
    
    /**
     * Shifts the record cache to make space. Returns true if was able to shift.
     * 
     */
    shiftCache() {
        // prepare record key holder
        let keyToShift

        // check what entry can be shifted
        const cacheIterator = this.dataCache.keys()
        while (true) {
            const { done, value } = cacheIterator.next()

            if (done) {
                break
            }

            const cacheEntry = this.dataCache.get(value)
            if (!cacheEntry.isGuarded) {
                keyToShift = value
                break
            }
        }

        if (keyToShift) {
            this.dataCache.delete(keyToShift)
            return true
        }

        return false
    }

    /**
     * Pushes a data to the cache, applying a guard so that it won't be shifted out. Returns true
     * if the data was pushed successfully. Otherwise, returns false.
     * 
     * @param {*} id - Id of the cache entry to be pushed.
     * @param {*} data - Data to be pushed to the cache.
     * @param {boolean} [shouldGuard=true] - Flag signifying if data should be guarded. Default is true.
     */
    pushToCache(id, data, shouldGuard = true) {
        let isSuccess = true

        // shift the cache if full
        if (this.dataCache.size <= this.cacheLimit) {
            isSuccess = this.shiftCache()
        }

        // if was able to shift the cache if full
        if (isSuccess) {
            this.dataCache.set(id, { isGuarded: shouldGuard, data })
        }

        return isSuccess
    }

    /**
     * Gets a data from the cache.
     * 
     * @param {*} id - Id of the cache entry to be retrieved.
     */
    getFromCache(id) {
        const output = this.dataCache.get(id)

        // inserts back to dataCache to set it as newest
        this.dataCache.delete(output)
        this.dataCache.set(id, output)

        return output.data
    }

    /**
     * Unguards a cache entry so that it can be shifted out of the cache.
     * 
     * @param {*} Id - Id of the cache entry to be unguarded.
     */
    unguardCacheRecord(id) {
        this.dataCache.has(id) && (this.dataCache.get(id).isGuarded = false)
    }

    /**
     * Adds a segment data to the Heapbase.
     * 
     * @param {*} data - Data to be added.
     * @param {*} id - Id of the data, for tracking.
     */
    add(data, id) {
        // get the id first and verify if existing
        if (this.idExchange.has(id)) {
            throw new Error(`Data of id ${id} is already found in the Heapbase.`)
        }

        // serialize into buffer first
        const heapdata = this.segmentSchema.serialize(data)

        // now add to heapbase
        this.heapbase.add(heapdata, id)

        // save to cache while insert is ongoing
        this.pushToCache(id, data)
    }

    /**
     * Removes a segment data from the Heapbase.
     * 
     * @param {*} id - Id of the data to be removed.
     */
    remove(id) {
        const heapindex = this.idExchange.get(id)
        if (heapindex === undefined) {
            throw new Error(`Data of id ${id} cannot be found in the Heapbase.`)
        }

        // delete in the cache, if there is
        if (this.dataCache.has(id)) {
            // make sure to prepare for rollback
            const cacheData = this.dataCache.get(id).data

            if (this.rollbackMap.has(id)) {
                this.rollbackMap.get(id).push(cacheData)
            } else {
                this.rollbackMap.set(id, [cacheData])
            }

            this.dataCache.delete(id)
        }

        // remove now from the heapbase
        this.heapbase.remove(heapindex, id)

        this.idExchange.delete(id)
    }

    /**
     * Updates a segment data in the Heapbase.
     * 
     * @param {*} id - Id of the data to be updated.
     * @param {*} data - Data which is the basis for the update.
     */
    update(id, data) {
        const heapindex = this.idExchange.get(id)
        if (heapindex === undefined) {
            throw new Error(`Data of id ${id} cannot be found in the Heapbase.`)
        }

        // get cache entry if any, and prepare for rollback
        var cacheData
        if (this.dataCache.has(id)) {
            cacheData = this.dataCache.get(id).data
            
            if (this.rollbackMap.has(id)) {
                this.rollbackMap.get(id).push(cacheData)
            } else {
                this.rollbackMap.set(id, [cacheData])
            }

            this.dataCache.set(id, data)
        }

        // make the segment
        const segment = this.segmentSchema.serialize(data)

        // update the heapbase
        this.heapbase.put(heapindex, segment, id)
    }

    /**
     * Gets a data from the Heapbase.
     * 
     * @param {*} id - Id of the data to be fetched.
     */
    async get(id) {
        // look in the cache first
        if (this.dataCache.has(id)) {
            return this.getFromCache(id)
        }

        const heapindex = this.idExchange.get(id)
        if (heapindex !== undefined) {
            // get the segment from the heapbase
            const segment = await this.heapbase.get(heapindex)

            // deserialize segment into data and save to cache
            const data = this.segmentSchema.deserialize(segment)
            this.pushToCache(id, data, false)

            return data
        }
    }
}

module.exports = {
    Heapbase,
    HeapbaseManager
}