const fs = require(`fs`)
const fsprom = fs.promises

const path = require(`path`)
const EventEmitter = require(`events`)

const PERSIST_DELAY_DEFAULT = 5000

// to accommodate BigInt values
BigInt.prototype.toJSON = function () {
    return this.toString()
}

class JSONBase extends EventEmitter {
    /**
     * Absolute path of the file used by the JSONBase as persistence medium.
     * 
     * @type {string}
     */
    filepath

    /**
     * Records of the in-memory database.
     * 
     * @type {Object.<string, any>}
     */
    records

    /**
     * Number of milliseconds to lapse before persisting the in-memory database.
     * 
     * @type {number}
     */
    persistDelay

    /**
     * Flag signifying if JSONBase is to persist its contents.
     * 
     * @type {boolean}
     */
    shouldPersist

    /**
     * Flag signifying readiness of the JSONBase. If set to true, the JSONBase has already
     * loaded its data and is ready for operations.
     * 
     * @type {boolean}
     */
    isLoaded

    /**
     * Creates a JSONBase. The constructor only creates an instance in memory; to load the
     * JSONBase's data, {@link JSONBase#load} should be called.
     * 
     * @param {string} filepath - Path to the persistence file for the JSONBase.
     * @param {number} [persistDelay] - Delay in milliseconds of the persist operation of the JSONBase.
     */
    constructor (filepath, persistDelay) {
        if (!path.isAbsolute(filepath)) {
            throw new Error("Cannot accept a relative filepath.")
        }

        super()

        this.filepath = filepath
        this.persistDelay = persistDelay || PERSIST_DELAY_DEFAULT
        this.records = {}
        this.isLoaded = false
        this.shouldPersist = false
    }

    /**
     * Loads the JSONBase.
     * 
     */
    async load() {
        // get the JSON from the file
        const handle = await fsprom.open(this.filepath, "a+")
        const serialJSON = await handle.readFile({ encoding: "utf8" })
        await handle.close()
        
        // try to load the JSON
        try {
            if (serialJSON.length > 0) {
                this.records = JSON.parse(serialJSON)
            }
        } catch (e) {
            throw e
        }

        this.isLoaded = true
    }

    /**
     * Persists the in-memory database to the file.
     * 
     */
    async persist() {
        // guard for what may be a very expensive process
        if (this.shouldPersist) {
            // transform in-memory database into JSON
            const serialJSON = JSON.stringify(this.records)

            // write the JSON to file
            const handle = await fsprom.open(this.filepath, "a+")
            await handle.writeFile(serialJSON)
            await handle.close()
        }
    }

    /**
     * Triggers the persist timeout of the JSONBase.
     * 
     */
    triggerPersist() {
        if (!this.shouldPersist) {
            this.shouldPersist = true

            const self = this
            setTimeout(function () {
                self.persist()
            }, this.persistDelay)
        }
    }
}

module.exports = JSONBase