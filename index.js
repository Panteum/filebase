const { Slotbase, SlotbaseManager } = require(`./src/slotbase`)
const { Heapbase, HeapbaseManager } = require(`./src/heapbase`)
const JSONBase = require(`./src/jsonbase`)
const JSONHeapbase = require(`./src/jsonheapbase`)

const SchemaCreator = require(`./src/schema`)

module.exports = {
    Slotbase,
    Heapbase,
    SlotbaseManager,
    HeapbaseManager,
    JSONBase,
    JSONHeapbase,
    SchemaCreator
}