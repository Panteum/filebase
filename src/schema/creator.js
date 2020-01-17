const { ENCODE_TYPE, SERIALIZE_TYPE } = require(`./constants`)

const { encodeBigUInt, encodeUInt, encodeSmallUInt, encodeTinyUInt, encodeBuffer } = require(`./encoder`)
const { decodeBigUInt, decodeUInt, decodeSmallUInt, decodeTinyUInt, decodeBuffer } = require(`./decoder`)

const { serializeString, hollowSerialize } = require(`./serializer`)
const { deserializeString, hollowDeserialize } = require(`./deserializer`)

/**
 * @typedef {import('../slotbase').SlotSegmentSchema} SlotSegmentSchema
 */

/**
 * @typedef {import('../heapbase').HeapSegmentSchema} HeapSegmentSchema
 */

/**
 * Choose an encoder from a given type.
 * 
 * @param {string} type - Type of the encoder.
 */ 
function chooseEncoderFromType(type) {
    switch (type) {
        case ENCODE_TYPE.BIG_UINT: return encodeBigUInt
        case ENCODE_TYPE.UINT: return encodeUInt
        case ENCODE_TYPE.SMALL_UINT: return encodeSmallUInt
        case ENCODE_TYPE.TINY_UINT: return encodeTinyUInt
        default: return encodeBuffer
    }
}

/**
 * Choose an decoder from a given type.
 * 
 * @param {string} type - Type of the decoder.
 */ 
function chooseDecoderFromType(type) {
    switch (type) {
        case ENCODE_TYPE.BIG_UINT: return decodeBigUInt
        case ENCODE_TYPE.UINT: return decodeUInt
        case ENCODE_TYPE.SMALL_UINT: return decodeSmallUInt
        case ENCODE_TYPE.TINY_UINT: return decodeTinyUInt
        default: return decodeBuffer
    }
}

/**
 * Choose an serializer from a given type.
 * 
 * @param {string} type - Type of the serializer.
 */ 
function chooseSerializerFromType(type) {
    switch (type) {
        case SERIALIZE_TYPE.STRING: return serializeString
        default: return hollowSerialize
    }
}

/**
 * Choose an deserializer from a given type.
 * 
 * @param {string} type - Type of the deserializer.
 */ 
function chooseDeserializerFromType(type) {
    switch (type) {
        case SERIALIZE_TYPE.STRING: return deserializeString
        default: return hollowDeserialize
    }
}

/**
 * Creates a schema for a segment in a slot record.
 * 
 * @param {string} name - Name of the slot schema.
 * @param {number} pos - Position of the segment of the schema in the slot record.
 * @param {number} size - Size of the segment, in bytes.
 * @param {boolean} [isId=false] - Flag signifying if schema is the id schema. Default is false.
 * @param {string} encodeType - Type of the encoder/decoder.
 * 
 * @returns {SlotSegmentSchema}
 */
function createSlotSchema(name, pos, size, encodeType, isId = false) {
    var encode = chooseEncoderFromType(encodeType).bind(null, pos)
    var decode = chooseDecoderFromType(encodeType)

    if (encodeType === ENCODE_TYPE.BUFFER) {
        decode = decode.bind(null, pos, size)
    } else {
        decode = decode.bind(null, pos)
    }

    return {
        name,
        pos,
        size,
        isId,
        encode,
        decode
    }
}

/**
 * Creates a schema for a segment in a heapbase.
 * 
 * @param {string} serializeType - Type of the de/serializer.
 * 
 * @returns {HeapSegmentSchema}
 */
function createHeapSchema(serializeType) {
    return {
        serialize: chooseSerializerFromType(serializeType),
        deserialize: chooseDeserializerFromType(serializeType)
    }
}

module.exports = {
    createHeapSchema,
    createSlotSchema
}