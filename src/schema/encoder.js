/**
 * Encode a 64 bit integer.
 * 
 * @param {Buffer} buffer - Buffer for encoding.
 * @param {BigInt} data - Data to encode.
 * @param {number} offset - Offset where to write the data.
 */
function encodeBigUInt(offset, buffer, data) {
    buffer.writeBigUInt64BE(data, offset)

    return buffer
}

/**
 * Encode a 32 bit integer.
 * 
 * @param {Buffer} buffer - Buffer for encoding.
 * @param {number} data - Data to encode.
 * @param {number} offset - Offset where to write the data.
 */
function encodeUInt(offset, buffer, data) {
    buffer.writeUInt32BE(data, offset)

    return buffer
}

/**
 * Encode a 16 bit integer.
 * 
 * @param {Buffer} buffer - Buffer for encoding.
 * @param {number} data - Data to encode.
 * @param {number} offset - Offset where to write the data.
 */
function encodeSmallUInt(offset, buffer, data) {
    buffer.writeUInt16BE(data, offset)

    return buffer
}

/**
 * Encode a 8 bit integer.
 * 
 * @param {Buffer} buffer - Buffer for encoding.
 * @param {number} data - Data to encode.
 * @param {number} offset - Offset where to write the data.
 */
function encodeTinyUInt(offset, buffer, data) {
    buffer.writeUInt8(data, offset)

    return buffer
}

/**
 * Encode a buffer.
 * 
 * @param {Buffer} buffer - Buffer for encoding.
 * @param {Buffer} data - Data to encode.
 * @param {number} offset - Offset where to write the data.
 */
function encodeBuffer(offset, buffer, data) {
    buffer.set(data, offset)

    return buffer
}

module.exports = {
    encodeBigUInt,
    encodeUInt,
    encodeSmallUInt,
    encodeTinyUInt,
    encodeBuffer
}