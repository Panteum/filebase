/**
 * Decode a 64 bit integer.
 * 
 * @param {Buffer} buffer - Buffer for encoding.
 * @param {number} offset - Offset where to write the data.
 */
function decodeBigUInt(offset, buffer) {
    return buffer.readBigUInt64BE(offset)
}

/**
 * Decode a 32 bit integer.
 * 
 * @param {Buffer} buffer - Buffer for decoding.
 * @param {number} offset - Offset where to read the data.
 */
function decodeUInt(offset, buffer) {
    return buffer.readUInt32BE(offset)
}

/**
 * Decode a 16 bit integer.
 * 
 * @param {Buffer} buffer - Buffer for decoding.
 * @param {number} offset - Offset where to read the data.
 */
function decodeSmallUInt(offset, buffer) {
    return buffer.readUInt16BE(offset)
}

/**
 * Decode a 8 bit integer.
 * 
 * @param {Buffer} buffer - Buffer for decoding.
 * @param {number} offset - Offset where to read the data.
 */
function decodeTinyUInt(offset, buffer) {
    return buffer.readUInt8(offset)
}

/**
 * Decode a buffer.
 * 
 * @param {Buffer} buffer - Buffer for decoding.
 * @param {number} offset - Offset where to read the data.
 * @param {number} size - Size of the buffer to read.
 */
function decodeBuffer(offset, size, buffer) {
    return buffer.slice(offset, size)
}

module.exports = {
    decodeBigUInt,
    decodeUInt,
    decodeSmallUInt,
    decodeTinyUInt,
    decodeBuffer
}