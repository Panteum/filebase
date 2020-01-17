/**
 * Serialize a string from a buffer.
 * 
 * @param {string} data - String data to serialize.
 */
function serializeString(data) {
    return Buffer.from(data)
}

/**
 * Return a buffer in a serializing fashion.
 * 
 * @param {Buffer} buffer - Buffer to serialize.
 */
function hollowSerialize(buffer) {
    return buffer
}

module.exports = {
    serializeString,
    hollowSerialize
}