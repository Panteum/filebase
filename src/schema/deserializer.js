/**
 * Deserialize a string from a buffer.
 * 
 * @param {Buffer} buffer - Buffer to deserialize.
 */
function deserializeString(buffer) {
    return buffer.toString()
}

/**
 * Return a buffer in a deserializing fashion.
 * 
 * @param {Buffer} buffer - Buffer to deserialize.
 */
function hollowDeserialize(buffer) {
    return buffer
}

module.exports = {
    deserializeString,
    hollowDeserialize
}