/**
 * Enum holding types of encoding/decoding.
 * 
 */
const ENCODE_TYPE = {
    BIG_UINT: `big_uint`,
    UINT: `uint`,
    SMALL_UINT: `small_uint`,
    TINY_UINT: `tiny_uint`,
    BUFFER: `buffer`
}

/**
 * Enum holding types of serializing/deserializing.
 * 
 */
const SERIALIZE_TYPE = {
    HOLLOW: `hollow`,
    STRING: `string`
}

module.exports = {
    ENCODE_TYPE,
    SERIALIZE_TYPE
}