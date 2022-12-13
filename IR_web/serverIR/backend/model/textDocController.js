const mongoose = require('mongoose');

const textSchema = mongoose.Schema({
    docid : String,
    text : String
})

module.exports = mongoose.model('textSchema' , textSchema);