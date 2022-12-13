const mongoose = require('mongoose');
    
const docSchema = new mongoose.Schema( {
    word : String,
    doc : [{
        document : String,
        score : String,
    }]
} , {collection : "sample_collection"});

const linkSchema = new mongoose.Schema( {
    doc : String,
    link : String,
    text : String
} , {collection : "sample_links"});

const docObject = mongoose.model('docObject', docSchema);
const docLink = mongoose.model('docLink', linkSchema);
    
// Exporting our model objects
module.exports = {
    docObject,docLink
}