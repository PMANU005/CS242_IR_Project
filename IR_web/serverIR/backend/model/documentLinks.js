//const mongoose = require('mongoose');
const {Schema , model} = require("mongoose") 

// const linkSchema = mongoose.Schema({
//     doc : String,
//     link : String,
//     text : String
// })

const linkSchema = new Schema( {
    doc : String,
    link : String,
    text : String
})

//module.exports = model('docLink' , linkSchema);
exports.linkScheme =  model( "sample_db" , linkSchema , "sample_links")