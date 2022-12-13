//const mongoose = require('mongoose');
// const docSchema = mongoose.Schema({
//     word : String,
//     docs : [{
//         document : String,
//         score : String,
//     }]
// })


const {Schema , model} = require("mongoose") 

const docSchema = new Schema( {
    word : String,
    doc : [{
        document : String,
        score : String,
    }]
}, {collection : 'sample_collection'})

//module.exports = model('agenda' , docSchema);
//module.exports = mongoose.model('sample_collection' , docSchema);

exports.agenda = model("sample_db" , docSchema , "sample_collection")